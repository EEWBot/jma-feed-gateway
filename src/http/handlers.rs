//! HTTPハンドラ。ここではXML生成もキャッシュ書き込みも行わない。

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use tokio::sync::watch;

use crate::fetcher;
use crate::http::etag;
use crate::jma::id::is_fetchable_id;
use crate::state::SharedState;

const ATOM_CONTENT_TYPE: &str = "application/atom+xml; charset=utf-8";
const XML_CONTENT_TYPE: &str = "application/xml; charset=utf-8";
const X_INSTANCE_STARTED: &str = "x-instance-started";

/// If-None-Match を評価して 200(body) か 304(bodyなし+ETag再送)を返す。
fn serve_cached(
    headers: &HeaderMap,
    etag_value: &str,
    body: Bytes,
    content_type: &'static str,
) -> Response {
    if let Some(inm) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        && etag::if_none_match(inm, etag_value)
    {
        // RFC 9110: 304 は body 無しで ETag を再送する
        return (
            StatusCode::NOT_MODIFIED,
            [(header::ETAG, etag_value.to_owned())],
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [
            (header::ETAG, etag_value.to_owned()),
            (header::CONTENT_TYPE, content_type.to_owned()),
        ],
        body,
    )
        .into_response()
}

/// GET /developer/xml/feed/eqvol.xml
///
/// 200/304の両方に `Last-Modified`(あれば)と `X-Instance-Started` を付与する。
/// `If-Modified-Since` は `If-None-Match` が無い場合のみ評価する(RFC 9110 §13.1.3)。
pub async fn feed(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    // Guard を .await 跨ぎで持たないよう load_full で Arc を取り出す
    let snapshot = state.feed.load_full();

    let not_modified = match headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        // If-None-Match があれば If-Modified-Since は無視
        Some(inm) => etag::if_none_match(inm, &snapshot.etag),
        None => match (
            headers
                .get(header::IF_MODIFIED_SINCE)
                .and_then(|v| v.to_str().ok()),
            snapshot.last_modified,
        ) {
            (Some(ims), Some(lm)) => etag::not_modified_since(ims, lm),
            _ => false,
        },
    };

    let mut response = if not_modified {
        // RFC 9110: 304 は body 無しで ETag を再送する
        (
            StatusCode::NOT_MODIFIED,
            [(header::ETAG, snapshot.etag.clone())],
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            [
                (header::ETAG, snapshot.etag.clone()),
                (header::CONTENT_TYPE, ATOM_CONTENT_TYPE.to_owned()),
            ],
            snapshot.body.clone(),
        )
            .into_response()
    };

    let response_headers = response.headers_mut();
    if let Some(lm) = &snapshot.last_modified_http
        && let Ok(value) = lm.parse()
    {
        response_headers.insert(header::LAST_MODIFIED, value);
    }
    if let Ok(value) = state.started_at.parse() {
        response_headers.insert(X_INSTANCE_STARTED, value);
    }
    response
}

/// GET /developer/xml/data/{file}
pub async fn data(
    State(state): State<SharedState>,
    Path(file): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(id) = file.strip_suffix(".xml") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if id.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }

    // 1. pinned(feed在中のdmdata由来entry)。DashMapのRef guardを
    //    .await 跨ぎで持たないよう Arc clone して即drop
    let pinned = state.pinned.get(id).map(|entry| Arc::clone(entry.value()));
    if let Some(entry) = pinned {
        return serve_cached(&headers, &entry.etag, entry.body.clone(), XML_CONTENT_TYPE);
    }

    // 2. entities(moka: ミス補充された実体 + feedから降格したdmdata entry)
    if let Some(entry) = state.entities.get(id).await {
        return serve_cached(&headers, &entry.etag, entry.body.clone(), XML_CONTENT_TYPE);
    }

    // 3. ミス: 電文IDとしてありうる形式のみ dmdata telegram.data から取得する。
    //    ゴミIDで dmdata API 呼び出しを浪費しない(inflight も起動しない)。
    if !is_fetchable_id(id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    // 4. アローリスト: feed在中IDのみアウトバウンドfetchを許可。
    //    (evict済みでもキャッシュ在中ならステップ1-2で配信済み)
    if !state.feed_ids.contains(id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    // singleflight: 先着のみ取得を起動し、全員が同じ watch で完成entryを待つ。
    // DashMapのentry guardを .await 跨ぎで持たないよう、match式でrxをcloneして抜ける。
    let mut rx = match state.inflight.entry(id.to_owned()) {
        Entry::Occupied(occupied) => occupied.get().clone(),
        Entry::Vacant(vacant) => {
            // トークン消費は実際にアウトバウンドfetchをspawnする先着のみ。
            // 既存inflightへの合流(Occupied)は消費しない。try_acquireは同期なので
            // entry guard保持中でも安全(early returnでguardはdrop、inflight未挿入)。
            if !state.fetch_limiter.try_acquire() {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let (tx, rx) = watch::channel(None);
            vacant.insert(rx.clone());
            tokio::spawn(fetcher::fetch_entity(state.clone(), id.to_owned(), tx));
            rx
        }
    };

    // 待機予算 = クライアント全体タイムアウト + 余裕1秒(fetchはこれを超えられない)
    let wait = Duration::from_secs(state.config.dmdata.fetch_timeout_secs + 1);
    let entry = match tokio::time::timeout(wait, rx.wait_for(|v| v.is_some())).await {
        Ok(Ok(value)) => value.as_ref().cloned(),
        // タイムアウト or Sender drop(取得失敗)
        _ => None,
    };
    match entry {
        Some(entry) => serve_cached(&headers, &entry.etag, entry.body.clone(), XML_CONTENT_TYPE),
        // 取得失敗・タイムアウト: dmdata telegram.data は認証必須でリダイレクト先に
        // ならないため404を返す(旧JMA上流への307は廃止)
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// フォールバック: 既存ルートに一致しなかった全パスを上流JMAへ 307 転送する。
///
/// DNS等で `https://www.data.jma.go.jp/` をこのサーバへ捻じ曲げた際、当サーバが扱わない
/// リソース(他フィード・スタイルシート・画像等)へ到達できるようにするためのもの。
///
/// オープンリダイレクト対策: 転送先の authority(ホスト)は設定値 `upstream_base_url` の
/// 固定定数のみから構成し、リクエストからは **path と query だけ** を連結する。
/// path は axum が常に `/` 始まりを保証するため、たとえ `//evil.com` や絶対形式URIを
/// 送られても結果は必ず `https://www.data.jma.go.jp/...`(JMAホスト上のパス)に固定され、
/// 別ホストへは飛ばない。
pub async fn upstream_redirect(
    State(state): State<SharedState>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let base = state.config.http.upstream_base_url.trim_end_matches('/');
    let location = match uri.query() {
        Some(query) => format!("{base}{}?{query}", uri.path()),
        None => format!("{base}{}", uri.path()),
    };
    // 制御文字混入等で HeaderValue にできない場合は転送しない
    let Ok(value) = HeaderValue::from_str(&location) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, value)]).into_response()
}

/// GET /healthz — 常時200。
pub async fn healthz() -> &'static str {
    "ok"
}

/// GET /readyz — ready なら200、でなければ503+状態JSON。
pub async fn readyz(State(state): State<SharedState>) -> Response {
    let snapshot = state.readiness.snapshot();
    let status = if state.readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(snapshot)).into_response()
}
