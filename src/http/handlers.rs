//! HTTPハンドラ。ここではXML生成もキャッシュ書き込みも行わない。

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use dashmap::mapref::entry::Entry;
use tokio::sync::watch;

use crate::fetcher;
use crate::http::etag;
use crate::jma::id::is_fetchable_id;
use crate::state::SharedState;
use crate::types::{EntityEntry, LatestEntry};

const ATOM_CONTENT_TYPE: HeaderValue =
    HeaderValue::from_static("application/atom+xml; charset=utf-8");
const XML_CONTENT_TYPE: HeaderValue = HeaderValue::from_static("application/xml; charset=utf-8");
const X_INSTANCE_STARTED: &str = "x-instance-started";

// 電文実体(telegram id 単位)は一度発行されたら内容不変(電文IDは一意)なので長期キャッシュ可
const DATA_CACHE_CONTROL: HeaderValue =
    HeaderValue::from_static("public, max-age=31536000, immutable");
// フィードは地震速報等の即時性が要るため、キャッシュ本体は保持させつつも
// 毎回オリジンへ再検証(If-None-Match)を強制する(no-cache: 「保存はして良いが
// 使う前に必ず再検証」の意味。ETag一致なら304で本文転送は省ける)。
// no-cacheのみで仕様上は毎回revalidation必須になるためmust-revalidateは本来冗長だが、
// no-cacheの解釈が緩い中間キャッシュ実装への防御として併記しておく。
const FEED_CACHE_CONTROL: HeaderValue =
    HeaderValue::from_static("public, no-cache, must-revalidate");
// /latest/* は同一URLの中身が新着で差し替わるため、フィードと同じく毎回再検証させる
const LATEST_CACHE_CONTROL: HeaderValue = FEED_CACHE_CONTROL;
// エラー応答・ヘルスチェックはCDN/ブラウザにキャッシュさせない
const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");

/// If-None-Match を評価して 200(body) か 304(bodyなし+ETag再送)を返す。
fn serve_cached(headers: &HeaderMap, entry: &EntityEntry, cache_control: HeaderValue) -> Response {
    if let Some(inm) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        && etag::if_none_match(inm, &entry.etag)
    {
        // RFC 9110: 304 は body 無しで ETag を再送する
        return (
            StatusCode::NOT_MODIFIED,
            [
                (header::ETAG, entry.etag_header.clone()),
                (header::CACHE_CONTROL, cache_control),
            ],
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [
            (header::ETAG, entry.etag_header.clone()),
            (header::CONTENT_TYPE, XML_CONTENT_TYPE),
            (header::CACHE_CONTROL, cache_control),
        ],
        entry.body.clone(),
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
            [
                (header::ETAG, snapshot.etag_header.clone()),
                (header::CACHE_CONTROL, FEED_CACHE_CONTROL),
            ],
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            [
                (header::ETAG, snapshot.etag_header.clone()),
                (header::CONTENT_TYPE, ATOM_CONTENT_TYPE),
                (header::CACHE_CONTROL, FEED_CACHE_CONTROL),
            ],
            snapshot.body.clone(),
        )
            .into_response()
    };

    let response_headers = response.headers_mut();
    if let Some(lm) = &snapshot.last_modified_header {
        response_headers.insert(header::LAST_MODIFIED, lm.clone());
    }
    response_headers.insert(X_INSTANCE_STARTED, state.started_at_header.clone());
    response
}

/// GET /developer/xml/data/{file}
pub async fn data(
    State(state): State<SharedState>,
    Path(file): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(id) = file.strip_suffix(".xml") else {
        return (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, NO_STORE)]).into_response();
    };
    if id.is_empty() {
        return (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, NO_STORE)]).into_response();
    }
    serve_entity(
        &state,
        id,
        &headers,
        DATA_CACHE_CONTROL,
        MissPolicy::FeedOnly,
    )
    .await
}

/// GET /latest/earthquake — 最新の VXSE51 / VXSE52 / VXSE53 の実体XML。
pub async fn latest_earthquake(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let latest = state.latest.load().earthquake.clone();
    serve_latest(&state, latest, &headers).await
}

/// GET /latest/tsunami — 最新の VTSE41 の実体XML。
pub async fn latest_tsunami(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    let latest = state.latest.load().tsunami.clone();
    serve_latest(&state, latest, &headers).await
}

/// `/latest/*` 共通。保管場所が空なら404。
/// 同じURLの中身が新着で差し替わるため、immutable ではなくフィードと同じ毎回再検証にする。
async fn serve_latest(
    state: &SharedState,
    latest: Option<LatestEntry>,
    headers: &HeaderMap,
) -> Response {
    match latest {
        Some(LatestEntry {
            entry: Some(entry), ..
        }) => serve_cached(headers, &entry, LATEST_CACHE_CONTROL),
        // 実体未取得(poll由来・ウォームアップ由来・起動時補完): dataハンドラと同じ経路で取得する
        Some(LatestEntry { meta, entry: None }) => {
            serve_entity(
                state,
                &meta.id,
                headers,
                LATEST_CACHE_CONTROL,
                MissPolicy::Latest,
            )
            .await
        }
        None => (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, NO_STORE)]).into_response(),
    }
}

/// キャッシュミス時に上流取得を許可する条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissPolicy {
    /// クライアント指定のID(dataハンドラ)。feed在中IDのみ取得し、それ以外は404。
    FeedOnly,
    /// `/latest` が保持するID。サーバ側(aggregator)が決めたIDなのでfeed外でも取得を許可する。
    Latest,
}

/// 上流取得の失敗(dmdataのエラー応答・通信失敗)。
/// 取得を許可した時点で電文は存在するはずなので、404ではなく上流障害として返す。
fn upstream_failed() -> Response {
    (StatusCode::BAD_GATEWAY, [(header::CACHE_CONTROL, NO_STORE)]).into_response()
}

/// 上流取得の待機予算切れ。
fn upstream_timed_out() -> Response {
    (
        StatusCode::GATEWAY_TIMEOUT,
        [(header::CACHE_CONTROL, NO_STORE)],
    )
        .into_response()
}

/// 電文IDの実体XMLを返す。pinned → entities → (取得許可されていれば)dmdataから
/// singleflight取得。ミス時の許可条件は `policy` で決まり、許可したうえでの取得失敗は
/// 502、待機予算切れは504を返す。
async fn serve_entity(
    state: &SharedState,
    id: &str,
    headers: &HeaderMap,
    cache_control: HeaderValue,
    policy: MissPolicy,
) -> Response {
    // 1. pinned(feed在中のdmdata由来entry)。DashMapのRef guardを
    //    .await 跨ぎで持たないよう Arc clone して即drop
    let pinned = state.pinned.get(id).map(|entry| Arc::clone(entry.value()));
    if let Some(entry) = pinned {
        return serve_cached(headers, &entry, cache_control);
    }

    // 2. entities(moka: ミス補充された実体 + feedから降格したdmdata entry)
    if let Some(entry) = state.entities.get(id).await {
        return serve_cached(headers, &entry, cache_control);
    }

    // 3. ミス: 電文IDとしてありうる形式のみ dmdata telegram.data から取得する。
    //    ゴミIDで dmdata API 呼び出しを浪費しない(inflight も起動しない)。
    //    /latest のIDはdmdata由来なので通常ここには来ない(来たら取得不能=上流障害扱い)
    if !is_fetchable_id(id) {
        return match policy {
            MissPolicy::FeedOnly => {
                (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, NO_STORE)]).into_response()
            }
            MissPolicy::Latest => upstream_failed(),
        };
    }

    // 4. アローリスト: feed在中IDのみアウトバウンドfetchを許可(/latest は対象外)。
    //    (evict済みでもキャッシュ在中ならステップ1-2で配信済み)
    if policy == MissPolicy::FeedOnly && !state.feed_ids.contains(id) {
        return (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, NO_STORE)]).into_response();
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
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [(header::CACHE_CONTROL, NO_STORE)],
                )
                    .into_response();
            }
            let (tx, rx) = watch::channel(None);
            vacant.insert(rx.clone());
            tokio::spawn(fetcher::fetch_entity(state.clone(), id.to_owned(), tx));
            rx
        }
    };

    // 待機予算 = クライアント全体タイムアウト + 余裕1秒(fetchはこれを超えられない)
    let wait = Duration::from_secs(state.config.dmdata.fetch_timeout_secs + 1);
    // 取得失敗・タイムアウト: dmdata telegram.data は認証必須でリダイレクト先に
    // ならないため(旧JMA上流への307は廃止)、上流障害として502/504を返す
    match tokio::time::timeout(wait, rx.wait_for(|v| v.is_some())).await {
        Ok(Ok(value)) => match value.as_ref() {
            Some(entry) => serve_cached(headers, entry, cache_control),
            None => upstream_failed(),
        },
        // Sender drop(取得失敗)
        Ok(Err(_)) => upstream_failed(),
        Err(_) => upstream_timed_out(),
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
        return (StatusCode::BAD_REQUEST, [(header::CACHE_CONTROL, NO_STORE)]).into_response();
    };
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(header::LOCATION, value), (header::CACHE_CONTROL, NO_STORE)],
    )
        .into_response()
}

/// GET /healthz — 常時200。
pub async fn healthz() -> Response {
    (StatusCode::OK, [(header::CACHE_CONTROL, NO_STORE)], "ok").into_response()
}

/// GET /readyz — ready なら200、でなければ503+状態JSON。
pub async fn readyz(State(state): State<SharedState>) -> Response {
    let snapshot = state.readiness.snapshot();
    let status = if state.readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, [(header::CACHE_CONTROL, NO_STORE)], Json(snapshot)).into_response()
}
