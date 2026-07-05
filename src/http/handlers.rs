//! HTTPハンドラ。ここではXML生成もキャッシュ書き込みも行わない。

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use bytes::Bytes;

use crate::fetcher;
use crate::http::etag;
use crate::jma::id::is_jma_id;
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

    // 2. entities(moka: JMA実体 + feedから降格したdmdata entry)
    if let Some(entry) = state.entities.get(id).await {
        return serve_cached(&headers, &entry.etag, entry.body.clone(), XML_CONTENT_TYPE);
    }

    // 3. ミス: JMA実ID形式(`{yyyyMMddHHmmss}_...`)のみ上流へ307。
    //    DMDATA電文ID等の非JMA形式は上流に存在しないため404
    //    (無駄なinflight起動もしない)。
    if !is_jma_id(id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    // singleflight: 先着のみバックグラウンド取得を起動し、全員に307を即返す
    if state.inflight.insert(id.to_owned(), ()).is_none() {
        tokio::spawn(fetcher::fetch_entity(state.clone(), id.to_owned()));
    }

    let location = format!(
        "{}/{}.xml",
        state.config.jma.data_base_url.trim_end_matches('/'),
        id
    );
    // Redirect::temporary = 307(Redirect::to は303なので不可)
    Redirect::temporary(&location).into_response()
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
