//! HTTPハンドラ。ここではXML生成もキャッシュ書き込みも行わない。

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use bytes::Bytes;

use crate::fetcher;
use crate::http::etag;
use crate::state::SharedState;

const ATOM_CONTENT_TYPE: &str = "application/atom+xml; charset=utf-8";
const XML_CONTENT_TYPE: &str = "application/xml; charset=utf-8";

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
pub async fn feed(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    // Guard を .await 跨ぎで持たないよう load_full で Arc を取り出す
    let snapshot = state.feed.load_full();
    serve_cached(
        &headers,
        &snapshot.etag,
        snapshot.body.clone(),
        ATOM_CONTENT_TYPE,
    )
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

    if let Some(entry) = state.entities.get(id).await {
        return serve_cached(&headers, &entry.etag, entry.body.clone(), XML_CONTENT_TYPE);
    }

    // ミスは常に307。JMAの実IDは `{yyyyMMddHHmmss}_{serial}_{TYPE}_{code}` 形式で
    // 上流に存在する。DMDATA由来の合成IDは上流に存在しない可能性があるが、
    // その場合は上流が404を返すのに任せる(キャッシュ在庫中はここに来ない)。
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
