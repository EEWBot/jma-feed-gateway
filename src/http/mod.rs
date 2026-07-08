//! HTTP層。読み取り専用(ArcSwap load / moka get のみ)。

pub mod etag;
pub mod handlers;

use axum::Router;
use axum::routing::get;

use crate::state::SharedState;

pub fn build_router(state: SharedState) -> Router {
    Router::new()
        .route("/developer/xml/feed/eqvol.xml", get(handlers::feed))
        // 長期版フィード。中身は現行の生成フィード(eqvol.xml)と同一を返す
        .route("/developer/xml/feed/eqvol_l.xml", get(handlers::feed))
        // axum 0.8: `.xml` サフィックス付きパラメータは使えないためファイル名全体で受ける
        .route("/developer/xml/data/{file}", get(handlers::data))
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        // 未対応パスは上流JMAへ 307 転送(DNS捻じ曲げ時の他リソース到達用)
        .fallback(handlers::upstream_redirect)
        .with_state(state)
}
