//! 初期一覧取得 + キャッシュミス時のバックグラウンド実体取得(singleflight)。

use std::cmp::min;
use std::time::Duration;

use crate::config::Config;
use crate::error::UpstreamError;
use crate::jma::{entity_parse, feed_parse};
use crate::state::SharedState;
use crate::types::{DedupKey, Event, EventSource, ItemMeta};

const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// JMAの初期一覧(Atom)をリトライ付きで取得しパースする。
/// HTTP公開前に完了必須。実体のプリフェッチは行わない。
pub async fn load_initial_feed(
    client: &reqwest::Client,
    config: &Config,
) -> Result<Vec<ItemMeta>, UpstreamError> {
    let attempts = config.jma.retry_attempts.max(1);
    let mut backoff = Duration::from_millis(config.jma.retry_initial_backoff_ms);
    let mut last_err = None;

    for attempt in 1..=attempts {
        match try_fetch_feed(client, config).await {
            Ok(items) => {
                tracing::info!(entries = items.len(), attempt, "initial feed loaded");
                return Ok(items);
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, max = attempts, "initial feed fetch failed");
                last_err = Some(e);
                if attempt < attempts {
                    tokio::time::sleep(backoff).await;
                    backoff = min(backoff.saturating_mul(2), MAX_BACKOFF);
                }
            }
        }
    }
    Err(last_err.expect("at least one attempt was made"))
}

async fn try_fetch_feed(
    client: &reqwest::Client,
    config: &Config,
) -> Result<Vec<ItemMeta>, UpstreamError> {
    let response = client.get(&config.jma.feed_url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(UpstreamError::Status(status));
    }
    let body = response.text().await?;
    feed_parse::parse(&body)
}

/// inflight マップから必ず remove するための Drop ガード。
/// panic 時も含めあらゆる経路で singleflight を解除する。
struct InflightGuard {
    state: SharedState,
    id: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.state.inflight.remove(&self.id);
    }
}

/// キャッシュミスした実体XMLをJMAからバックグラウンド取得する。
/// 呼び出し側(dataハンドラ)が `state.inflight.insert(id, ())` で先着ガードを
/// 取得済みであることが前提。完了・失敗いずれでも必ずガードを解除する。
///
/// 取得結果は直接mokaに入れず、メタ抽出(Control/Head)のうえ
/// `Event { source: JmaFeed, .. }` として mpsc で aggregator へ送る(single-writer 維持)。
/// JmaFeed 由来のEventは entities 挿入のみで一覧は再生成されない。
pub async fn fetch_entity(state: SharedState, id: String) {
    let _guard = InflightGuard {
        state: state.clone(),
        id: id.clone(),
    };

    let url = format!(
        "{}/{}.xml",
        state.config.jma.data_base_url.trim_end_matches('/'),
        id
    );

    let body = match fetch_bytes(&state.client, &url).await {
        Ok(body) => body,
        Err(e) => {
            tracing::warn!(error = %e, id = %id, "entity fetch failed");
            return;
        }
    };

    // メタ抽出(失敗しても本文はキャッシュ対象とする)
    let entity_meta = std::str::from_utf8(&body)
        .ok()
        .and_then(|xml| entity_parse::parse_entity_meta(xml).ok())
        .unwrap_or_default();
    let meta = ItemMeta {
        id: id.clone(),
        title: entity_meta.title,
        updated: entity_meta.report_date_time,
        author: entity_meta.publishing_office,
        content: entity_meta.headline_text,
        link: url,
    };

    let event = Event {
        source: EventSource::JmaFeed,
        dedup_key: DedupKey::composite(id.clone(), meta.updated.clone(), &body),
        xml_body: body,
        meta,
    };
    if state.event_tx.send(event).await.is_err() {
        tracing::warn!(id = %id, "aggregator channel closed; fetched entity dropped");
    } else {
        tracing::debug!(id = %id, "entity event sent to aggregator");
    }
}

async fn fetch_bytes(client: &reqwest::Client, url: &str) -> Result<bytes::Bytes, UpstreamError> {
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(UpstreamError::Status(status));
    }
    Ok(response.bytes().await?)
}
