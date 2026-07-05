//! 初期一覧取得 + キャッシュミス時のバックグラウンド実体取得(singleflight)。

use std::cmp::min;
use std::collections::HashSet;
use std::time::Duration;

use crate::config::Config;
use crate::error::UpstreamError;
use crate::jma::{entity_parse, feed_parse};
use crate::state::SharedState;
use crate::types::{DedupKey, Event, EventSource, ItemMeta};

const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// JMAの初期一覧(Atom)をリトライ付きで取得しパースする。
/// 長期フィード(eqvol_l.xml)→短期フィード(eqvol.xml)の順に取得し、
/// IDで重複排除マージのうえ電文種別でフィルターする。
/// 長期フィードの失敗は非致命(ベストエフォートのバックフィル)、
/// 短期フィードの失敗は従来どおり致命。
/// HTTP公開前に完了必須。実体のプリフェッチは行わない。
pub async fn load_initial_feed(
    client: &reqwest::Client,
    config: &Config,
) -> Result<Vec<ItemMeta>, UpstreamError> {
    // 長期フィード: 失敗しても空リストで続行する(ベストエフォート)
    let long_items = match fetch_feed_with_retry(client, config, &config.jma.long_feed_url, "long")
        .await
    {
        Ok(items) => items,
        Err(e) => {
            tracing::warn!(error = %e, url = %config.jma.long_feed_url, "long feed unavailable; continuing without backfill");
            Vec::new()
        }
    };

    // 短期フィード: 失敗は致命(従来動作を維持)
    let short_items = fetch_feed_with_retry(client, config, &config.jma.feed_url, "short").await?;

    let long_count = long_items.len();
    let short_count = short_items.len();
    let merged = merge_feeds(short_items, long_items);
    let merged_count = merged.len();
    let filtered = filter_by_types(merged, &config.jma.telegram_types);

    tracing::info!(
        long = long_count,
        short = short_count,
        merged = merged_count,
        after_filter = filtered.len(),
        "initial feeds loaded"
    );
    Ok(filtered)
}

/// 単一フィードURLをリトライ付きで取得しパースする。
/// attempts = retry_attempts.max(1)、backoffは初期値から倍々(上限 MAX_BACKOFF)。
async fn fetch_feed_with_retry(
    client: &reqwest::Client,
    config: &Config,
    url: &str,
    label: &str,
) -> Result<Vec<ItemMeta>, UpstreamError> {
    let attempts = config.jma.retry_attempts.max(1);
    let mut backoff = Duration::from_millis(config.jma.retry_initial_backoff_ms);
    let mut last_err = None;

    for attempt in 1..=attempts {
        match try_fetch_feed(client, url).await {
            Ok(items) => {
                tracing::info!(
                    entries = items.len(),
                    attempt,
                    feed = label,
                    "initial feed loaded"
                );
                return Ok(items);
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, max = attempts, feed = label, url = %url, "initial feed fetch failed");
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
    url: &str,
) -> Result<Vec<ItemMeta>, UpstreamError> {
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(UpstreamError::Status(status));
    }
    let body = response.text().await?;
    feed_parse::parse(&body)
}

/// 両フィードをIDで重複排除しつつマージし、updated降順で返す。
/// 重複時は preferred(短期フィード)側のentryを採用する。
fn merge_feeds(preferred: Vec<ItemMeta>, secondary: Vec<ItemMeta>) -> Vec<ItemMeta> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut merged: Vec<ItemMeta> = Vec::with_capacity(preferred.len() + secondary.len());
    for item in preferred.into_iter().chain(secondary) {
        if seen.insert(item.id.clone()) {
            merged.push(item);
        }
    }
    // JMAのupdatedは両フィードとも+09:00固定のRFC3339なので辞書順比較=時系列比較。
    merged.sort_by(|a, b| b.updated.cmp(&a.updated));
    merged
}

/// telegram_typesが空なら全通過。非空なら種別抽出不能なentryも除外する。
fn filter_by_types(items: Vec<ItemMeta>, types: &[String]) -> Vec<ItemMeta> {
    if types.is_empty() {
        return items;
    }
    items
        .into_iter()
        .filter(|m| {
            crate::jma::id::telegram_type(&m.id)
                .is_some_and(|t| types.iter().any(|c| c.eq_ignore_ascii_case(t)))
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// テスト用の最小 ItemMeta を作る。
    fn meta(id: &str, updated: &str) -> ItemMeta {
        ItemMeta {
            id: id.to_string(),
            updated: updated.to_string(),
            ..ItemMeta::default()
        }
    }

    fn codes() -> Vec<String> {
        [
            "VXSE51", "VXSE52", "VXSE53", "VXSE56", "VXSE60", "VXSE61", "VXSE62", "VYSE50",
            "VYSE51", "VYSE52", "VYSE60", "VTSE41", "VTSE51", "VTSE52", "VZSE40",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn merge_dedups_and_prefers_short_feed() {
        let mut short_dup = meta("dup", "2026-07-05T10:00:00+09:00");
        short_dup.title = "short".to_string();
        let short = vec![short_dup];

        let mut long_dup = meta("dup", "2026-07-05T10:00:00+09:00");
        long_dup.title = "long".to_string();
        let long = vec![long_dup, meta("only-long", "2026-07-05T09:00:00+09:00")];

        let merged = merge_feeds(short, long);
        assert_eq!(merged.len(), 2);
        // 重複IDは短期フィード側を採用
        assert_eq!(merged[0].id, "dup");
        assert_eq!(merged[0].title, "short");
        assert_eq!(merged[1].id, "only-long");
    }

    #[test]
    fn merge_sorts_updated_descending() {
        let short = vec![meta("s1", "2026-07-05T10:00:00+09:00")];
        let long = vec![
            meta("l1", "2026-07-05T08:00:00+09:00"),
            meta("l2", "2026-07-05T11:00:00+09:00"),
        ];
        let merged = merge_feeds(short, long);
        let ids: Vec<&str> = merged.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["l2", "s1", "l1"]);
    }

    #[test]
    fn filter_keeps_listed_types_and_drops_others() {
        let items = vec![
            meta(
                "20260705050045_0_VXSE53_010000",
                "2026-07-05T10:00:00+09:00",
            ),
            meta(
                "20260705050045_0_VXSE41_010000",
                "2026-07-05T10:00:00+09:00",
            ),
            meta(
                "ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b",
                "2026-07-05T10:00:00+09:00",
            ),
        ];
        let filtered = filter_by_types(items, &codes());
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "20260705050045_0_VXSE53_010000");
    }

    #[test]
    fn filter_matches_case_insensitively() {
        let items = vec![meta(
            "20260705050045_0_VXSE53_010000",
            "2026-07-05T10:00:00+09:00",
        )];
        let types = vec!["vxse53".to_string()];
        let filtered = filter_by_types(items, &types);
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn empty_types_passes_everything_through() {
        let items = vec![
            meta(
                "ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b",
                "2026-07-05T10:00:00+09:00",
            ),
            meta(
                "20260705050045_0_VXSE41_010000",
                "2026-07-05T10:00:00+09:00",
            ),
        ];
        let filtered = filter_by_types(items.clone(), &[]);
        assert_eq!(filtered, items);
    }
}
