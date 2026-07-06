//! 全WS切断中のフォールバックpolling。
//!
//! 毎分 `poll.offset_secs` 秒(壁時計基準)にJMA短期フィード(eqvol.xml)を
//! `If-Modified-Since` 付きconditional GETし、新規電文をfeedへpublishする。
//! WSが1本でも接続中のtickは何もしない(poll_active=false)。
//!
//! 不変条件:
//! - `last_modified` は前回200の `Last-Modified` ヘッダ生値をそのまま保持し、
//!   再フォーマットせずに `If-Modified-Since` へ返す(クロックスキュー免疫)。
//! - entry の実体fetchに失敗した場合は `seen_ids` へ登録しない(次分リトライ)。
//! - WS復帰後にWSから届く重複電文は aggregator の本文ハッシュdedupeがdropする。

use std::sync::atomic::Ordering;
use std::time::Duration;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::UpstreamError;
use crate::fetcher;
use crate::jma::feed_parse;
use crate::state::{AppState, SharedState};
use crate::types::{DedupKey, Event, EventSource, ItemMeta};

/// poll_once 1回分の結果。テストからの観測用に公開する。
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// 304 Not Modified(フィード未更新)。
    NotModified,
    /// 200 を処理し、この件数をpublishした。
    Published(usize),
}

/// pollerの状態。壁時計ループ(`run`)とは分離してテスト可能にする。
#[doc(hidden)]
pub struct Poller {
    state: SharedState,
    /// 前回200の `Last-Modified` ヘッダ生値。そのまま `If-Modified-Since` に返す
    /// (再フォーマット禁止 = クロックスキュー免疫)。
    last_modified: Option<String>,
    /// 処理済みJMAフィードentry ID(TTLは `cache.seen_ttl_secs` を流用)。
    seen_ids: moka::sync::Cache<String, ()>,
    /// 遷移ログ用: 直前tickでpolling稼働していたか。
    was_polling: bool,
}

impl Poller {
    #[doc(hidden)]
    pub fn new(state: SharedState) -> Self {
        let seen_ids = moka::sync::Cache::builder()
            .max_capacity(65_536)
            .time_to_live(Duration::from_secs(state.config.cache.seen_ttl_secs))
            .build();
        Self {
            state,
            last_modified: None,
            seen_ids,
            was_polling: false,
        }
    }

    /// poll_active を更新し、遷移時のみログを出す。
    fn set_active(&mut self, active: bool) {
        self.state
            .readiness
            .poll_active
            .store(active, Ordering::Relaxed);
        if active && !self.was_polling {
            tracing::info!("poll fallback activated (all ws down)");
        } else if !active && self.was_polling {
            tracing::info!("poll fallback deactivated");
        }
        self.was_polling = active;
    }

    /// 1 tick分のpoll。成功(200/304とも)で poll_active=true、失敗で false。
    #[doc(hidden)]
    pub async fn poll_once(&mut self) -> Result<PollOutcome, UpstreamError> {
        let result = self.poll_inner().await;
        self.set_active(result.is_ok());
        result
    }

    async fn poll_inner(&mut self) -> Result<PollOutcome, UpstreamError> {
        let config = self.state.config.clone();

        // conditional GET: 前回のLast-Modified生値をそのまま返す
        let mut request = self.state.client.get(&config.jma.feed_url);
        if let Some(lm) = &self.last_modified {
            request = request.header(reqwest::header::IF_MODIFIED_SINCE, lm);
        }
        let response = request.send().await?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            tracing::debug!("poll: feed not modified");
            return Ok(PollOutcome::NotModified);
        }
        if !response.status().is_success() {
            return Err(UpstreamError::Status(response.status()));
        }
        // ヘッダ無しの200では旧値を維持する
        if let Some(lm) = response
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
        {
            self.last_modified = Some(lm.to_string());
        }

        let body = response.text().await?;
        let items = feed_parse::parse(&body)?;
        let items = fetcher::filter_by_types(items, &config.jma.telegram_types);

        // watermark: aggregatorが単調clamp済みのフィードLast-Modified
        let watermark = self.state.feed.load().last_modified;
        let slack = Duration::from_secs(config.poll.watermark_slack_secs);
        let mut candidates = select_candidates(items, watermark, slack, &self.seen_ids);

        // バースト保護: 最新 entry_fetch_limit 件を残す(古い側はseen未登録のまま
        // 次分以降に持ち越し)
        if candidates.len() > config.poll.entry_fetch_limit {
            let deferred = candidates.len() - config.poll.entry_fetch_limit;
            tracing::warn!(deferred, "poll candidates exceed entry_fetch_limit");
            candidates.drain(..deferred);
        }

        // 昇順にpublishする(feed先頭が最新になり、容量evictionが最古から落ちる)
        let mut published = 0usize;
        for meta in candidates {
            let url = if meta.link.is_empty() {
                format!(
                    "{}/{}.xml",
                    config.jma.data_base_url.trim_end_matches('/'),
                    meta.id
                )
            } else {
                meta.link.clone()
            };
            let body = match fetcher::fetch_bytes(&self.state.client, &url).await {
                Ok(body) => body,
                Err(e) => {
                    // seen登録しない → 次分リトライ
                    tracing::warn!(error = %e, id = %meta.id, "poll entry fetch failed");
                    continue;
                }
            };
            let id = meta.id.clone();
            let event = Event {
                source: EventSource::JmaPoll,
                dedup_key: DedupKey::composite(id.clone(), meta.updated.clone(), &body),
                xml_body: body,
                meta,
            };
            if self.state.event_tx.send(event).await.is_err() {
                tracing::warn!("aggregator channel closed; polled entry dropped");
                break;
            }
            self.seen_ids.insert(id, ());
            published += 1;
        }

        tracing::info!(published, "poll tick completed");
        Ok(PollOutcome::Published(published))
    }
}

/// 次のpoll時刻(毎分 `offset_secs` 秒)までの待ち時間。戻り値は (0, 60s]。
/// ちょうど offset 秒なら次分まで待つ(純関数)。
fn duration_until_next_tick(now: OffsetDateTime, offset_secs: u64) -> Duration {
    const MINUTE_NANOS: u64 = 60_000_000_000;
    let in_minute = u64::from(now.second()) * 1_000_000_000 + u64::from(now.nanosecond());
    let target = offset_secs * 1_000_000_000;
    let mut wait = (target + MINUTE_NANOS - in_minute) % MINUTE_NANOS;
    if wait == 0 {
        wait = MINUTE_NANOS;
    }
    Duration::from_nanos(wait)
}

/// 全WS切断中か(1本も接続していない)。
fn all_ws_down(state: &AppState) -> bool {
    !state
        .readiness
        .ws_connected
        .iter()
        .any(|b| b.load(Ordering::Relaxed))
}

/// フィードentryからfetch候補を選ぶ。
/// - seen済みIDは除外
/// - `updated < watermark - slack` は既配信としてseen登録のうえ除外
/// - 同秒以上・パース不能updatedは安全側で候補に残す(fail-open。
///   重複は後段の本文ハッシュdedupeが吸収する)
/// - updated昇順で返す
fn select_candidates(
    entries: Vec<ItemMeta>,
    watermark: Option<OffsetDateTime>,
    slack: Duration,
    seen: &moka::sync::Cache<String, ()>,
) -> Vec<ItemMeta> {
    let cutoff = watermark.map(|w| w - slack);
    let mut candidates: Vec<ItemMeta> = entries
        .into_iter()
        .filter(|meta| {
            if seen.contains_key(&meta.id) {
                return false;
            }
            if let Some(cutoff) = cutoff
                && let Ok(updated) = OffsetDateTime::parse(&meta.updated, &Rfc3339)
                && updated < cutoff
            {
                // watermark以前の既配信entry。再fetchしないようseen登録して除外
                seen.insert(meta.id.clone(), ());
                return false;
            }
            true
        })
        .collect();
    // JMAのupdatedは+09:00固定のRFC3339なので辞書順比較=時系列比較
    candidates.sort_by(|a, b| a.updated.cmp(&b.updated));
    candidates
}

/// pollerタスク本体。mainからspawnする。
/// 毎周回、壁時計から次tickまでの待ち時間を再計算する(ドリフト自己補正)。
pub async fn run(state: SharedState) {
    let poll_config = &state.config.poll;
    if !poll_config.enabled {
        tracing::info!("poll fallback disabled by config");
        return;
    }
    let offset_secs = poll_config.offset_secs;
    tracing::info!(offset_secs, "poll fallback task started");

    let mut poller = Poller::new(state.clone());
    loop {
        let wait = duration_until_next_tick(OffsetDateTime::now_utc(), offset_secs);
        tokio::time::sleep(wait).await;

        // graceful shutdown: aggregator停止(チャネルクローズ)で終了
        if state.event_tx.is_closed() {
            poller.set_active(false);
            tracing::warn!("event channel closed; poll task exiting");
            return;
        }

        // WSが1本でも接続中ならpollしない
        if !all_ws_down(&state) {
            poller.set_active(false);
            continue;
        }

        if let Err(e) = poller.poll_once().await {
            // intra-minuteリトライはしない(次tickで再試行)
            tracing::warn!(error = %e, "poll failed; retrying next tick");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(hms: (u8, u8, u8), nanos: u32) -> OffsetDateTime {
        time::macros::datetime!(2026-07-06 00:00:00 UTC)
            .replace_time(time::Time::from_hms_nano(hms.0, hms.1, hms.2, nanos).unwrap())
    }

    #[test]
    fn next_tick_before_offset_waits_until_offset() {
        // :05.5 → 14.5s
        let wait = duration_until_next_tick(at((12, 34, 5), 500_000_000), 20);
        assert_eq!(wait, Duration::from_millis(14_500));
    }

    #[test]
    fn next_tick_after_offset_waits_for_next_minute() {
        // :45 → 35s
        let wait = duration_until_next_tick(at((12, 34, 45), 0), 20);
        assert_eq!(wait, Duration::from_secs(35));
    }

    #[test]
    fn next_tick_exactly_at_offset_waits_full_minute() {
        let wait = duration_until_next_tick(at((12, 34, 20), 0), 20);
        assert_eq!(wait, Duration::from_secs(60));
    }

    #[test]
    fn next_tick_offset_zero() {
        let wait = duration_until_next_tick(at((12, 34, 0), 0), 0);
        assert_eq!(wait, Duration::from_secs(60));
        let wait = duration_until_next_tick(at((12, 34, 59), 0), 0);
        assert_eq!(wait, Duration::from_secs(1));
    }

    #[test]
    fn next_tick_accounts_for_nanoseconds() {
        // :19.999999999 → 1ns
        let wait = duration_until_next_tick(at((12, 34, 19), 999_999_999), 20);
        assert_eq!(wait, Duration::from_nanos(1));
    }

    fn meta(id: &str, updated: &str) -> ItemMeta {
        ItemMeta {
            id: id.to_string(),
            updated: updated.to_string(),
            ..ItemMeta::default()
        }
    }

    fn seen_cache() -> moka::sync::Cache<String, ()> {
        moka::sync::Cache::builder().max_capacity(1024).build()
    }

    fn watermark() -> OffsetDateTime {
        // = 2026-07-05T04:10:00+09:00
        time::macros::datetime!(2026-07-04 19:10:00 UTC)
    }

    #[test]
    fn candidates_older_than_watermark_minus_slack_are_skipped_and_marked_seen() {
        let seen = seen_cache();
        let entries = vec![
            meta("old", "2026-07-05T03:59:59+09:00"), // watermark-600sより古い
            meta("new", "2026-07-05T04:11:00+09:00"),
        ];
        let selected =
            select_candidates(entries, Some(watermark()), Duration::from_secs(600), &seen);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "new");
        // 既配信扱いでseen登録される
        assert!(seen.contains_key("old"));
        assert!(!seen.contains_key("new"));
    }

    #[test]
    fn candidates_within_slack_are_kept() {
        let seen = seen_cache();
        // watermark-600s ちょうど(同秒)はkeep
        let entries = vec![meta("boundary", "2026-07-05T04:00:00+09:00")];
        let selected =
            select_candidates(entries, Some(watermark()), Duration::from_secs(600), &seen);
        assert_eq!(selected.len(), 1);
        assert!(!seen.contains_key("boundary"));
    }

    #[test]
    fn candidates_with_unparseable_updated_are_kept() {
        let seen = seen_cache();
        let entries = vec![meta("weird", "not-a-date")];
        let selected =
            select_candidates(entries, Some(watermark()), Duration::from_secs(600), &seen);
        // fail-open: 後段の本文ハッシュdedupeが吸収する
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn candidates_already_seen_are_skipped() {
        let seen = seen_cache();
        seen.insert("done".to_string(), ());
        let entries = vec![
            meta("done", "2026-07-05T04:11:00+09:00"),
            meta("todo", "2026-07-05T04:12:00+09:00"),
        ];
        let selected =
            select_candidates(entries, Some(watermark()), Duration::from_secs(600), &seen);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "todo");
    }

    #[test]
    fn candidates_are_sorted_ascending_by_updated() {
        let seen = seen_cache();
        let entries = vec![
            meta("c", "2026-07-05T04:13:00+09:00"),
            meta("a", "2026-07-05T04:11:00+09:00"),
            meta("b", "2026-07-05T04:12:00+09:00"),
        ];
        let selected =
            select_candidates(entries, Some(watermark()), Duration::from_secs(600), &seen);
        let ids: Vec<&str> = selected.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn no_watermark_keeps_everything() {
        let seen = seen_cache();
        let entries = vec![
            meta("very-old", "2000-01-01T00:00:00+09:00"),
            meta("new", "2026-07-05T04:11:00+09:00"),
        ];
        let selected = select_candidates(entries, None, Duration::from_secs(600), &seen);
        assert_eq!(selected.len(), 2);
    }
}
