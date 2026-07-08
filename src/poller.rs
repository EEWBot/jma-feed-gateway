//! 全WS切断中のフォールバックpolling。
//!
//! 毎分 `poll.offset_secs` 秒(壁時計基準)に dmdata telegram.list を1ページ取得し、
//! 新規電文を telegram.data v1 から取得してfeedへpublishする。
//! WSが1本でも接続中のtickは何もしない(poll_active=false)。
//!
//! 不変条件:
//! - fetch失敗・上限超過で未処理のentryは `pending` に保持し(seen未登録)、
//!   次tickでwatermark再選別を**通さず**処理する。watermarkは新しい項目の
//!   publishで前進するため、再選別に通すと古い持ち越し分が既配信扱いで
//!   永久に失われる。
//! - 上流リストの一覧から消えたpendingは破棄する(滞留の自然な上限)。
//! - WS復帰後もpendingは処理し切る(WSは切断中に発行された電文を再配信しない)。
//! - poll由来のEventは `DedupKey::TelegramId`(WSと同一ID)を使うため、
//!   WS復帰後に届く重複電文は aggregator の `seen` dedupeがdropする。

use std::sync::atomic::Ordering;
use std::time::Duration;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::DmdataError;
use crate::fetcher;
use crate::state::{AppState, SharedState};
use crate::types::{DedupKey, Event, EventSource, ItemMeta};

/// pollerの状態。壁時計ループ(`run`)とは分離してテスト可能にする。
#[doc(hidden)]
pub struct Poller {
    state: SharedState,
    /// 処理済みdmdata電文ID(TTLは `cache.seen_ttl_secs` を流用)。
    seen_ids: moka::sync::Cache<String, ()>,
    /// 未処理の持ち越しentry(fetch失敗・entry_fetch_limit超過)。updated昇順。
    /// watermark再選別を通さずに次tickで処理する。
    pending: Vec<ItemMeta>,
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
            seen_ids,
            pending: Vec::new(),
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

    /// 1 tick分のpoll。成功で poll_active=true、失敗で false。
    /// 成功時はpublish件数を返す。
    #[doc(hidden)]
    pub async fn poll_once(&mut self) -> Result<usize, DmdataError> {
        let result = self.poll_inner().await;
        self.set_active(result.is_ok());
        result
    }

    async fn poll_inner(&mut self) -> Result<usize, DmdataError> {
        let config = self.state.config.clone();

        // telegram.list を1ページだけ取得する(条件付きGETは無い)。
        // nextPooling トークンは使わない — WS復帰でpollingが止まると陳腐化する
        // ため、ステートレスなwatermark方式で候補を選別する
        let classification = config.dmdata.classifications.join(",");
        let page = self
            .state
            .dmdata_api
            .telegram_list(&classification, None, fetcher::LIST_PAGE_LIMIT)
            .await?;
        let items: Vec<ItemMeta> = page
            .items
            .iter()
            .filter_map(|item| fetcher::select_item(item, &config.dmdata.types))
            .collect();

        // 上流リストの一覧から消えたpendingは破棄(滞留の自然な上限)
        self.pending.retain(|p| items.iter().any(|m| m.id == p.id));
        // pending済みIDはfetch対象と確定済み — watermark再選別を通さない。
        // watermarkは新項目のpublishで前進するため、再選別に通すと古い
        // 持ち越し分が既配信扱いになり永久に失われる
        let items: Vec<ItemMeta> = items
            .into_iter()
            .filter(|m| self.pending.iter().all(|p| p.id != m.id))
            .collect();

        // watermark: aggregatorが単調clamp済みのフィードLast-Modified
        let watermark = self.state.feed.load().last_modified;
        let slack = Duration::from_secs(config.poll.watermark_slack_secs);
        let new_candidates = select_candidates(items, watermark, slack, &self.seen_ids);

        let mut candidates = std::mem::take(&mut self.pending);
        candidates.extend(new_candidates);
        // updated は select_item で+09:00へ正規化済みのRFC3339なので辞書順比較=時系列比較
        candidates.sort_by(|a, b| a.updated.cmp(&b.updated));

        // バースト保護: 最新 entry_fetch_limit 件を処理し、古い側はpendingへ戻す
        let deferred = candidates
            .len()
            .saturating_sub(config.poll.entry_fetch_limit);
        if deferred > 0 {
            tracing::warn!(deferred, "poll candidates exceed entry_fetch_limit");
            self.pending.extend(candidates.drain(..deferred));
        }

        let published = self.publish_candidates(candidates).await;

        tracing::info!(
            published,
            pending = self.pending.len(),
            "poll tick completed"
        );
        Ok(published)
    }

    /// 候補(updated昇順)を実体fetch→publishする。fetch失敗分は `pending` へ
    /// 戻す(seen未登録)。publish件数を返す。
    async fn publish_candidates(&mut self, candidates: Vec<ItemMeta>) -> usize {
        let mut published = 0usize;
        for meta in candidates {
            let body = match self.state.dmdata_api.telegram_get(&meta.id).await {
                Ok(body) => body,
                Err(e) => {
                    // seen登録せずpendingへ → 次tickでwatermark再選別を通さずリトライ
                    tracing::warn!(error = %e, id = %meta.id, "poll entry fetch failed");
                    self.pending.push(meta);
                    continue;
                }
            };
            let id = meta.id.clone();
            let event = Event {
                source: EventSource::DmdataPoll,
                // WSと同じdmdata電文ID — WS復帰時・poll重複時のdedupが `seen` で成立する
                dedup_key: DedupKey::TelegramId(id.clone()),
                xml_body: body,
                meta,
            };
            if self.state.event_tx.send(event).await.is_err() {
                // runが次tickの is_closed チェックで終了する
                tracing::warn!("aggregator channel closed; polled entry dropped");
                return published;
            }
            self.seen_ids.insert(id, ());
            published += 1;
        }
        published
    }

    /// poll対象外のtick(WS復帰後)で持ち越し分のみ処理する。
    /// WSは切断中に発行された電文を再配信しないため、pendingを放置すると
    /// その電文は次のアウトエージまで(リスト保持期間を過ぎれば永久に)失われる。
    #[doc(hidden)]
    pub async fn drain_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let mut candidates = std::mem::take(&mut self.pending);
        let deferred = candidates
            .len()
            .saturating_sub(self.state.config.poll.entry_fetch_limit);
        if deferred > 0 {
            self.pending.extend(candidates.drain(..deferred));
        }
        let published = self.publish_candidates(candidates).await;
        tracing::info!(
            published,
            pending = self.pending.len(),
            "pending drained while ws connected"
        );
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

/// リストentryからfetch候補を選ぶ。
/// - seen済みIDは除外
/// - `updated < watermark - slack` は既配信としてseen登録のうえ除外
/// - 同秒以上・パース不能updatedは安全側で候補に残す(fail-open。
///   重複は後段のaggregator `seen`(TelegramId)dedupeが吸収する)
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
    // updated は select_item で+09:00へ正規化済みのRFC3339なので辞書順比較=時系列比較
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

        // WSが1本でも接続中ならpollしない(ただし持ち越し分は処理し切る —
        // WSは切断中に発行された電文を再配信しない)
        if !all_ws_down(&state) {
            poller.set_active(false);
            poller.drain_pending().await;
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
        // fail-open: 後段のaggregator seen(TelegramId)dedupeが吸収する
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
