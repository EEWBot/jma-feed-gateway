//! 全WS切断中のフォールバックpollingと、WS復帰時のcatch-up poll。
//!
//! `poll.interval_secs` 秒ごとに dmdata telegram.list を1ページ取得し、
//! 新規電文をmeta-onlyのEventとしてfeedへpublishする(実体はここでは取得しない)。
//! 実体は初回HTTPアクセス時にCacheFill経路(singleflight + `[rate_limit]`)で
//! 遅延取得される。
//! 全WS復帰時は `ws_recovered` 通知で即座にcatch-up pollを1回走らせ、
//! 切断窓 [切断, 復帰] の取り逃しをtick時刻を待たずに埋める。
//!
//! 不変条件:
//! - 候補選別は共有Deduper(`state.deduper`)+ `state.feed_ids`(feed在中ID)の
//!   事前フィルタのみ。pollerは独自のseen状態を持たず、publish後のseen登録も
//!   aggregatorに任せる。この事前フィルタは「mpscチャネル在中でaggregator未処理」
//!   のWS eventを見逃しうるが、帰結は無駄なpublish 1回と後段dropのみで、
//!   フィードの正しさ・重複には影響しない(正しさはaggregatorの
//!   `check_and_insert` が唯一の判定点として保証する)。
//! - backlogフラグ契約: list取得に失敗したtickだけ `backlog = true`。
//!   backlogがある限りWS接続中でもtickごとにpollを続ける
//!   (WSは切断中に発行された電文を再配信しないため、pollで捌き切る)。
//!   成功したtickで自動クリアされる。
//! - WS接続中のpoll(backlog消化・catch-up)では `poll_active` を更新しない
//!   (readinessの意味「fallbackが生きた供給源」を保つ)。
//! - poll由来のEventは `DedupKey::TelegramId`(WSと同一ID)を使うため、
//!   WS復帰後に届く重複電文は aggregator のdedupeがdropする。

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::error::DmdataError;
use crate::fetcher;
use crate::state::SharedState;
use crate::types::{DedupKey, Event, EventSource, ItemMeta};

/// pollerの状態。壁時計ループ(`run`)とは分離してテスト可能にする。
#[doc(hidden)]
pub struct Poller {
    state: SharedState,
    /// 前tickのlist取得が失敗し、取り逃しが残った可能性。
    /// WS接続中でもtickでpollを続ける根拠になる。
    backlog: bool,
    /// 遷移ログ用: 直前tickでpolling稼働していたか。
    was_polling: bool,
}

impl Poller {
    #[doc(hidden)]
    pub fn new(state: SharedState) -> Self {
        Self {
            state,
            backlog: false,
            was_polling: false,
        }
    }

    /// backlog(未処理候補の持ち越し)が残っているか。
    #[doc(hidden)]
    pub fn has_backlog(&self) -> bool {
        self.backlog
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

    /// 1 tick分のpoll。`fallback`(全WS切断中)のときだけ poll_active を
    /// 成否で更新する。list取得失敗は backlog=true にして次tickの
    /// リトライへ委ねる(catch-upの取り逃しゼロ保証が単発通知に依存しない)。
    /// 成功時はpublish件数を返す。
    #[doc(hidden)]
    pub async fn poll_once(&mut self, fallback: bool) -> Result<usize, DmdataError> {
        let result = self.poll_inner().await;
        if result.is_err() {
            self.backlog = true;
        }
        if fallback {
            self.set_active(result.is_ok());
        }
        result
    }

    async fn poll_inner(&mut self) -> Result<usize, DmdataError> {
        let config = self.state.config.clone();

        // telegram.list を1ページだけ取得する(条件付きGETは無い)。
        // nextPooling トークンは使わない — WS復帰でpollingが止まると陳腐化する
        let classification = config.dmdata.classifications.join(",");
        let page = self
            .state
            .dmdata_api
            .telegram_list(&classification, None, fetcher::LIST_PAGE_LIMIT)
            .await?;

        // 候補選別: 共有Deduper既知(publish済み)または feed_ids 在中(feed在中)の
        // IDは無駄なpublishを避けるためskipする
        let mut candidates: Vec<ItemMeta> = page
            .items
            .iter()
            .filter_map(|item| fetcher::select_item(item, &config.dmdata.types))
            .filter(|meta| {
                !self
                    .state
                    .deduper
                    .contains(&DedupKey::TelegramId(meta.id.clone()))
                    && !self.state.feed_ids.contains(&meta.id)
            })
            .collect();
        // updated は select_item で+09:00へ正規化済みのRFC3339なので辞書順比較=時系列比較
        candidates.sort_by(|a, b| a.updated.cmp(&b.updated));

        let mut published = 0usize;
        for meta in candidates {
            // meta-onlyでpublishする。実体は初回アクセス時にCacheFill経路
            // (singleflight + rate limiter)で遅延取得される
            let event = Event {
                source: EventSource::DmdataPoll,
                // WSと同じdmdata電文ID — WS復帰時・poll重複時のdedupが成立する
                dedup_key: DedupKey::TelegramId(meta.id.clone()),
                xml_body: None,
                meta,
            };
            if self.state.event_tx.send(event).await.is_err() {
                // runが次tickの is_closed チェックで終了する
                tracing::warn!("aggregator channel closed; polled entry dropped");
                return Ok(published);
            }
            // seen登録はしない(書き込みはaggregatorのみ)
            published += 1;
        }

        // listを取り切れた=取り逃しなし。backlogはlist取得失敗のみが立てる
        self.backlog = false;
        tracing::info!(published, "poll tick completed");
        Ok(published)
    }
}

/// pollerタスク本体。mainからspawnする。
/// `interval_secs` 秒周期でpollし、`ws_recovered` 通知でのwakeは全断エピソードからの
/// 復帰を意味し、tick時刻を待たずにcatch-up pollを走らせる(コールドスタートの初回
/// 接続はエピソード無しのため通知されない)。
pub async fn run(state: SharedState) {
    let poll_config = &state.config.poll;
    if !poll_config.enabled {
        tracing::info!("poll fallback disabled by config");
        return;
    }
    let interval_secs = poll_config.interval_secs;
    tracing::info!(interval_secs, "poll fallback task started");

    let mut poller = Poller::new(state.clone());
    loop {
        let wait = Duration::from_secs(interval_secs);
        let mut catch_up = false;
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = state.readiness.ws_recovered.notified() => {
                catch_up = true;
            }
        }

        // graceful shutdown: aggregator停止(チャネルクローズ)で終了
        if state.event_tx.is_closed() {
            poller.set_active(false);
            tracing::warn!("event channel closed; poll task exiting");
            return;
        }

        let fallback = state.readiness.all_ws_down();
        if !fallback {
            // WS接続中のpoll(catch-up / backlog消化)は poll_active を立てない
            poller.set_active(false);
            if !catch_up && !poller.has_backlog() {
                continue;
            }
        }

        if let Err(e) = poller.poll_once(fallback).await {
            // tick内リトライはしない(backlog経由で次tickが再試行する)
            tracing::warn!(error = %e, "poll failed; retrying next tick");
        }
    }
}
