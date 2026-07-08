//! アプリケーション共有状態。HTTP層は読み取り専用でアクセスする。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dashmap::{DashMap, DashSet};
use tokio::sync::{Notify, mpsc, watch};

use crate::config::Config;
use crate::dmdata::api::DmdataApi;
use crate::types::{DedupKey, EntityEntry, Event, FeedSnapshot};

/// キャッシュミス待機者へ完成entryを配るwatch。None=取得中、Some=完成。
/// Sender drop(None のまま)=取得失敗のシグナル。
pub type InflightRx = watch::Receiver<Option<Arc<EntityEntry>>>;
pub type InflightTx = watch::Sender<Option<Arc<EntityEntry>>>;

/// 重複排除。TTL付きのseenキャッシュ。
/// キーはソース固有ID(dmdata電文ID等)。WS / poll / warmupの全経路が同一の
/// dmdata電文IDを持つため、これだけでクロス経路dedupeが成立する。
/// 書き込み(insert)は aggregator と起動時seedのみ。pollerは事前フィルタとして
/// `contains` を読むだけ(single-writer維持)。
pub struct Deduper {
    seen: moka::sync::Cache<DedupKey, ()>,
}

impl Deduper {
    pub fn new(ttl: Duration) -> Self {
        Self {
            seen: moka::sync::Cache::builder()
                .max_capacity(65_536)
                .time_to_live(ttl)
                .build(),
        }
    }

    /// 未見なら登録して true、既見なら false。
    pub fn check_and_insert(&self, key: &DedupKey) -> bool {
        if self.seen.contains_key(key) {
            return false;
        }
        self.seen.insert(key.clone(), ());
        true
    }

    /// 既見か(登録はしない)。
    pub fn contains(&self, key: &DedupKey) -> bool {
        self.seen.contains_key(key)
    }

    /// 登録のみ(起動時seed用)。
    pub fn insert(&self, key: DedupKey) {
        self.seen.insert(key, ());
    }
}

/// スライディングウィンドウ式レートリミッタ。
/// 外部リクエスト起因のアウトバウンドfetch(dmdata telegram.data)の保護に使う。
pub struct RateLimiter {
    limit: usize,
    window: Duration,
    timestamps: Mutex<VecDeque<Instant>>,
}

impl RateLimiter {
    pub fn new(limit: usize, window: Duration) -> Self {
        Self {
            limit,
            window,
            timestamps: Mutex::new(VecDeque::new()),
        }
    }

    /// window超過分をpop → len < limit なら now を積んで true。
    /// 同期ロックのため .await 跨ぎでの呼び出し禁止。
    pub fn try_acquire(&self) -> bool {
        let now = Instant::now();
        let mut timestamps = self.timestamps.lock().expect("rate limiter lock poisoned");
        while let Some(front) = timestamps.front() {
            if now.duration_since(*front) >= self.window {
                timestamps.pop_front();
            } else {
                break;
            }
        }
        if timestamps.len() < self.limit {
            timestamps.push_back(now);
            true
        } else {
            false
        }
    }
}

/// readiness 状態。Ordering は Relaxed で十分(単なるフラグ)。
#[derive(Debug)]
pub struct Readiness {
    pub initial_feed_loaded: AtomicBool,
    pub aggregator_running: AtomicBool,
    /// WS接続状態。要素数は設定した ws_endpoints の本数(1〜2)と一致する。
    /// 更新は `mark_ws_connected` / `mark_ws_disconnected` 経由で行う。
    pub ws_connected: Box<[AtomicBool]>,
    /// フォールバックpolling稼働状態。poller が poll_once の成否で更新する。
    pub poll_active: AtomicBool,
    /// 全断エピソードからの復帰通知(pollerのcatch-up poll用)。
    /// Notifyのpermit(最大1)に積まれるため、poller処理中の通知も失われない。
    pub ws_recovered: Notify,
    /// 「全断エピソードが発生し、まだcatch-upしていない」内部状態。
    /// 初期値 false — 起動直後の未接続状態はエピソード扱いしない
    /// (コールドスタートでは初回接続時にcatch-upは走らない)。
    fully_down: AtomicBool,
}

impl Readiness {
    /// WS接続本数を指定して構築する。
    pub fn new(ws_count: usize) -> Self {
        Self {
            initial_feed_loaded: AtomicBool::new(false),
            aggregator_running: AtomicBool::new(false),
            ws_connected: (0..ws_count).map(|_| AtomicBool::new(false)).collect(),
            poll_active: AtomicBool::new(false),
            ws_recovered: Notify::new(),
            fully_down: AtomicBool::new(false),
        }
    }

    /// ready = 初期一覧取得済み && aggregator稼働中 &&
    /// (WSがいずれか接続中 || フォールバックpolling稼働中)。
    pub fn is_ready(&self) -> bool {
        self.initial_feed_loaded.load(Ordering::Relaxed)
            && self.aggregator_running.load(Ordering::Relaxed)
            && (self.ws_connected.iter().any(|b| b.load(Ordering::Relaxed))
                || self.poll_active.load(Ordering::Relaxed))
    }

    /// 全WS切断中か(1本も接続していない)。
    pub fn all_ws_down(&self) -> bool {
        !self.ws_connected.iter().any(|b| b.load(Ordering::Relaxed))
    }

    /// WS接続確立(startメッセージ受信)。全断エピソード後の初回接続なら
    /// `ws_recovered` を通知する(swapにより2本同時復帰でも通知は1回)。
    ///
    /// 既知の限界: 全断の瞬間に別接続のstartが割り込む数百ms級の競合窓では
    /// エピソードが記録されないことがあるが、その窓の電文は割り込んだWS購読
    /// 自体がカバーする。
    pub fn mark_ws_connected(&self, index: usize) {
        if let Some(flag) = self.ws_connected.get(index) {
            flag.store(true, Ordering::Relaxed);
        }
        if self.fully_down.swap(false, Ordering::Relaxed) {
            self.ws_recovered.notify_one();
        }
    }

    /// WS切断(セッション終了)。全断になったらエピソードを記録する
    /// (再接続ループの連続失敗による再呼び出しは冪等)。
    pub fn mark_ws_disconnected(&self, index: usize) {
        if let Some(flag) = self.ws_connected.get(index) {
            flag.store(false, Ordering::Relaxed);
        }
        if self.all_ws_down() {
            self.fully_down.store(true, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> ReadinessSnapshot {
        ReadinessSnapshot {
            feed: self.initial_feed_loaded.load(Ordering::Relaxed),
            aggregator: self.aggregator_running.load(Ordering::Relaxed),
            ws: self
                .ws_connected
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            poll: self.poll_active.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadinessSnapshot {
    pub feed: bool,
    pub aggregator: bool,
    pub ws: Vec<bool>,
    pub poll: bool,
}

pub struct AppState {
    pub config: Arc<Config>,
    /// 完成済みAtomフィード。更新は aggregator のみ。
    pub feed: ArcSwap<FeedSnapshot>,
    /// 実体XMLキャッシュ。value は Clone 必須のため `Arc<EntityEntry>`。
    pub entities: moka::future::Cache<String, Arc<EntityEntry>>,
    /// DMDATA由来でフィード在中のentry本体。aggregatorのみ書き込み。
    /// feedから溢れたら entities(moka) へ降格(TTL分の猶予付きで配信継続)。
    ///
    /// 不変条件: `pinned` のキー集合 = feed一覧(metas)中のdmdata由来ID。
    /// 上限は feed_entries(100件・数MB)に自然に抑えられる。
    /// 再起動後は pinned 空 + feedはJMAウォームアップIDのみで整合。
    pub pinned: DashMap<String, Arc<EntityEntry>>,
    /// singleflight 用の先着ガード + 待機者への配布口。
    /// fetch側は完成entryをsendするか、失敗時はSenderをdropする。
    /// InflightGuard により完了/失敗いずれでも必ずキーがremoveされる。
    pub inflight: DashMap<String, InflightRx>,
    /// publish済み電文のdedupキャッシュ(TTL = `cache.seen_ttl_secs`)。
    /// 書き込みは aggregator(+起動時seed)のみ。pollerは候補の事前フィルタ
    /// として読むだけ。
    pub deduper: Deduper,
    /// 現在のfeedメンバーシップ(warmup由来含む)。ミス時のアウトバウンドfetchの
    /// アローリスト。mainがaggregator起動前にseedし、以降はaggregatorのみ書く。
    /// 不変条件: キー集合 = aggregatorの `metas` のID集合。
    pub feed_ids: DashSet<String>,
    /// 外部リクエスト起因のアウトバウンドfetchのレートリミッタ(`[rate_limit]`)。
    /// poll由来entryもmeta-onlyでpublishされるため、実体は初回アクセス時に
    /// この制限下で遅延取得される。warmup(初期一覧)は実体を取得しないため対象外。
    pub fetch_limiter: RateLimiter,
    pub readiness: Readiness,
    /// DMDATA APIクライアント(warmup / キャッシュミス補充 / WS認可で共用)。
    pub dmdata_api: DmdataApi,
    /// aggregator への Event 送信口。fetch_entity はこれ経由で送る(single-writer 維持)。
    pub event_tx: mpsc::Sender<Event>,
    /// インスタンス起動時刻(RFC3339、構築時に1回だけ計算)。
    /// `X-Instance-Started` ヘッダとして返し、再起動検知に使う。
    pub started_at: String,
}

pub type SharedState = Arc<AppState>;

impl AppState {
    pub fn new(config: Arc<Config>, dmdata_api: DmdataApi, event_tx: mpsc::Sender<Event>) -> Self {
        let entities = moka::future::Cache::builder()
            .max_capacity(config.cache.entity_capacity)
            .time_to_live(Duration::from_secs(config.cache.entity_ttl_secs))
            .build();
        let started_at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("rfc3339 formatting cannot fail");
        let readiness = Readiness::new(config.dmdata.ws_endpoints.len());
        let deduper = Deduper::new(Duration::from_secs(config.cache.seen_ttl_secs));
        let fetch_limiter = RateLimiter::new(
            config.rate_limit.max_requests,
            Duration::from_secs(config.rate_limit.window_secs),
        );
        Self {
            config,
            feed: ArcSwap::from_pointee(FeedSnapshot::empty()),
            entities,
            pinned: DashMap::new(),
            inflight: DashMap::new(),
            deduper,
            feed_ids: DashSet::new(),
            fetch_limiter,
            readiness,
            dmdata_api,
            event_tx,
            started_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deduper_rejects_second_insert() {
        let deduper = Deduper::new(Duration::from_secs(60));
        let key = DedupKey::TelegramId("t1".into());
        assert!(deduper.check_and_insert(&key));
        assert!(!deduper.check_and_insert(&key));
        assert!(deduper.check_and_insert(&DedupKey::TelegramId("t2".into())));
    }

    #[test]
    fn deduper_composite_keys_differ_by_hash() {
        let deduper = Deduper::new(Duration::from_secs(60));
        let a = DedupKey::composite("e1", "2026-07-05T04:10:00+09:00", b"body-a");
        let b = DedupKey::composite("e1", "2026-07-05T04:10:00+09:00", b"body-b");
        assert!(deduper.check_and_insert(&a));
        assert!(deduper.check_and_insert(&b));
        assert!(!deduper.check_and_insert(&a));
    }

    #[test]
    fn deduper_contains_and_insert() {
        let deduper = Deduper::new(Duration::from_secs(60));
        let key = DedupKey::TelegramId("t1".into());
        assert!(!deduper.contains(&key));
        deduper.insert(key.clone());
        assert!(deduper.contains(&key));
        // seed済みキーは check_and_insert でも既見扱い
        assert!(!deduper.check_and_insert(&key));
    }

    #[test]
    fn rate_limiter_allows_up_to_limit_then_rejects() {
        let limiter = RateLimiter::new(3, Duration::from_secs(60));
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire(), "4th acquire must be rejected");
    }

    #[test]
    fn rate_limiter_allows_again_after_window() {
        let limiter = RateLimiter::new(1, Duration::from_millis(30));
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        std::thread::sleep(Duration::from_millis(50));
        // window経過分がpopされ再度取得できる
        assert!(limiter.try_acquire());
    }

    #[test]
    fn all_ws_down_truth_table() {
        let r = Readiness::new(2);
        // (false, false) → 全断
        assert!(r.all_ws_down());
        r.ws_connected[0].store(true, Ordering::Relaxed);
        // (true, false) → 接続あり
        assert!(!r.all_ws_down());
        r.ws_connected[1].store(true, Ordering::Relaxed);
        // (true, true) → 接続あり
        assert!(!r.all_ws_down());
        r.ws_connected[0].store(false, Ordering::Relaxed);
        // (false, true) → 接続あり
        assert!(!r.all_ws_down());
        r.ws_connected[1].store(false, Ordering::Relaxed);
        assert!(r.all_ws_down());
    }

    async fn notified_within_10ms(r: &Readiness) -> bool {
        tokio::time::timeout(Duration::from_millis(10), r.ws_recovered.notified())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn recovery_after_full_down_episode_notifies() {
        let r = Readiness::new(2);
        r.mark_ws_connected(0);
        r.mark_ws_disconnected(0); // 全断エピソード
        r.mark_ws_connected(1);
        assert!(notified_within_10ms(&r).await);
    }

    #[tokio::test]
    async fn connect_without_episode_does_not_notify() {
        let r = Readiness::new(2);
        // 起動直後の初回接続(未接続状態はエピソード扱いしない)
        r.mark_ws_connected(0);
        r.mark_ws_connected(1);
        assert!(!notified_within_10ms(&r).await);
    }

    #[tokio::test]
    async fn partial_down_does_not_notify_on_reconnect() {
        let r = Readiness::new(2);
        r.mark_ws_connected(0);
        r.mark_ws_connected(1);
        // 1本だけ切断→復帰: 全断を経ていないので通知なし
        r.mark_ws_disconnected(0);
        r.mark_ws_connected(0);
        assert!(!notified_within_10ms(&r).await);
    }

    #[tokio::test]
    async fn repeated_disconnects_then_recovery_notifies_once() {
        let r = Readiness::new(1);
        r.mark_ws_connected(0);
        // 再接続ループの連続失敗(冪等)
        r.mark_ws_disconnected(0);
        r.mark_ws_disconnected(0);
        r.mark_ws_disconnected(0);
        r.mark_ws_connected(0);
        assert!(notified_within_10ms(&r).await, "first wait must resolve");
        assert!(
            !notified_within_10ms(&r).await,
            "exactly one notification must be issued"
        );
    }
}
