//! アプリケーション共有状態。HTTP層は読み取り専用でアクセスする。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::types::{EntityEntry, Event, FeedSnapshot};

/// readiness 状態。Ordering は Relaxed で十分(単なるフラグ)。
#[derive(Debug)]
pub struct Readiness {
    pub initial_feed_loaded: AtomicBool,
    pub aggregator_running: AtomicBool,
    /// WS接続状態。要素数は設定した ws_endpoints の本数(1〜2)と一致する。
    pub ws_connected: Box<[AtomicBool]>,
}

impl Readiness {
    /// WS接続本数を指定して構築する。
    pub fn new(ws_count: usize) -> Self {
        Self {
            initial_feed_loaded: AtomicBool::new(false),
            aggregator_running: AtomicBool::new(false),
            ws_connected: (0..ws_count).map(|_| AtomicBool::new(false)).collect(),
        }
    }

    /// ready = 初期一覧取得済み && aggregator稼働中 && WSがいずれか接続中。
    pub fn is_ready(&self) -> bool {
        self.initial_feed_loaded.load(Ordering::Relaxed)
            && self.aggregator_running.load(Ordering::Relaxed)
            && self.ws_connected.iter().any(|b| b.load(Ordering::Relaxed))
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
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadinessSnapshot {
    pub feed: bool,
    pub aggregator: bool,
    pub ws: Vec<bool>,
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
    /// singleflight 用の先着ガード。fetch 完了/失敗時に必ず remove すること。
    pub inflight: DashMap<String, ()>,
    pub readiness: Readiness,
    pub client: reqwest::Client,
    /// aggregator への Event 送信口。fetch_entity はこれ経由で送る(single-writer 維持)。
    pub event_tx: mpsc::Sender<Event>,
    /// インスタンス起動時刻(RFC3339、構築時に1回だけ計算)。
    /// `X-Instance-Started` ヘッダとして返し、再起動検知に使う。
    pub started_at: String,
}

pub type SharedState = Arc<AppState>;

impl AppState {
    pub fn new(
        config: Arc<Config>,
        client: reqwest::Client,
        event_tx: mpsc::Sender<Event>,
    ) -> Self {
        let entities = moka::future::Cache::builder()
            .max_capacity(config.cache.entity_capacity)
            .time_to_live(Duration::from_secs(config.cache.entity_ttl_secs))
            .build();
        let started_at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("rfc3339 formatting cannot fail");
        let readiness = Readiness::new(config.dmdata.ws_endpoints.len());
        Self {
            config,
            feed: ArcSwap::from_pointee(FeedSnapshot::empty()),
            entities,
            pinned: DashMap::new(),
            inflight: DashMap::new(),
            readiness,
            client,
            event_tx,
            started_at,
        }
    }
}
