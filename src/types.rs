//! モジュール間で共有するコア型。

use bytes::Bytes;

use crate::http::etag::compute_etag;

/// Atom entry 1件分のメタデータ。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ItemMeta {
    /// entry ID(JMA UUID または DMDATA 由来の合成ID。URLではなく素のID)。
    pub id: String,
    /// Control/Title 相当。
    pub title: String,
    /// Head/ReportDateTime 相当(RFC3339文字列)。
    pub updated: String,
    /// Control/PublishingOffice 相当。
    pub author: String,
    /// Head/Headline/Text 相当。
    pub content: String,
    /// 上流の実体XML URL。
    pub link: String,
}

/// イベントの発生源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventSource {
    /// DMDATA WebSocket 由来。`conn` は接続インデックス(0=tokyo, 1=osaka)。
    Dmdata { telegram_id: String, conn: usize },
    /// JMAフィード/実体取得(キャッシュミス補充)由来。一覧は再生成しない。
    JmaFeed,
}

/// 重複排除キー。
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum DedupKey {
    /// DMDATA電文一意ID。
    TelegramId(String),
    /// DMDATA IDが無い場合の合成キー。
    Composite {
        entry_id: String,
        updated: String,
        body_hash: [u8; 32],
    },
}

impl DedupKey {
    /// XML本文の blake3 ハッシュから Composite キーを作る。
    pub fn composite(entry_id: impl Into<String>, updated: impl Into<String>, body: &[u8]) -> Self {
        DedupKey::Composite {
            entry_id: entry_id.into(),
            updated: updated.into(),
            body_hash: *blake3::hash(body).as_bytes(),
        }
    }
}

/// Aggregator へ mpsc で送るイベント。
/// NOTE(phase2): dmdata::ws / fetcher::fetch_entity がこの型を生成し、
/// aggregator が唯一の書き込み点として処理する。
#[derive(Debug, Clone)]
pub struct Event {
    pub source: EventSource,
    pub dedup_key: DedupKey,
    /// 展開済みXML実体。
    pub xml_body: Bytes,
    pub meta: ItemMeta,
}

/// 実体XMLキャッシュ(moka)の値。moka の value は Clone 必須のため
/// 常に `Arc<EntityEntry>` で保持する。
#[derive(Debug)]
pub struct EntityEntry {
    pub body: Bytes,
    /// 引用符込みの強ETag(例: `"abcd..."`)。事前生成。
    pub etag: String,
    pub meta: ItemMeta,
}

impl EntityEntry {
    pub fn new(body: Bytes, meta: ItemMeta) -> Self {
        let etag = compute_etag(&body);
        Self { body, etag, meta }
    }
}

/// 完成済みAtomフィードのスナップショット。`ArcSwap<FeedSnapshot>` で保持する。
#[derive(Debug)]
pub struct FeedSnapshot {
    /// 完成済みAtom XML。ハンドラでは生成せずこのBytesを返すのみ。
    pub body: Bytes,
    /// 引用符込みの強ETag。
    pub etag: String,
    /// フィードの updated(RFC3339文字列)。
    pub last_updated: String,
}

impl FeedSnapshot {
    pub fn new(body: Bytes, last_updated: String) -> Self {
        let etag = compute_etag(&body);
        Self {
            body,
            etag,
            last_updated,
        }
    }

    /// 起動直後(初期一覧未取得)用の空スナップショット。
    pub fn empty() -> Self {
        Self::new(Bytes::new(), String::new())
    }
}
