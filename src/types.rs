//! モジュール間で共有するコア型。

use bytes::Bytes;

use crate::http::etag::compute_etag;

/// Atom entry 1件分のメタデータ。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ItemMeta {
    /// entry ID(DMDATA電文一意ID。URLではなく素のID)。
    /// entry の link は feed_render が `id` から自サーバの data URL を生成する。
    pub id: String,
    /// Control/Title 相当。
    pub title: String,
    /// Head/ReportDateTime 相当(RFC3339文字列)。
    pub updated: String,
    /// Control/PublishingOffice 相当。
    pub author: String,
    /// Head/Headline/Text 相当。
    pub content: String,
}

/// イベントの発生源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventSource {
    /// DMDATA WebSocket 由来。`conn` は接続インデックス(0=tokyo, 1=osaka)。
    Dmdata { telegram_id: String, conn: usize },
    /// キャッシュミス補充(dmdata telegram.data)由来。一覧は再生成しない。
    CacheFill,
    /// 全WS切断中のfallback polling(dmdata telegram.list)由来。
    /// dmdata電文IDを持ち、WS由来と同じpinned+publish経路に載る。
    DmdataPoll,
}

/// 重複排除キー。
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum DedupKey {
    /// DMDATA電文一意ID。
    TelegramId(String),
    /// キャッシュミス補充(CacheFill)経路の合成キー。
    /// 同一entry idでも本文更新を区別できるよう body_hash を含む。
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

/// `Last-Modified` 用のIMF-fixdate整形。
/// `Rfc2822` は `+0000` を出力し有効なHTTP-dateにならないためカスタム記述を使う。
const IMF_FIXDATE: &[time::format_description::BorrowedFormatItem<'static>] = time::macros::format_description!(
    "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
);

/// `OffsetDateTime` をIMF-fixdate文字列(UTC)に整形する。
pub fn format_imf_fixdate(dt: time::OffsetDateTime) -> String {
    dt.to_offset(time::UtcOffset::UTC)
        .format(&IMF_FIXDATE)
        .expect("imf-fixdate formatting cannot fail")
}

/// IMF-fixdate文字列をパースする(UTCとして解釈)。
pub fn parse_imf_fixdate(s: &str) -> Option<time::OffsetDateTime> {
    time::PrimitiveDateTime::parse(s.trim(), &IMF_FIXDATE)
        .ok()
        .map(time::PrimitiveDateTime::assume_utc)
}

/// RFC3339文字列を+09:00(JST)へ正規化して再整形する。
/// dmdataのフォールバック値(head.time / receivedTime)はZ表記のUTCが混ざるため、
/// `ItemMeta.updated` をここで+09:00に統一し、辞書順比較=時系列比較を成立させる。
/// パース不能な文字列は警告を出して原文のまま返す(空文字列は警告なしで素通し)。
pub fn normalize_rfc3339_to_jst(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    let rfc3339 = time::format_description::well_known::Rfc3339;
    match time::OffsetDateTime::parse(s, &rfc3339) {
        Ok(dt) => dt
            .to_offset(time::UtcOffset::from_hms(9, 0, 0).expect("valid JST offset"))
            .format(&rfc3339)
            .expect("rfc3339 formatting cannot fail"),
        Err(e) => {
            tracing::warn!(error = %e, value = %s, "updated is not RFC3339; kept as-is");
            s.to_string()
        }
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
    /// `Last-Modified` 用時刻(単調化済み。aggregator::publish が計算)。
    pub last_modified: Option<time::OffsetDateTime>,
    /// `last_modified` のIMF-fixdate文字列(事前計算)。
    pub last_modified_http: Option<String>,
}

impl FeedSnapshot {
    pub fn new(
        body: Bytes,
        last_updated: String,
        last_modified: Option<time::OffsetDateTime>,
    ) -> Self {
        let etag = compute_etag(&body);
        let last_modified_http = last_modified.map(format_imf_fixdate);
        Self {
            body,
            etag,
            last_updated,
            last_modified,
            last_modified_http,
        }
    }

    /// 起動直後(初期一覧未取得)用の空スナップショット。
    pub fn empty() -> Self {
        Self::new(Bytes::new(), String::new(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_rfc3339_converts_utc_z_to_jst() {
        assert_eq!(
            normalize_rfc3339_to_jst("2026-07-08T01:00:00Z"),
            "2026-07-08T10:00:00+09:00"
        );
    }

    #[test]
    fn normalize_rfc3339_keeps_jst_as_is() {
        assert_eq!(
            normalize_rfc3339_to_jst("2026-07-08T10:00:00+09:00"),
            "2026-07-08T10:00:00+09:00"
        );
    }

    #[test]
    fn normalize_rfc3339_passes_through_unparsable_input() {
        assert_eq!(normalize_rfc3339_to_jst("not-a-date"), "not-a-date");
        assert_eq!(normalize_rfc3339_to_jst(""), "");
    }

    #[test]
    fn normalized_timestamps_sort_lexicographically_in_time_order() {
        // Z表記(UTC)と+09:00が混在しても、正規化後は辞書順=時系列
        let mut updated = vec![
            normalize_rfc3339_to_jst("2026-07-08T02:00:00Z"), // 11:00 JST
            normalize_rfc3339_to_jst("2026-07-08T10:00:00+09:00"),
            normalize_rfc3339_to_jst("2026-07-08T00:30:00Z"), // 09:30 JST
        ];
        updated.sort();
        assert_eq!(
            updated,
            vec![
                "2026-07-08T09:30:00+09:00",
                "2026-07-08T10:00:00+09:00",
                "2026-07-08T11:00:00+09:00",
            ]
        );
    }
}
