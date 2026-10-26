//! モジュール間で共有するコア型。

use std::sync::Arc;

use axum::http::HeaderValue;
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
    /// 電文種別コード(dmdata head.type。例: VXSE53)。不明な場合は空文字列。
    pub telegram_type: String,
}

/// `/latest/earthquake` の対象電文種別(震度速報 / 震源に関する情報 / 震源・震度に関する情報)。
pub const LATEST_EARTHQUAKE_TYPES: &[&str] = &["VXSE51", "VXSE52", "VXSE53"];
/// `/latest/tsunami` の対象電文種別(津波警報・注意報・予報a)。
pub const LATEST_TSUNAMI_TYPES: &[&str] = &["VTSE41"];

/// `/latest/*` が返す最新電文1件。
#[derive(Debug, Clone)]
pub struct LatestEntry {
    pub meta: ItemMeta,
    /// 実体。poll由来・ウォームアップ由来で未取得なら `None`
    /// (CacheFillで届いた時点で aggregator が埋める)。
    pub entry: Option<Arc<EntityEntry>>,
}

/// 種別ごとの最新電文の保管場所。feed(pinned / entities)とは独立に保持し、
/// feedから押し出されても新しい電文が来るまで残る。本文はpinnedと同じ `Arc` を
/// 共有するためコピーは発生しない。書き込みは aggregator のみで、
/// `ArcSwap<LatestEntries>` で公開する。
#[derive(Debug, Clone, Default)]
pub struct LatestEntries {
    pub earthquake: Option<LatestEntry>,
    pub tsunami: Option<LatestEntry>,
}

impl LatestEntries {
    /// 初期一覧(updated降順)から各種別の先頭一致を最新とする。実体は未取得(None)。
    pub fn from_metas(metas: &[ItemMeta]) -> Self {
        let mut latest = Self::default();
        latest.offer_metas(metas);
        latest
    }

    /// 一覧(updated降順)の各電文を実体なしで offer する。
    pub fn offer_metas(&mut self, metas: &[ItemMeta]) {
        // 古い順に offer すれば先頭一致が最後に勝つ(同一updatedも先頭優先になる)
        for meta in metas.iter().rev() {
            self.offer(meta, None);
        }
    }

    /// 対象種別で、保持中の電文以上に新しければ差し替える。差し替えたら true。
    /// updated は+09:00正規化済みRFC3339なので辞書順比較=時系列比較。
    pub fn offer(&mut self, meta: &ItemMeta, entry: Option<&Arc<EntityEntry>>) -> bool {
        let Some(slot) = self.slot_mut(&meta.telegram_type) else {
            return false;
        };
        if slot
            .as_ref()
            .is_some_and(|current| current.meta.updated > meta.updated)
        {
            return false;
        }
        *slot = Some(LatestEntry {
            meta: meta.clone(),
            entry: entry.cloned(),
        });
        true
    }

    /// 実体未取得のスロットがこのIDなら実体を埋める。埋めたら true。
    /// CacheFill由来のmetaは電文種別を持たないためIDで照合する。
    pub fn fill(&mut self, id: &str, entry: &Arc<EntityEntry>) -> bool {
        let mut filled = false;
        for latest in [&mut self.earthquake, &mut self.tsunami]
            .into_iter()
            .flatten()
        {
            if latest.entry.is_none() && latest.meta.id == id {
                latest.entry = Some(Arc::clone(entry));
                filled = true;
            }
        }
        filled
    }

    fn slot_mut(&mut self, telegram_type: &str) -> Option<&mut Option<LatestEntry>> {
        let is = |types: &[&str]| types.iter().any(|t| t.eq_ignore_ascii_case(telegram_type));
        if is(LATEST_EARTHQUAKE_TYPES) {
            Some(&mut self.earthquake)
        } else if is(LATEST_TSUNAMI_TYPES) {
            Some(&mut self.tsunami)
        } else {
            None
        }
    }
}

/// イベントの発生源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventSource {
    /// DMDATA WebSocket 由来。`conn` は接続インデックス(0=tokyo, 1=osaka)。
    Dmdata { telegram_id: String, conn: usize },
    /// キャッシュミス補充(dmdata telegram.data)由来。一覧は再生成しない。
    CacheFill,
    /// 全WS切断中のfallback polling(dmdata telegram.list)由来。
    /// dmdata電文IDを持つがmeta-only(実体なし)でpublishされ、
    /// 実体は初回アクセス時にCacheFill経路で遅延取得される。
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
    /// 展開済みXML実体。WS / CacheFill 由来は `Some`、poll由来はmeta-only(`None`)。
    /// `None` のentryの実体は初回アクセス時にCacheFill経路で遅延取得される。
    pub xml_body: Option<Bytes>,
    pub meta: ItemMeta,
}

/// 実体XMLキャッシュ(moka)の値。moka の value は Clone 必須のため
/// 常に `Arc<EntityEntry>` で保持する。
#[derive(Debug)]
pub struct EntityEntry {
    pub body: Bytes,
    /// 引用符込みの強ETag(例: `"abcd..."`)。事前生成。
    pub etag: String,
    /// `etag` の `HeaderValue`(事前生成。送出時は clone=参照カウントのみ)。
    pub etag_header: HeaderValue,
    pub meta: ItemMeta,
}

impl EntityEntry {
    pub fn new(body: Bytes, meta: ItemMeta) -> Self {
        let etag = compute_etag(&body);
        let etag_header =
            HeaderValue::from_str(&etag).expect("etag is always a valid header value");
        Self {
            body,
            etag,
            etag_header,
            meta,
        }
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
    /// `etag` の `HeaderValue`(事前生成。送出時は clone=参照カウントのみ)。
    pub etag_header: HeaderValue,
    /// フィードの updated(RFC3339文字列)。
    pub last_updated: String,
    /// `Last-Modified` 用時刻(単調化済み。aggregator::publish が計算)。
    pub last_modified: Option<time::OffsetDateTime>,
    /// `last_modified` のIMF-fixdate `HeaderValue`(事前生成)。
    pub last_modified_header: Option<HeaderValue>,
}

impl FeedSnapshot {
    pub fn new(
        body: Bytes,
        last_updated: String,
        last_modified: Option<time::OffsetDateTime>,
    ) -> Self {
        let etag = compute_etag(&body);
        let etag_header =
            HeaderValue::from_str(&etag).expect("etag is always a valid header value");
        let last_modified_header = last_modified.map(|dt| {
            HeaderValue::from_str(&format_imf_fixdate(dt))
                .expect("imf-fixdate is always a valid header value")
        });
        Self {
            body,
            etag,
            etag_header,
            last_updated,
            last_modified,
            last_modified_header,
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

    fn typed_meta(id: &str, telegram_type: &str, updated: &str) -> ItemMeta {
        ItemMeta {
            id: id.into(),
            telegram_type: telegram_type.into(),
            updated: updated.into(),
            ..ItemMeta::default()
        }
    }

    fn latest_id(latest: &Option<LatestEntry>) -> Option<&str> {
        latest.as_ref().map(|l| l.meta.id.as_str())
    }

    fn entity(body: &'static [u8]) -> Arc<EntityEntry> {
        Arc::new(EntityEntry::new(
            Bytes::from_static(body),
            ItemMeta::default(),
        ))
    }

    #[test]
    fn latest_from_metas_picks_first_match_per_category() {
        // フィード順(updated降順)。同一updatedは先頭(フィード上位)が勝つ
        let metas = vec![
            typed_meta("ID_VXSE61", "VXSE61", "2026-07-05T04:30:00+09:00"),
            typed_meta("ID_VXSE52", "VXSE52", "2026-07-05T04:20:00+09:00"),
            typed_meta("ID_VTSE41_NEW", "VTSE41", "2026-07-05T04:20:00+09:00"),
            typed_meta("ID_VXSE53", "VXSE53", "2026-07-05T04:20:00+09:00"),
            typed_meta("ID_VTSE41_OLD", "vtse41", "2026-07-05T04:10:00+09:00"),
        ];
        let latest = LatestEntries::from_metas(&metas);
        assert_eq!(latest_id(&latest.earthquake), Some("ID_VXSE52"));
        assert_eq!(latest_id(&latest.tsunami), Some("ID_VTSE41_NEW"));
        assert!(latest.earthquake.unwrap().entry.is_none());
    }

    #[test]
    fn latest_from_metas_none_when_absent() {
        let metas = vec![
            typed_meta("ID_VTSE51", "VTSE51", "2026-07-05T04:10:00+09:00"),
            typed_meta("ID_X", "", "2026-07-05T04:00:00+09:00"),
        ];
        let latest = LatestEntries::from_metas(&metas);
        assert!(latest.earthquake.is_none());
        assert!(latest.tsunami.is_none());
    }

    #[test]
    fn latest_offer_keeps_newer_and_ignores_other_types() {
        let mut latest = LatestEntries::default();
        let new = typed_meta("ID_NEW", "VXSE53", "2026-07-05T04:20:00+09:00");
        assert!(latest.offer(&new, Some(&entity(b"new"))));
        // 遅延publishされた古い電文では差し替わらない
        let old = typed_meta("ID_OLD", "VXSE51", "2026-07-05T04:00:00+09:00");
        assert!(!latest.offer(&old, None));
        // 対象外種別は無視
        let other = typed_meta("ID_VXSE61", "VXSE61", "2026-07-05T05:00:00+09:00");
        assert!(!latest.offer(&other, None));

        let eq = latest.earthquake.as_ref().unwrap();
        assert_eq!(eq.meta.id, "ID_NEW");
        assert_eq!(&eq.entry.as_ref().unwrap().body[..], b"new");
        assert!(latest.tsunami.is_none());
    }

    #[test]
    fn latest_fill_only_fills_missing_entry_with_matching_id() {
        let mut latest = LatestEntries::default();
        latest.offer(
            &typed_meta("ID_TS", "VTSE41", "2026-07-05T04:20:00+09:00"),
            None,
        );
        assert!(!latest.fill("ID_OTHER", &entity(b"other")));
        assert!(latest.fill("ID_TS", &entity(b"filled")));
        // 埋まった後は上書きしない
        assert!(!latest.fill("ID_TS", &entity(b"again")));
        let ts = latest.tsunami.as_ref().unwrap();
        assert_eq!(&ts.entry.as_ref().unwrap().body[..], b"filled");
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
