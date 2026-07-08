//! entry ID の導出・正規化。
//!
//! entry IDは通常 DMDATA電文一意ID(384bitハッシュの16進文字列)。
//! 電文IDが欠落するまれな場合のみ `{yyyyMMddHHmmss}_{serial}_{電文種別コード}_{EventID}`
//! 形式の合成ID(`synthesize_id`)にフォールバックする。

/// URL(例: `https://.../developer/xml/data/{id}.xml`)から素のIDを取り出す。
/// 最終パスセグメントの `.xml` を除いた部分。URL形式でなければ None。
#[cfg(test)]
pub fn extract_id_from_url(url: &str) -> Option<&str> {
    let path = url.split(['?', '#']).next()?;
    let segment = path.rsplit('/').next()?;
    let id = segment.strip_suffix(".xml").unwrap_or(segment);
    if id.is_empty() { None } else { Some(id) }
}

/// 電文IDとしてありうる形式か(長さ1..=128 かつ `[A-Za-z0-9_-]` のみ)。
/// dataハンドラでミス時に dmdata telegram.data へ取得に行ってよいかのゲートに使う。
/// DMDATA電文ID(16進ハッシュ)と合成ID(`synthesize_id`)の双方を通す。
pub fn is_fetchable_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// DMDATA電文IDが空の場合のフォールバック。決定的な合成IDを生成する。
/// 形式: `{Control/DateTimeのyyyyMMddHHmmss}_{serial or 0}_{電文種別コード}_{EventID}`
/// 決定的なので2系統(tokyo/osaka)間でも一致する。
///
/// 通常は `WsData.id`(DMDATA電文一意ID)をそのままentry IDに使うため、
/// これが使われるのは電文IDが欠落しているまれな場合のみ。合成IDはdmdata上流に
/// 存在しないため、キャッシュから外れた後のミスは補充に失敗し404になりうる。
pub fn synthesize_id(
    control_datetime: &str,
    serial: Option<&str>,
    telegram_type: &str,
    event_id: &str,
) -> String {
    // "2026-07-05T04:10:00Z" などから数字のみ抽出し先頭14桁(yyyyMMddHHmmss)を使う
    let compact: String = control_datetime
        .chars()
        .filter(char::is_ascii_digit)
        .take(14)
        .collect();
    let serial = match serial {
        Some(s) if !s.trim().is_empty() => s.trim(),
        _ => "0",
    };
    format!("{compact}_{serial}_{telegram_type}_{event_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthesize_is_deterministic() {
        let a = synthesize_id(
            "2026-07-05T04:10:00+09:00",
            Some("2"),
            "VXSE53",
            "20260705041000",
        );
        let b = synthesize_id(
            "2026-07-05T04:10:00+09:00",
            Some("2"),
            "VXSE53",
            "20260705041000",
        );
        assert_eq!(a, b);
        assert_eq!(a, "20260705041000_2_VXSE53_20260705041000");
    }

    #[test]
    fn synthesize_defaults_serial_to_zero() {
        let id = synthesize_id("2026-07-05T04:10:00Z", None, "VXSE53", "E1");
        assert_eq!(id, "20260705041000_0_VXSE53_E1");
        let id = synthesize_id("2026-07-05T04:10:00Z", Some("  "), "VXSE53", "E1");
        assert_eq!(id, "20260705041000_0_VXSE53_E1");
    }

    #[test]
    fn is_fetchable_id_accepts_telegram_and_synthesized_ids() {
        // DMDATA電文ID(384bitハッシュ = 96文字の16進)
        let hash_id = "a".repeat(96);
        assert!(is_fetchable_id(&hash_id));
        // 合成IDフォールバック
        assert!(is_fetchable_id("20260705050045_0_VXSE53_010000"));
        // ハイフン・境界長も通す
        assert!(is_fetchable_id("ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b"));
        assert!(is_fetchable_id("x"));
        assert!(is_fetchable_id(&"0".repeat(128)));
    }

    #[test]
    fn is_fetchable_id_rejects_garbage() {
        // 空
        assert!(!is_fetchable_id(""));
        // 長すぎ(>128)
        assert!(!is_fetchable_id(&"0".repeat(129)));
        // `.` / `/` / 空白入り
        assert!(!is_fetchable_id("foo.bar"));
        assert!(!is_fetchable_id("foo/bar"));
        assert!(!is_fetchable_id("foo bar"));
        // 非ASCII
        assert!(!is_fetchable_id("電文"));
    }

    #[test]
    fn extract_id_from_url_works() {
        assert_eq!(
            extract_id_from_url(
                "https://www.data.jma.go.jp/developer/xml/data/ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b.xml"
            ),
            Some("ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b")
        );
        assert_eq!(
            extract_id_from_url("https://host/data/abc.xml?x=1"),
            Some("abc")
        );
        assert_eq!(extract_id_from_url("abc.xml"), Some("abc"));
        assert_eq!(extract_id_from_url("https://host/data/"), None);
    }
}
