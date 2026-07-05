//! entry ID の導出・正規化。
//!
//! JMAフィードの実IDは `{yyyyMMddHHmmss}_{serial}_{電文種別コード}_{官署コード}` 形式
//! (例: `20260705050045_0_VFVO53_010000`)。UUIDではない。

/// URL(例: `https://.../developer/xml/data/{id}.xml`)から素のIDを取り出す。
/// 最終パスセグメントの `.xml` を除いた部分。URL形式でなければ None。
pub fn extract_id_from_url(url: &str) -> Option<&str> {
    let path = url.split(['?', '#']).next()?;
    let segment = path.rsplit('/').next()?;
    let id = segment.strip_suffix(".xml").unwrap_or(segment);
    if id.is_empty() { None } else { Some(id) }
}

/// 素のID `{datetime}_{serial}_{type}_{office}` から電文種別コード(第3フィールド)を取り出す。
/// UUID等アンダースコア区切りでないIDは None。
pub fn telegram_type(id: &str) -> Option<&str> {
    let t = id.split('_').nth(2)?;
    (!t.is_empty()).then_some(t)
}

/// JMA実ID形式 `{yyyyMMddHHmmss}_{...}` かどうかを判定する
/// (先頭14バイトがASCII数字 + 15バイト目が `_` + 残りが非空)。
/// dataハンドラでミス時に上流JMAへ307してよいかのゲートに使う。
pub fn is_jma_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    bytes.len() > 15
        && bytes[..14].iter().all(u8::is_ascii_digit)
        && bytes[14] == b'_'
}

/// DMDATA電文IDが空の場合のフォールバック。決定的な合成IDを生成する。
/// 形式: `{Control/DateTimeのyyyyMMddHHmmss}_{serial or 0}_{電文種別コード}_{EventID}`
/// 決定的なので2系統(tokyo/osaka)間でも一致する。
///
/// 通常は `WsData.id`(DMDATA電文一意ID)をそのままentry IDに使うため、
/// これが使われるのは電文IDが欠落しているまれな場合のみ。合成IDはJMA形式に
/// マッチするため、キャッシュから外れた後の307リダイレクトは上流404になりうる。
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
    fn telegram_type_extracts_third_field() {
        assert_eq!(
            telegram_type("20260705050045_0_VXSE53_010000"),
            Some("VXSE53")
        );
        // UUID(アンダースコア区切りでない)は None
        assert_eq!(telegram_type("ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b"), None);
        // 第3フィールドが空の場合も None
        assert_eq!(telegram_type("a__"), None);
    }

    #[test]
    fn is_jma_id_accepts_real_jma_format() {
        assert!(is_jma_id("20260705050045_0_VFVO53_010000"));
        // 合成IDフォールバックもJMA形式にマッチする
        assert!(is_jma_id("20260705041000_2_VXSE53_20260705040500"));
    }

    #[test]
    fn is_jma_id_rejects_non_jma_formats() {
        // UUID
        assert!(!is_jma_id("ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b"));
        // dmdata電文ID風(先頭14バイトが数字でない)
        assert!(!is_jma_id("a6bffef53b0eb56e844eda276e0c9741"));
        // 空
        assert!(!is_jma_id(""));
        // `_` なし14桁
        assert!(!is_jma_id("20260705050045"));
        // 14桁 + `_` のみ(残りが空)
        assert!(!is_jma_id("20260705050045_"));
        // 13桁 + `_`
        assert!(!is_jma_id("2026070505004_0_VFVO53_010000"));
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
