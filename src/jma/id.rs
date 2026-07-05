//! entry ID の導出・正規化(JMA UUID / DMDATA合成IDの両対応)。

/// JMA採番のUUID形式(8-4-4-4-12の16進)かどうか。
fn is_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    b.iter().enumerate().all(|(i, &c)| match i {
        8 | 13 | 18 | 23 => c == b'-',
        _ => c.is_ascii_hexdigit(),
    })
}

/// DMDATA由来の合成ID(JMA本家に存在しないID)かどうか。
/// JMAの実IDはUUID形式なので、UUIDでなければ合成IDとみなし、
/// キャッシュミス時にJMAへ307せず404を返す判断に使う。
pub fn is_synthetic(id: &str) -> bool {
    !is_uuid(id)
}

/// URL(例: `https://.../developer/xml/data/{id}.xml`)から素のIDを取り出す。
/// 最終パスセグメントの `.xml` を除いた部分。URL形式でなければ None。
pub fn extract_id_from_url(url: &str) -> Option<&str> {
    let path = url.split(['?', '#']).next()?;
    let segment = path.rsplit('/').next()?;
    let id = segment.strip_suffix(".xml").unwrap_or(segment);
    if id.is_empty() { None } else { Some(id) }
}

/// DMDATA data由来のentryにはJMAのUUIDが含まれないため、決定的な合成IDを生成する。
/// 形式: `{Control/DateTimeのyyyyMMddHHmmss}_{serial or 0}_{電文種別コード}_{EventID}`
/// 決定的なので2系統(tokyo/osaka)間でも一致する。
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
    fn uuid_is_not_synthetic() {
        assert!(!is_synthetic("ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b"));
        assert!(!is_synthetic("CA7203BD-93B1-3F3E-B3F0-B6D4BE3B7A5B"));
    }

    #[test]
    fn synthesized_is_synthetic() {
        let id = synthesize_id("2026-07-05T04:10:00Z", Some("2"), "VXSE53", "20260705041000");
        assert!(is_synthetic(&id));
        assert!(is_synthetic("not-a-uuid"));
        assert!(is_synthetic(""));
    }

    #[test]
    fn synthesize_is_deterministic() {
        let a = synthesize_id("2026-07-05T04:10:00+09:00", Some("2"), "VXSE53", "20260705041000");
        let b = synthesize_id("2026-07-05T04:10:00+09:00", Some("2"), "VXSE53", "20260705041000");
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
    fn extract_id_from_url_works() {
        assert_eq!(
            extract_id_from_url(
                "https://www.data.jma.go.jp/developer/xml/data/ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b.xml"
            ),
            Some("ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b")
        );
        assert_eq!(extract_id_from_url("https://host/data/abc.xml?x=1"), Some("abc"));
        assert_eq!(extract_id_from_url("abc.xml"), Some("abc"));
        assert_eq!(extract_id_from_url("https://host/data/"), None);
    }
}
