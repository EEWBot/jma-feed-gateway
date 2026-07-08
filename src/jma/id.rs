//! entry ID の導出・正規化。
//!
//! entry IDは DMDATA電文一意ID(384bitハッシュの16進文字列)。

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
/// DMDATA電文ID(16進ハッシュ)や UUID 形式などを通す。
pub fn is_fetchable_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_fetchable_id_accepts_telegram_ids() {
        // DMDATA電文ID(384bitハッシュ = 96文字の16進)
        let hash_id = "a".repeat(96);
        assert!(is_fetchable_id(&hash_id));
        // アンダースコア混じりも通す
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
