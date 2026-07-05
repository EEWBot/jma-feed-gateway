//! 強ETag生成と If-None-Match 照合(純粋関数)。

/// 本文の blake3 ハッシュから引用符込みの強ETagを生成する。
pub fn compute_etag(body: &[u8]) -> String {
    format!("\"{}\"", blake3::hash(body).to_hex())
}

/// If-None-Match ヘッダ値と強ETagを照合する。
///
/// - `*` は常にマッチ
/// - カンマ区切りの複数値に対応
/// - `W/"..."` は弱比較(If-None-Match は弱比較で照合する; RFC 9110 §13.1.2)
pub fn if_none_match(header: &str, etag: &str) -> bool {
    if header.trim() == "*" {
        return true;
    }
    header.split(',').any(|candidate| {
        let candidate = candidate.trim();
        let candidate = candidate.strip_prefix("W/").unwrap_or(candidate);
        candidate == etag
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etag_is_strong_and_quoted() {
        let etag = compute_etag(b"hello");
        assert!(etag.starts_with('"') && etag.ends_with('"'));
        assert!(!etag.starts_with("W/"));
        // 決定的
        assert_eq!(etag, compute_etag(b"hello"));
        assert_ne!(etag, compute_etag(b"world"));
    }

    #[test]
    fn matches_exact() {
        let etag = compute_etag(b"body");
        assert!(if_none_match(&etag, &etag));
        assert!(!if_none_match("\"other\"", &etag));
    }

    #[test]
    fn matches_star() {
        assert!(if_none_match("*", "\"anything\""));
        assert!(if_none_match("  *  ", "\"anything\""));
    }

    #[test]
    fn matches_multiple_values() {
        let etag = compute_etag(b"body");
        let header = format!("\"aaa\", {}, \"bbb\"", etag);
        assert!(if_none_match(&header, &etag));
        assert!(!if_none_match("\"aaa\", \"bbb\"", &etag));
    }

    #[test]
    fn matches_weak_prefix() {
        let etag = compute_etag(b"body");
        let header = format!("W/{}", etag);
        assert!(if_none_match(&header, &etag));
    }
}
