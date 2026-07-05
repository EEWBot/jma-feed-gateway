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

/// If-Modified-Since ヘッダ値を評価する(RFC 9110 §13.1.3)。
///
/// ヘッダをIMF-fixdateとしてパースし、`last_modified <= parsed`(秒粒度)なら
/// true(= 304を返してよい)。パース不能なら false(= 200)。
/// 呼び出し側の責務: `If-None-Match` が存在する場合はこの関数を呼ばないこと。
pub fn not_modified_since(header: &str, last_modified: time::OffsetDateTime) -> bool {
    let Some(ims) = crate::types::parse_imf_fixdate(header) else {
        return false;
    };
    // IMF-fixdateは秒粒度のため、last_modified側もサブ秒を切り捨てて比較する
    let truncated = last_modified
        .to_offset(time::UtcOffset::UTC)
        .replace_nanosecond(0)
        .expect("0 is a valid nanosecond");
    truncated <= ims
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

    fn dt(s: &str) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).unwrap()
    }

    #[test]
    fn not_modified_since_equal_time_is_true() {
        let lm = dt("2026-07-05T04:10:12Z");
        assert!(not_modified_since("Sun, 05 Jul 2026 04:10:12 GMT", lm));
    }

    #[test]
    fn not_modified_since_newer_ims_is_true() {
        let lm = dt("2026-07-05T04:10:12Z");
        assert!(not_modified_since("Sun, 05 Jul 2026 05:00:00 GMT", lm));
    }

    #[test]
    fn not_modified_since_older_ims_is_false() {
        let lm = dt("2026-07-05T04:10:12Z");
        assert!(!not_modified_since("Sun, 05 Jul 2026 04:10:11 GMT", lm));
    }

    #[test]
    fn not_modified_since_garbage_is_false() {
        let lm = dt("2026-07-05T04:10:12Z");
        assert!(!not_modified_since("not a date", lm));
        assert!(!not_modified_since("", lm));
    }

    #[test]
    fn not_modified_since_truncates_subseconds() {
        // last_modified にサブ秒があっても秒粒度で比較する
        let lm = dt("2026-07-05T04:10:12.9Z");
        assert!(not_modified_since("Sun, 05 Jul 2026 04:10:12 GMT", lm));
    }
}
