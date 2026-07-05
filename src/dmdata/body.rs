//! WS data の body 展開: base64デコード + gzip/zip展開(純粋関数)。

use std::io::{Cursor, Read};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;

use crate::error::DmdataError;

/// `compression`("gzip"/"zip"/None)と `encoding`("base64"/None)に従って
/// body を展開済みXMLバイト列に変換する。
pub fn decode_body(
    body: &str,
    compression: Option<&str>,
    encoding: Option<&str>,
) -> Result<Bytes, DmdataError> {
    match compression {
        Some("gzip") => {
            let compressed = decode_base64(body)?;
            let mut decoder = flate2::read::GzDecoder::new(Cursor::new(compressed));
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|e| DmdataError::Body(format!("gzip decompression failed: {e}")))?;
            Ok(Bytes::from(out))
        }
        Some("zip") => {
            let compressed = decode_base64(body)?;
            // zip は Cursor で包み、先頭エントリのみ読む
            let mut archive = zip::ZipArchive::new(Cursor::new(compressed))
                .map_err(|e| DmdataError::Body(format!("zip open failed: {e}")))?;
            if archive.is_empty() {
                return Err(DmdataError::Body("zip archive has no entries".into()));
            }
            let mut file = archive
                .by_index(0)
                .map_err(|e| DmdataError::Body(format!("zip entry read failed: {e}")))?;
            let mut out = Vec::new();
            file.read_to_end(&mut out)
                .map_err(|e| DmdataError::Body(format!("zip decompression failed: {e}")))?;
            Ok(Bytes::from(out))
        }
        Some(other) => Err(DmdataError::Body(format!("unknown compression: {other}"))),
        None => {
            if encoding == Some("base64") {
                Ok(Bytes::from(decode_base64(body)?))
            } else {
                Ok(Bytes::copy_from_slice(body.as_bytes()))
            }
        }
    }
}

fn decode_base64(body: &str) -> Result<Vec<u8>, DmdataError> {
    // 改行等を除去してからデコード(防御的)
    let cleaned: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    BASE64
        .decode(cleaned.as_bytes())
        .map_err(|e| DmdataError::Body(format!("base64 decode failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const XML: &str = "<?xml version=\"1.0\"?><Report>展開テスト</Report>";

    #[test]
    fn plain_body_passes_through() {
        let out = decode_body(XML, None, None).expect("plain must decode");
        assert_eq!(&out[..], XML.as_bytes());
    }

    #[test]
    fn base64_without_compression() {
        let encoded = BASE64.encode(XML.as_bytes());
        let out = decode_body(&encoded, None, Some("base64")).expect("base64 must decode");
        assert_eq!(&out[..], XML.as_bytes());
    }

    #[test]
    fn gzip_base64_roundtrip() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(XML.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = BASE64.encode(&compressed);

        let out = decode_body(&encoded, Some("gzip"), Some("base64")).expect("gzip must decode");
        assert_eq!(&out[..], XML.as_bytes());
    }

    #[test]
    fn zip_base64_roundtrip_first_entry() {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options: zip::write::SimpleFileOptions = Default::default();
        writer.start_file("telegram.xml", options).unwrap();
        writer.write_all(XML.as_bytes()).unwrap();
        let compressed = writer.finish().unwrap().into_inner();
        let encoded = BASE64.encode(&compressed);

        let out = decode_body(&encoded, Some("zip"), Some("base64")).expect("zip must decode");
        assert_eq!(&out[..], XML.as_bytes());
    }

    #[test]
    fn unknown_compression_is_error() {
        assert!(decode_body("x", Some("br"), None).is_err());
    }

    #[test]
    fn broken_base64_is_error() {
        assert!(decode_body("%%%", Some("gzip"), Some("base64")).is_err());
    }
}
