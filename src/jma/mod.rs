//! JMA XML関連の純粋関数群。

pub mod entity_parse;
pub mod feed_parse;
pub mod feed_render;
pub mod id;

use quick_xml::escape::unescape;
use quick_xml::events::BytesRef;

use crate::error::UpstreamError;

/// 実体参照(GeneralRef)イベントを解決して文字列断片を返す。
///
/// quick-xml 0.41 以降、`&amp;` や `&lt;` などの実体参照はテキストと分離した
/// `Event::GeneralRef` として届く。イベントは実体名(`amp` 等)のみを持つため、
/// `&name;` に再構成して解決する。名前付き実体と数値文字参照の双方に対応。
pub(crate) fn resolve_entity_ref(e: &BytesRef) -> Result<String, UpstreamError> {
    let name = e.decode().map_err(|e| UpstreamError::Parse(e.to_string()))?;
    let raw = format!("&{name};");
    let resolved = unescape(&raw).map_err(|e| UpstreamError::Parse(e.to_string()))?;
    Ok(resolved.into_owned())
}
