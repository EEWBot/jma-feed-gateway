//! 実体XML(JMA電文)の Control / Head メタ抽出(純粋関数)。
//! DMDATA の xmlReport(JSON)よりも、展開済みXML bodyのパースを正とする。

use quick_xml::Reader;
use quick_xml::events::Event as XmlEvent;

use crate::error::UpstreamError;
use crate::jma::resolve_entity_ref;

/// 電文XMLの Control / Head から抽出したメタ情報。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EntityMeta {
    /// Control/Title
    pub title: String,
    /// Control/DateTime
    pub date_time: String,
    /// Control/PublishingOffice
    pub publishing_office: String,
    /// Head/ReportDateTime
    pub report_date_time: String,
    /// Head/EventID
    pub event_id: String,
    /// Head/Serial
    pub serial: String,
    /// Head/Headline/Text
    pub headline_text: String,
}

/// 電文XMLをパースして Control/Head のメタ情報を返す。
pub fn parse_entity_meta(xml: &str) -> Result<EntityMeta, UpstreamError> {
    let mut reader = Reader::from_str(xml);
    // trim_text は使わない。quick-xml 0.41 ではテキストが実体参照ごとに分割されるため、
    // 断片単位でトリムすると語間の空白が失われる。バッファに累積し End で一括トリムする。

    let mut meta = EntityMeta::default();
    // ローカル名のスタック(名前空間は無視)
    let mut stack: Vec<String> = Vec::new();
    // 現在の要素のテキスト断片を溜めるバッファ。
    // quick-xml 0.41 以降、テキストは実体参照(GeneralRef)ごとに分割されて届くため、
    // End で確定させるまで累積する必要がある。
    let mut buf = String::new();

    loop {
        match reader.read_event().map_err(|e| {
            UpstreamError::Parse(format!("xml error at {}: {e}", reader.error_position()))
        })? {
            XmlEvent::Start(e) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                stack.push(name);
                buf.clear();
            }
            XmlEvent::Text(e) => {
                buf.push_str(&e.decode().map_err(|e| UpstreamError::Parse(e.to_string()))?);
            }
            XmlEvent::GeneralRef(e) => {
                buf.push_str(&resolve_entity_ref(&e)?);
            }
            XmlEvent::End(_) => {
                // pop 前のスタック末尾が、いま閉じた要素のパス。
                let target = match path_tail(&stack) {
                    ["Control", "Title"] => Some(&mut meta.title),
                    ["Control", "DateTime"] => Some(&mut meta.date_time),
                    ["Control", "PublishingOffice"] => Some(&mut meta.publishing_office),
                    ["Head", "ReportDateTime"] => Some(&mut meta.report_date_time),
                    ["Head", "EventID"] => Some(&mut meta.event_id),
                    ["Head", "Serial"] => Some(&mut meta.serial),
                    ["Headline", "Text"] => Some(&mut meta.headline_text),
                    _ => None,
                };
                if let Some(slot) = target
                    && slot.is_empty()
                {
                    *slot = buf.trim().to_string();
                }
                buf.clear();
                stack.pop();
            }
            XmlEvent::Eof => break,
            _ => {}
        }
    }

    Ok(meta)
}

/// スタック末尾2要素を返すヘルパ。
fn path_tail(stack: &[String]) -> [&str; 2] {
    let n = stack.len();
    [
        if n >= 2 { stack[n - 2].as_str() } else { "" },
        if n >= 1 { stack[n - 1].as_str() } else { "" },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Report xmlns="http://xml.kishou.go.jp/jmaxml1/">
<Control>
<Title>震源・震度に関する情報</Title>
<DateTime>2026-07-04T19:10:00Z</DateTime>
<Status>通常</Status>
<EditorialOffice>気象庁本庁</EditorialOffice>
<PublishingOffice>気象庁</PublishingOffice>
</Control>
<Head xmlns="http://xml.kishou.go.jp/jmaxml1/informationBasis1/">
<Title>震源・震度情報</Title>
<ReportDateTime>2026-07-05T04:10:00+09:00</ReportDateTime>
<TargetDateTime>2026-07-05T04:05:00+09:00</TargetDateTime>
<EventID>20260705040500</EventID>
<InfoType>発表</InfoType>
<Serial>2</Serial>
<Headline>
<Text>5日04時05分ころ、地震がありました。</Text>
</Headline>
</Head>
<Body/>
</Report>"#;

    #[test]
    fn extracts_control_and_head() {
        let meta = parse_entity_meta(SAMPLE).expect("must parse");
        assert_eq!(meta.title, "震源・震度に関する情報");
        assert_eq!(meta.date_time, "2026-07-04T19:10:00Z");
        assert_eq!(meta.publishing_office, "気象庁");
        assert_eq!(meta.report_date_time, "2026-07-05T04:10:00+09:00");
        assert_eq!(meta.event_id, "20260705040500");
        assert_eq!(meta.serial, "2");
        assert_eq!(meta.headline_text, "5日04時05分ころ、地震がありました。");
    }

    #[test]
    fn head_title_does_not_overwrite_control_title() {
        let meta = parse_entity_meta(SAMPLE).expect("must parse");
        // Control/Title が先勝ちで、Head/Title に上書きされないこと
        assert_ne!(meta.title, "震源・震度情報");
    }

    #[test]
    fn missing_fields_stay_empty() {
        let meta = parse_entity_meta("<Report><Control><Title>t</Title></Control></Report>")
            .expect("must parse");
        assert_eq!(meta.title, "t");
        assert!(meta.event_id.is_empty());
        assert!(meta.serial.is_empty());
    }

    #[test]
    fn broken_xml_is_error() {
        assert!(parse_entity_meta("<Report><Control></Report>").is_err());
    }
}
