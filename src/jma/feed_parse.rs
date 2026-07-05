//! JMA Atomフィード(eqvol.xml)のパース → `Vec<ItemMeta>`(純粋関数)。

use quick_xml::Reader;
use quick_xml::events::Event as XmlEvent;

use crate::error::UpstreamError;
use crate::jma::id::extract_id_from_url;
use crate::types::ItemMeta;

/// entry 内で収集対象のフィールド。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Title,
    Id,
    Updated,
    AuthorName,
    Content,
}

/// Atomフィードをパースして entry のメタデータ列を返す(フィード出現順)。
pub fn parse(xml: &str) -> Result<Vec<ItemMeta>, UpstreamError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut items = Vec::new();
    let mut current: Option<ItemMeta> = None;
    let mut in_author = false;
    let mut field: Option<Field> = None;
    let mut text = String::new();

    loop {
        match reader.read_event().map_err(|e| {
            UpstreamError::Parse(format!("xml error at {}: {e}", reader.error_position()))
        })? {
            XmlEvent::Start(e) => {
                let name = e.local_name();
                let name = name.as_ref();
                if name == b"entry" {
                    current = Some(ItemMeta::default());
                } else if current.is_some() {
                    field = match name {
                        b"title" => Some(Field::Title),
                        b"id" => Some(Field::Id),
                        b"updated" => Some(Field::Updated),
                        b"content" => Some(Field::Content),
                        b"author" => {
                            in_author = true;
                            None
                        }
                        b"name" if in_author => Some(Field::AuthorName),
                        _ => None,
                    };
                    text.clear();
                }
            }
            XmlEvent::Empty(e) => {
                // entry 内の <link href="..."/>
                if e.local_name().as_ref() == b"link"
                    && let Some(meta) = current.as_mut()
                {
                    for attr in e.attributes().flatten() {
                        if attr.key.local_name().as_ref() == b"href" {
                            meta.link = attr
                                .unescape_value()
                                .map_err(|e| UpstreamError::Parse(e.to_string()))?
                                .into_owned();
                        }
                    }
                }
            }
            XmlEvent::Text(e) => {
                if field.is_some() {
                    text.push_str(
                        &e.unescape()
                            .map_err(|e| UpstreamError::Parse(e.to_string()))?,
                    );
                }
            }
            XmlEvent::End(e) => {
                let name = e.local_name();
                let name = name.as_ref();
                if name == b"entry" {
                    if let Some(meta) = current.take()
                        && !meta.id.is_empty()
                    {
                        items.push(meta);
                    }
                    in_author = false;
                    field = None;
                } else if name == b"author" {
                    in_author = false;
                } else if let (Some(f), Some(meta)) = (field.take(), current.as_mut()) {
                    let value = std::mem::take(&mut text);
                    match f {
                        Field::Title => meta.title = value,
                        Field::Updated => meta.updated = value,
                        Field::AuthorName => meta.author = value,
                        Field::Content => meta.content = value,
                        Field::Id => {
                            // entry <id> はURL形式。素のIDを抽出し、linkが未取得ならURLを流用
                            if meta.link.is_empty() {
                                meta.link = value.clone();
                            }
                            meta.id = extract_id_from_url(&value).unwrap_or(&value).to_string();
                        }
                    }
                }
            }
            XmlEvent::Eof => break,
            _ => {}
        }
    }

    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../../tests/fixtures/eqvol_sample.xml");

    #[test]
    fn parses_fixture_entries() {
        let items = parse(FIXTURE).expect("fixture must parse");
        assert_eq!(items.len(), 2);

        let first = &items[0];
        assert_eq!(first.id, "ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b");
        assert_eq!(first.title, "震源・震度に関する情報");
        assert_eq!(first.updated, "2026-07-05T04:10:12+09:00");
        assert_eq!(first.author, "気象庁");
        assert_eq!(first.content, "5日04時05分ころ、地震がありました。");
        assert_eq!(
            first.link,
            "https://www.data.jma.go.jp/developer/xml/data/ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b.xml"
        );

        let second = &items[1];
        assert_eq!(second.id, "0af03cd5-25a9-3ba5-b73b-c9b7ce0f8a55");
        assert_eq!(second.title, "震度速報");
        // エスケープ済みテキストが復元されること
        assert!(second.content.contains("<br/>"));
    }

    #[test]
    fn ignores_feed_level_elements() {
        let items = parse(FIXTURE).expect("fixture must parse");
        // フィードレベルの title/id が entry に混入しないこと
        assert!(items.iter().all(|m| m.title != "高頻度(地震火山)"));
        assert!(items.iter().all(|m| !m.id.contains("eqvol")));
    }

    #[test]
    fn empty_feed_yields_no_items() {
        let xml = r#"<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>t</title></feed>"#;
        assert!(parse(xml).expect("must parse").is_empty());
    }

    #[test]
    fn broken_xml_is_error() {
        assert!(parse("<feed><entry></feed>").is_err());
    }
}
