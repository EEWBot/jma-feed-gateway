//! `ItemMeta` 列 → Atom XML Bytes(quick-xml Writer 直書き)。
//! 更新時のみ呼ばれる。HTTPハンドラでは呼ばないこと。

use bytes::Bytes;
use quick_xml::Writer;
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event as XmlEvent};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::offset;

const ATOM_NS: &str = "http://www.w3.org/2005/Atom";
const FEED_PATH: &str = "/developer/xml/feed/eqvol.xml";

/// 現在時刻をRFC3339(JST固定)で返す。
pub fn now_jst_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .to_offset(offset!(+9))
        .format(&Rfc3339)
        .expect("rfc3339 formatting cannot fail")
}

/// Atomフィードを生成する。`base_url` は自サーバの公開ベースURL。
/// entry の id / link は自サーバの data URL を指す。
pub fn render(metas: &[crate::types::ItemMeta], base_url: &str) -> Bytes {
    let base = base_url.trim_end_matches('/');
    let feed_updated = metas
        .first()
        .filter(|m| !m.updated.is_empty())
        .map(|m| m.updated.clone())
        .unwrap_or_else(now_jst_rfc3339);

    let mut writer = Writer::new(Vec::with_capacity(4096));
    // Vec<u8> への書き込みは失敗しない
    writer
        .write_event(XmlEvent::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .expect("write to Vec cannot fail");

    let mut feed = BytesStart::new("feed");
    feed.push_attribute(("xmlns", ATOM_NS));
    feed.push_attribute(("xml:lang", "ja"));
    writer
        .write_event(XmlEvent::Start(feed))
        .expect("write to Vec cannot fail");

    write_text_element(&mut writer, "title", "高頻度(地震火山)");
    write_text_element(&mut writer, "subtitle", "JMAXML relay feed");
    write_text_element(&mut writer, "updated", &feed_updated);
    write_text_element(&mut writer, "id", &format!("{base}{FEED_PATH}"));
    let mut link = BytesStart::new("link");
    link.push_attribute(("rel", "self"));
    link.push_attribute(("href", format!("{base}{FEED_PATH}").as_str()));
    writer
        .write_event(XmlEvent::Empty(link))
        .expect("write to Vec cannot fail");
    write_text_element(
        &mut writer,
        "rights",
        "気象庁防災情報XMLを中継配信しています。出典: 気象庁ホームページ",
    );

    for meta in metas {
        let entry_url = format!("{base}/developer/xml/data/{}.xml", meta.id);
        writer
            .write_event(XmlEvent::Start(BytesStart::new("entry")))
            .expect("write to Vec cannot fail");
        write_text_element(&mut writer, "title", &meta.title);
        write_text_element(&mut writer, "id", &entry_url);
        let mut link = BytesStart::new("link");
        link.push_attribute(("type", "application/xml"));
        link.push_attribute(("href", entry_url.as_str()));
        writer
            .write_event(XmlEvent::Empty(link))
            .expect("write to Vec cannot fail");
        write_text_element(&mut writer, "updated", &meta.updated);
        writer
            .write_event(XmlEvent::Start(BytesStart::new("author")))
            .expect("write to Vec cannot fail");
        write_text_element(&mut writer, "name", &meta.author);
        writer
            .write_event(XmlEvent::End(BytesEnd::new("author")))
            .expect("write to Vec cannot fail");
        let mut content = BytesStart::new("content");
        content.push_attribute(("type", "text"));
        writer
            .write_event(XmlEvent::Start(content))
            .expect("write to Vec cannot fail");
        // BytesText::new は書き込み時に自動エスケープする
        writer
            .write_event(XmlEvent::Text(BytesText::new(&meta.content)))
            .expect("write to Vec cannot fail");
        writer
            .write_event(XmlEvent::End(BytesEnd::new("content")))
            .expect("write to Vec cannot fail");
        writer
            .write_event(XmlEvent::End(BytesEnd::new("entry")))
            .expect("write to Vec cannot fail");
    }

    writer
        .write_event(XmlEvent::End(BytesEnd::new("feed")))
        .expect("write to Vec cannot fail");

    Bytes::from(writer.into_inner())
}

fn write_text_element(writer: &mut Writer<Vec<u8>>, name: &str, text: &str) {
    writer
        .write_event(XmlEvent::Start(BytesStart::new(name)))
        .expect("write to Vec cannot fail");
    writer
        .write_event(XmlEvent::Text(BytesText::new(text)))
        .expect("write to Vec cannot fail");
    writer
        .write_event(XmlEvent::End(BytesEnd::new(name)))
        .expect("write to Vec cannot fail");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jma::feed_parse;
    use crate::types::ItemMeta;

    fn sample_meta() -> ItemMeta {
        ItemMeta {
            id: "ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b".into(),
            title: "震源・震度に関する情報".into(),
            updated: "2026-07-05T04:10:12+09:00".into(),
            author: "気象庁".into(),
            content: "5日04時05分ころ、地震がありました。".into(),
            link: "https://www.data.jma.go.jp/developer/xml/data/ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b.xml".into(),
        }
    }

    #[test]
    fn roundtrip_render_then_parse() {
        let metas = vec![
            sample_meta(),
            ItemMeta {
                id: "0af03cd5-25a9-3ba5-b73b-c9b7ce0f8a55".into(),
                title: "震度速報".into(),
                updated: "2026-07-05T04:07:03+09:00".into(),
                author: "気象庁".into(),
                content: "強い揺れを感じました。".into(),
                link: String::new(),
            },
        ];
        let xml = render(&metas, "http://127.0.0.1:8080/");
        let xml_str = std::str::from_utf8(&xml).expect("must be utf-8");
        assert!(xml_str.starts_with("<?xml"));

        let parsed = feed_parse::parse(xml_str).expect("rendered feed must parse");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].id, metas[0].id);
        assert_eq!(parsed[0].title, metas[0].title);
        assert_eq!(parsed[0].updated, metas[0].updated);
        assert_eq!(parsed[0].author, metas[0].author);
        assert_eq!(parsed[0].content, metas[0].content);
        // entry の link は自サーバのURLを指す
        assert_eq!(
            parsed[0].link,
            "http://127.0.0.1:8080/developer/xml/data/ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b.xml"
        );
    }

    #[test]
    fn escapes_special_characters() {
        let mut meta = sample_meta();
        meta.title = "A & B <tag>".into();
        meta.content = "1 < 2 & \"quote\"".into();
        let xml = render(&[meta.clone()], "http://localhost");
        let xml_str = std::str::from_utf8(&xml).unwrap();
        assert!(xml_str.contains("A &amp; B &lt;tag&gt;"));
        assert!(!xml_str.contains("A & B <tag>"));

        let parsed = feed_parse::parse(xml_str).expect("must parse");
        assert_eq!(parsed[0].title, meta.title);
        assert_eq!(parsed[0].content, meta.content);
    }

    #[test]
    fn feed_updated_uses_first_entry() {
        let xml = render(&[sample_meta()], "http://localhost");
        let xml_str = std::str::from_utf8(&xml).unwrap();
        assert!(xml_str.contains("<updated>2026-07-05T04:10:12+09:00</updated>"));
    }

    #[test]
    fn empty_feed_renders_with_now() {
        let xml = render(&[], "http://localhost");
        let xml_str = std::str::from_utf8(&xml).unwrap();
        assert!(xml_str.contains("<feed"));
        assert!(xml_str.contains("+09:00</updated>"));
        assert!(feed_parse::parse(xml_str).expect("must parse").is_empty());
    }
}
