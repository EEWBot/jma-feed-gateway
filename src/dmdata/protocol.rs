//! DMDATA WebSocketメッセージのserde型。
//! 参照: docs/entity/dmdata/ws/*.java

use serde::{Deserialize, Serialize};

/// WSメッセージ。`type` フィールドで判別する。
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WsMessage {
    Start(WsStart),
    Ping(WsPing),
    Pong(WsPong),
    Data(Box<WsData>),
    Error(WsError),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsStart {
    #[serde(default)]
    pub socket_id: Option<i64>,
    #[serde(default)]
    pub classifications: Vec<String>,
    #[serde(default)]
    pub types: Option<Vec<String>>,
    #[serde(default)]
    pub test: Option<String>,
    #[serde(default)]
    pub formats: Vec<String>,
    #[serde(default)]
    pub app_name: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsPing {
    #[serde(default)]
    pub ping_id: Option<String>,
}

/// DMDATAのJSON ping への応答。`{"type":"pong","pingId":<同値>}` を返す。
/// (WSプロトコルレベルのping/pongとは別物。プロトコルpingはtungsteniteが自動応答する)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsPong {
    #[serde(rename = "type")]
    pub message_type: PongType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PongType {
    Pong,
}

impl WsPong {
    pub fn reply_to(ping: &WsPing) -> Self {
        Self {
            message_type: PongType::Pong,
            ping_id: ping.ping_id.clone(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("WsPong serialization cannot fail")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsData {
    /// "2.0" を期待。
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub classification: Option<String>,
    /// DMDATA電文一意ID(dedupキー)。
    pub id: String,
    #[serde(default)]
    pub passing: Vec<Passing>,
    #[serde(default)]
    pub head: Option<DataHead>,
    #[serde(default)]
    pub xml_report: Option<XmlReport>,
    #[serde(default)]
    pub format: Option<String>,
    /// "gzip" / "zip" / null。
    #[serde(default)]
    pub compression: Option<String>,
    /// "base64" / null。
    #[serde(default)]
    pub encoding: Option<String>,
    /// XML実体(インライン)。
    pub body: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Passing {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataHead {
    /// 電文種別コード(例: VXSE53)。
    #[serde(rename = "type")]
    pub telegram_type: String,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
    #[serde(default)]
    pub test: bool,
    #[serde(default)]
    pub xml: bool,
}

/// JSON側のXMLメタ情報。展開済みXML bodyのパースが正で、これはフォールバック。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XmlReport {
    #[serde(default)]
    pub control: Option<XmlControl>,
    #[serde(default)]
    pub head: Option<XmlHead>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XmlControl {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub date_time: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub editorial_office: Option<String>,
    #[serde(default)]
    pub publishing_office: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XmlHead {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub report_date_time: Option<String>,
    #[serde(default)]
    pub target_date_time: Option<String>,
    #[serde(default)]
    pub event_id: Option<String>,
    #[serde(default)]
    pub serial: Option<String>,
    #[serde(default)]
    pub info_type: Option<String>,
    #[serde(default)]
    pub info_kind: Option<String>,
    #[serde(default)]
    pub info_kind_version: Option<String>,
    #[serde(default)]
    pub headline: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WsError {
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub code: Option<i64>,
    #[serde(default)]
    pub close: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    const START_JSON: &str = include_str!("../../tests/fixtures/ws_start.json");
    const PING_JSON: &str = include_str!("../../tests/fixtures/ws_ping.json");
    const DATA_JSON: &str = include_str!("../../tests/fixtures/ws_data.json");
    const ERROR_JSON: &str = include_str!("../../tests/fixtures/ws_error.json");

    #[test]
    fn deserializes_start() {
        let msg: WsMessage = serde_json::from_str(START_JSON).expect("start must parse");
        let WsMessage::Start(start) = msg else {
            panic!("expected start");
        };
        assert_eq!(start.socket_id, Some(12345));
        assert_eq!(start.classifications, vec!["telegram.earthquake"]);
        assert_eq!(start.app_name.as_deref(), Some("jma-feed-gateway-1"));
    }

    #[test]
    fn deserializes_ping_and_builds_pong() {
        let msg: WsMessage = serde_json::from_str(PING_JSON).expect("ping must parse");
        let WsMessage::Ping(ping) = msg else {
            panic!("expected ping");
        };
        assert_eq!(ping.ping_id.as_deref(), Some("nBglV1"));

        let pong = WsPong::reply_to(&ping).to_json();
        let value: serde_json::Value = serde_json::from_str(&pong).unwrap();
        assert_eq!(value["type"], "pong");
        assert_eq!(value["pingId"], "nBglV1");
    }

    #[test]
    fn pong_omits_ping_id_when_ping_has_none() {
        let msg: WsMessage = serde_json::from_str(r#"{"type":"ping"}"#).expect("ping must parse");
        let WsMessage::Ping(ping) = msg else {
            panic!("expected ping");
        };
        assert!(ping.ping_id.is_none());

        let pong = WsPong::reply_to(&ping).to_json();
        assert_eq!(pong, r#"{"type":"pong"}"#);
    }

    #[test]
    fn deserializes_data() {
        let msg: WsMessage = serde_json::from_str(DATA_JSON).expect("data must parse");
        let WsMessage::Data(data) = msg else {
            panic!("expected data");
        };
        assert_eq!(data.version.as_deref(), Some("2.0"));
        assert_eq!(data.id, "TELEGRAM_ID_1");
        assert!(data.compression.is_none());
        let head = data.head.as_ref().expect("head");
        assert_eq!(head.telegram_type, "VXSE53");
        assert!(!head.test);
        let report = data.xml_report.as_ref().expect("xmlReport");
        assert_eq!(
            report.control.as_ref().unwrap().date_time.as_deref(),
            Some("2026-07-04T19:10:00Z")
        );
        assert_eq!(report.head.as_ref().unwrap().serial.as_deref(), Some("2"));
        assert!(data.body.contains("<Report"));
        assert_eq!(data.passing.len(), 2);
    }

    #[test]
    fn deserializes_error() {
        let msg: WsMessage = serde_json::from_str(ERROR_JSON).expect("error must parse");
        let WsMessage::Error(err) = msg else {
            panic!("expected error");
        };
        assert_eq!(err.code, Some(4103));
        assert!(err.close);
    }

    #[test]
    fn unknown_type_is_error() {
        assert!(serde_json::from_str::<WsMessage>(r#"{"type":"mystery"}"#).is_err());
    }
}
