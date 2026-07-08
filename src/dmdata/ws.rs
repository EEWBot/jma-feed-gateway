//! DMDATA WebSocket接続タスク。
//! 受信→body展開→`Event`構築→mpsc送信のみを行い、キャッシュには触れない。
//! 参照: docs/gateway/DmdataGateway.java

use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::dmdata::api::{DmdataApi, SocketStartRequest};
use crate::dmdata::body::decode_body;
use crate::dmdata::protocol::{WsData, WsMessage, WsPong};
use crate::error::DmdataError;
use crate::jma::entity_parse::parse_entity_meta;
use crate::state::SharedState;
use crate::types::{DedupKey, Event, EventSource, ItemMeta, normalize_rfc3339_to_jst};

/// 受信メッセージ1件に対して呼び出し側が行うべきアクション(純粋関数の出力)。
#[derive(Debug)]
pub enum WsAction {
    /// 何もしない(pong受信、パース不能等)。
    None,
    /// startメッセージ受信。readiness を接続済みにする。
    Started,
    /// テキストを返信する(DMDATAのJSON pingへのpong応答)。
    Reply(String),
    /// Event を aggregator へ送る。
    Publish(Box<Event>),
    /// サーバ指示によりクローズして再接続する。
    Close { reason: String },
}

/// WSテキストメッセージ1件を処理してアクションを返す(I/Oなし・テスト可能)。
pub fn handle_ws_message(text: &str, conn_index: usize) -> WsAction {
    let message: WsMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(conn = conn_index, error = %e, "failed to parse ws message");
            return WsAction::None;
        }
    };
    match message {
        WsMessage::Start(start) => {
            tracing::info!(conn = conn_index, app_name = ?start.app_name, socket_id = ?start.socket_id, "ws start received");
            WsAction::Started
        }
        WsMessage::Ping(ping) => {
            // DMDATAのJSON pingにはJSONで応答する(WSプロトコルpingとは別物)
            tracing::trace!(conn = conn_index, ping_id = ?ping.ping_id, "ws ping");
            WsAction::Reply(WsPong::reply_to(&ping).to_json())
        }
        WsMessage::Pong(_) => WsAction::None,
        WsMessage::Error(error) => {
            tracing::error!(conn = conn_index, code = ?error.code, message = ?error.error, close = error.close, "ws error message");
            if error.close {
                WsAction::Close {
                    reason: error.error.unwrap_or_else(|| "server error".into()),
                }
            } else {
                WsAction::None
            }
        }
        WsMessage::Data(data) => match build_event(*data, conn_index) {
            Ok(Some(event)) => WsAction::Publish(Box::new(event)),
            Ok(None) => WsAction::None,
            Err(e) => {
                tracing::warn!(conn = conn_index, error = %e, "failed to process ws data");
                WsAction::None
            }
        },
    }
}

/// XMLの値を優先し、空ならJSON(xmlReport)側の値にフォールバックする。
fn pick(primary: &str, fallback: Option<&String>) -> String {
    if !primary.is_empty() {
        primary.to_string()
    } else {
        fallback.cloned().unwrap_or_default()
    }
}

/// dataメッセージからEventを構築する。テスト電文はスキップ(None)。
fn build_event(data: WsData, conn_index: usize) -> Result<Option<Event>, DmdataError> {
    if data.version.as_deref() != Some("2.0") {
        tracing::warn!(conn = conn_index, version = ?data.version, "ws data version is not 2.0, may not be compatible");
    }

    let head = data
        .head
        .as_ref()
        .ok_or_else(|| DmdataError::Body("data message has no head".into()))?;
    if head.test {
        tracing::debug!(conn = conn_index, id = %data.id, "test telegram skipped");
        return Ok(None);
    }
    // dmdataは常に電文IDを保証する。空IDは不正エントリとしてガードし破棄する。
    if data.id.is_empty() {
        return Err(DmdataError::Body("data message has empty id".into()));
    }
    let telegram_type = head.telegram_type.clone();

    let xml_body = decode_body(
        &data.body,
        data.compression.as_deref(),
        data.encoding.as_deref(),
    )?;

    // メタ抽出は展開済みXML(Control/Head)を正とし、xmlReport(JSON)はフォールバック
    let entity_meta = std::str::from_utf8(&xml_body)
        .ok()
        .and_then(|xml| parse_entity_meta(xml).ok())
        .unwrap_or_default();
    let report = data.xml_report.unwrap_or_default();
    let control = report.control.unwrap_or_default();
    let xml_head = report.head.unwrap_or_default();

    // entry ID はDMDATAの電文一意IDをそのまま使う(空IDは前段でガード済み)。
    let id = data.id.clone();

    let mut updated = pick(
        &entity_meta.report_date_time,
        xml_head.report_date_time.as_ref(),
    );
    if updated.is_empty() {
        updated = head.time.clone().unwrap_or_default();
    }
    // フォールバック(head.time)はZ表記UTCが混ざるため、select_item と同様に+09:00へ統一する
    let updated = normalize_rfc3339_to_jst(&updated);
    let title = pick(&entity_meta.title, control.title.as_ref());
    let author = pick(
        &entity_meta.publishing_office,
        control.publishing_office.as_ref(),
    );
    let content = pick(&entity_meta.headline_text, xml_head.headline.as_ref());

    let meta = ItemMeta {
        id: id.clone(),
        title: if title.is_empty() {
            telegram_type.clone()
        } else {
            title
        },
        updated: updated.clone(),
        author,
        content,
    };

    // dedupはDMDATA電文一意ID。空IDは前段でガード済み。
    let dedup_key = DedupKey::TelegramId(data.id.clone());

    Ok(Some(Event {
        source: EventSource::Dmdata {
            telegram_id: data.id,
            conn: conn_index,
        },
        dedup_key,
        xml_body,
        meta,
    }))
}

/// WS接続タスク: 認可→接続→受信ループを繰り返す。切断時は指数バックオフで再接続。
pub async fn run_connection(
    index: usize,
    endpoint: String,
    tx: mpsc::Sender<Event>,
    state: SharedState,
) {
    let cfg = &state.config.dmdata;
    let api = state.dmdata_api.clone();
    let app_name = format!("{}-{}", cfg.app_name, index + 1);

    let initial_backoff = Duration::from_secs(cfg.reconnect.initial_secs.max(1));
    let max_backoff = Duration::from_secs(cfg.reconnect.max_secs.max(1));
    let multiplier = cfg.reconnect.multiplier.max(1.0);
    let mut backoff = initial_backoff;

    loop {
        let session = run_session(index, &endpoint, &api, &app_name, &tx, &state).await;
        state.readiness.mark_ws_disconnected(index);
        if tx.is_closed() {
            tracing::warn!(conn = index, "event channel closed; ws task exiting");
            return;
        }
        match session {
            Ok(started) => {
                tracing::info!(conn = index, "ws session ended");
                if started {
                    // 接続確立まで到達したセッションの後はバックオフをリセット
                    backoff = initial_backoff;
                }
            }
            Err(e) => {
                tracing::warn!(conn = index, error = %e, "ws session failed");
            }
        }
        let jitter = Duration::from_millis(rand::random_range(0..1000));
        tracing::info!(conn = index, backoff = ?backoff, "ws reconnecting after backoff");
        tokio::time::sleep(backoff + jitter).await;
        backoff = std::cmp::min(backoff.mul_f64(multiplier), max_backoff);
    }
}

/// 1セッション分: (設定により)残存ソケット掃除 → socket_start → connect → 受信ループ。
/// 戻り値はstartメッセージを受信したかどうか。
async fn run_session(
    index: usize,
    endpoint: &str,
    api: &DmdataApi,
    app_name: &str,
    tx: &mpsc::Sender<Event>,
    state: &SharedState,
) -> Result<bool, DmdataError> {
    let cfg = &state.config.dmdata;

    // 同名appNameの残存ソケットを掃除(失敗しても続行)
    if cfg.cleanup_stale_sockets {
        match api.socket_list_open().await {
            Ok(list) => {
                for item in list
                    .items
                    .iter()
                    .filter(|item| item.app_name.as_deref() == Some(app_name))
                {
                    tracing::info!(
                        conn = index,
                        socket_id = item.id,
                        "closing stale dmdata socket"
                    );
                    if let Err(e) = api.socket_close(item.id).await {
                        tracing::warn!(conn = index, socket_id = item.id, error = %e, "failed to close stale socket");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(conn = index, error = %e, "failed to list open sockets");
            }
        }
    }

    // ticketは使い捨て: 接続のたびに socket_start で取り直す
    let request = SocketStartRequest::new(
        cfg.classifications.clone(),
        if cfg.types.is_empty() {
            None
        } else {
            Some(cfg.types.clone())
        },
        app_name.to_string(),
    );
    let start = api.socket_start(&request).await?;
    let url = format!("{endpoint}?ticket={}", start.ticket);

    let (ws, _) = connect_async(url.as_str())
        .await
        .map_err(|e| DmdataError::Ws(format!("connect failed: {e}")))?;
    tracing::info!(conn = index, endpoint, "ws connected");

    let (mut sink, mut stream) = ws.split();

    // 受信ループ。WSプロトコルPingはtungsteniteが自動Pongするが、
    // そのためにもストリームをpollし続ける必要がある。
    while let Some(item) = stream.next().await {
        let message = item.map_err(|e| DmdataError::Ws(format!("receive failed: {e}")))?;
        match message {
            Message::Text(text) => match handle_ws_message(text.as_str(), index) {
                WsAction::None => {}
                WsAction::Started => {
                    // start受信=購読確立。全断エピソード後ならcatch-up pollが通知される
                    state.readiness.mark_ws_connected(index);
                }
                WsAction::Reply(json) => {
                    sink.send(Message::text(json))
                        .await
                        .map_err(|e| DmdataError::Ws(format!("send failed: {e}")))?;
                }
                WsAction::Publish(event) => {
                    // send().await で取りこぼしなく送る(try_sendは使わない)
                    if tx.send(*event).await.is_err() {
                        return Err(DmdataError::Ws("event channel closed".into()));
                    }
                }
                WsAction::Close { reason } => {
                    return Err(DmdataError::Ws(format!("server requested close: {reason}")));
                }
            },
            Message::Close(frame) => {
                tracing::info!(conn = index, frame = ?frame, "ws closed by server");
                break;
            }
            // Ping/Pong/Binary等は無視(プロトコルPingは自動応答)
            _ => {}
        }
    }

    Ok(state
        .readiness
        .ws_connected
        .get(index)
        .map(|flag| flag.load(Ordering::Relaxed))
        .unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;

    const START_JSON: &str = include_str!("../../tests/fixtures/ws_start.json");
    const PING_JSON: &str = include_str!("../../tests/fixtures/ws_ping.json");
    const DATA_JSON: &str = include_str!("../../tests/fixtures/ws_data.json");
    const ERROR_JSON: &str = include_str!("../../tests/fixtures/ws_error.json");

    #[test]
    fn start_returns_started() {
        assert!(matches!(
            handle_ws_message(START_JSON, 0),
            WsAction::Started
        ));
    }

    #[test]
    fn ping_returns_pong_reply_with_same_id() {
        let WsAction::Reply(json) = handle_ws_message(PING_JSON, 0) else {
            panic!("expected reply");
        };
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["type"], "pong");
        assert_eq!(value["pingId"], "nBglV1");
    }

    #[test]
    fn error_with_close_returns_close() {
        let WsAction::Close { reason } = handle_ws_message(ERROR_JSON, 0) else {
            panic!("expected close");
        };
        assert!(reason.contains("Duplicate connection"));
    }

    #[test]
    fn garbage_returns_none() {
        assert!(matches!(handle_ws_message("not json", 0), WsAction::None));
        assert!(matches!(
            handle_ws_message(r#"{"type":"unknown"}"#, 0),
            WsAction::None
        ));
    }

    #[test]
    fn data_builds_event_from_xml_body() {
        let WsAction::Publish(event) = handle_ws_message(DATA_JSON, 1) else {
            panic!("expected publish");
        };
        // entry ID はDMDATA電文一意IDをそのまま使う
        assert_eq!(event.meta.id, "TELEGRAM_ID_1");
        assert_eq!(event.meta.title, "震源・震度に関する情報");
        assert_eq!(event.meta.updated, "2026-07-05T04:10:00+09:00");
        assert_eq!(event.meta.author, "気象庁");
        assert_eq!(event.meta.content, "5日04時05分ころ、地震がありました。");
        assert_eq!(
            event.dedup_key,
            DedupKey::TelegramId("TELEGRAM_ID_1".into())
        );
        assert_eq!(
            event.source,
            EventSource::Dmdata {
                telegram_id: "TELEGRAM_ID_1".into(),
                conn: 1
            }
        );
        assert!(
            std::str::from_utf8(&event.xml_body)
                .unwrap()
                .contains("<Report")
        );
    }

    #[test]
    fn data_falls_back_to_xml_report_when_body_is_not_parseable() {
        let mut value: serde_json::Value = serde_json::from_str(DATA_JSON).unwrap();
        value["body"] = serde_json::Value::String("<broken".into());
        let text = value.to_string();

        let WsAction::Publish(event) = handle_ws_message(&text, 0) else {
            panic!("expected publish via xmlReport fallback");
        };
        assert_eq!(event.meta.id, "TELEGRAM_ID_1");
        assert_eq!(event.meta.title, "震源・震度に関する情報");
        assert_eq!(event.meta.author, "気象庁");
    }

    #[test]
    fn test_telegram_is_skipped() {
        let mut value: serde_json::Value = serde_json::from_str(DATA_JSON).unwrap();
        value["head"]["test"] = serde_json::Value::Bool(true);
        let text = value.to_string();
        assert!(matches!(handle_ws_message(&text, 0), WsAction::None));
    }

    #[test]
    fn data_with_empty_id_returns_none() {
        // dmdataは常に電文IDを保証する。空IDは不正エントリとしてガードし破棄する。
        let mut value: serde_json::Value = serde_json::from_str(DATA_JSON).unwrap();
        value["id"] = serde_json::Value::String(String::new());
        let text = value.to_string();
        assert!(matches!(handle_ws_message(&text, 0), WsAction::None));
    }
}
