//! DMDATA WebSocket接続タスク。
//! 受信→body展開→`Event`構築→mpsc送信のみを行い、キャッシュには触れない。
//! 参照: docs/gateway/DmdataGateway.java
//!
//! # タスク構成
//!
//! 接続1本につき2タスクに分ける。WebSocketのcontrol plane(read / JSON ping応答 /
//! watchdog ping-pong)を、aggregatorへのbackpressureから構造的に切り離すため。
//!
//! ```text
//! protocol task (run_session)
//!  ├─ stream read / JSON ping応答 / watchdog ping-pong
//!  └─ Event → ローカル境界付きキュー(LOCAL_QUEUE_CAPACITY)
//!
//! forwarder task (接続ごと、run_connection が spawn)
//!  └─ ローカルキュー → aggregator の event_tx
//! ```
//!
//! 不変条件:
//! - protocol task は `local_tx` にしか送らない。`event_tx` を直接触らない。
//! - `forward_events` が `event_tx` への唯一の無制限 `.await` 点。
//! - ローカルキュー満杯 = aggregator 停滞。stream の読み取りを止めて取りこぼしを
//!   防ぐ(try_sendは使わない)が、`reserve()` を ping/watchdog と同じ `select!` に
//!   置くため control plane は生き続ける。pong を読めないので `PONG_TIMEOUT` で
//!   watchdog が発火し、セッションを落として `ws_connected=false` にする。
//!   「静かに詰まったまま接続中を主張する」より「切れて poll fallback に渡す」を選ぶ。
//! - sink への write は `WRITE_TIMEOUT` 付き。network write が固まったまま
//!   watchdog へ戻れなくなるのを防ぐ。

use std::time::Duration;

use futures_util::{Sink, SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::dmdata::api::{DmdataApi, SocketStartRequest};
use crate::dmdata::body::decode_body;
use crate::dmdata::protocol::{WsClientPing, WsClientPong, WsData, WsMessage};
use crate::error::DmdataError;
use crate::jma::entity_parse::parse_entity_meta;
use crate::state::{SharedState, WsConnectionGuard};
use crate::supervisor::TaskExit;
use crate::types::{DedupKey, Event, EventSource, ItemMeta, normalize_rfc3339_to_jst};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const SOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
const LOCAL_QUEUE_CAPACITY: usize = 256;
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

struct PendingPing {
    id: String,
    sent_at: tokio::time::Instant,
}

struct PingWatchdog {
    conn: usize,
    seq: u64,
    pending: Option<PendingPing>,
    pong_timeout: Duration,
}

impl PingWatchdog {
    fn with_timeout(conn: usize, pong_timeout: Duration) -> Self {
        Self {
            conn,
            seq: 0,
            pending: None,
            pong_timeout,
        }
    }

    fn next_ping(&mut self, now: tokio::time::Instant) -> Option<WsClientPing> {
        if self.pending.is_some() {
            return None;
        }
        self.seq += 1;
        let id = format!("wd{}-{}", self.conn, self.seq);
        self.pending = Some(PendingPing {
            id: id.clone(),
            sent_at: now,
        });
        Some(WsClientPing::new(id))
    }

    fn on_pong(&mut self, ping_id: Option<&str>) -> bool {
        let matched = matches!(
            (&self.pending, ping_id),
            (Some(pending), Some(id)) if pending.id == id
        );
        if matched {
            self.pending = None;
        }
        matched
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.pending
            .as_ref()
            .map(|pending| pending.sent_at + self.pong_timeout)
    }
}

/// 受信メッセージ1件に対して呼び出し側が行うべきアクション(純粋関数の出力)。
#[derive(Debug)]
pub enum WsAction {
    /// 何もしない(pong受信、パース不能等)。
    None,
    /// startメッセージ受信。readiness を接続済みにする。
    Started { socket_id: Option<i64> },
    /// テキストを返信する(DMDATAのJSON pingへのpong応答)。
    Reply(String),
    /// pong受信。watchdogの生存タイマをリセットする。
    Pong { ping_id: Option<String> },
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
            WsAction::Started {
                socket_id: start.socket_id,
            }
        }
        WsMessage::Ping(ping) => {
            // DMDATAのJSON pingにはJSONで応答する(WSプロトコルpingとは別物)
            tracing::trace!(conn = conn_index, ping_id = ?ping.ping_id, "ws ping");
            WsAction::Reply(WsClientPong::reply_to(&ping).to_json())
        }
        WsMessage::Pong(pong) => WsAction::Pong {
            ping_id: pong.ping_id,
        },
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
        xml_body: Some(xml_body),
        meta,
    }))
}

/// ローカルキュー → aggregator の中継タスク。
///
/// `event_tx` を無制限に `.await` してよい唯一の場所。ここが詰まっても
/// protocol task の control plane(ping/pong/watchdog)は止まらない。
///
/// 終了条件は2つ:
/// 1. protocol 側が `local_tx` を drop した(接続タスク自体の終了)。
/// 2. `event_tx` がクローズした(aggregator が消えた)。
///
/// どちらでも `rx` が drop されるため、protocol 側の `local_tx.reserve()` /
/// `try_reserve()` が `Closed` を返す。これが「aggregator が消えた」の伝播経路。
async fn forward_events(index: usize, mut rx: mpsc::Receiver<Event>, tx: mpsc::Sender<Event>) {
    while let Some(event) = rx.recv().await {
        if tx.send(event).await.is_err() {
            tracing::warn!(conn = index, "event channel closed; ws forwarder exiting");
            return;
        }
    }
    tracing::debug!(conn = index, "ws forwarder finished (protocol task ended)");
}

/// forwarder タスクの生存期間を接続タスクに束ねる Drop ガード。
///
/// `WsConnectionGuard` / `PollActiveGuard` と同じ契約 — panic unwind でも
/// future の drop(cancel)でも forwarder が孤児として残らない。
/// abort のためキュー在中の Event はその時点で失われるが、これが起きるのは
/// プロセス停止時か supervisor による再起動時に限られる。
struct ForwarderGuard {
    handle: JoinHandle<()>,
}

impl ForwarderGuard {
    fn new(handle: JoinHandle<()>) -> Self {
        Self { handle }
    }
}

impl Drop for ForwarderGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// sink へのテキスト送信。`WRITE_TIMEOUT` を超えたらセッションを落とす。
/// timeout 後の sink は不定状態だが、呼び出し側はセッションごと捨てる。
async fn send_text<S>(sink: &mut S, text: String, what: &str) -> Result<(), DmdataError>
where
    S: Sink<Message> + Unpin,
    <S as Sink<Message>>::Error: std::fmt::Display,
{
    match tokio::time::timeout(WRITE_TIMEOUT, sink.send(Message::text(text))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(DmdataError::Ws(format!("{what} send failed: {e}"))),
        Err(_) => Err(DmdataError::Ws(format!(
            "{what} send timed out after {}s",
            WRITE_TIMEOUT.as_secs()
        ))),
    }
}

/// ping tick 1回分。pong 待ちが残っていれば何もしない。
async fn send_ping<S>(
    sink: &mut S,
    watchdog: &mut PingWatchdog,
    index: usize,
) -> Result<(), DmdataError>
where
    S: Sink<Message> + Unpin,
    <S as Sink<Message>>::Error: std::fmt::Display,
{
    let Some(ping) = watchdog.next_ping(tokio::time::Instant::now()) else {
        return Ok(());
    };
    tracing::trace!(conn = index, ping_id = %ping.ping_id, "ws watchdog ping");
    send_text(sink, ping.to_json(), "ping").await
}

/// watchdog 発火時の後始末。残存ソケットを非同期で閉じ、セッションのエラーを返す。
fn on_watchdog_timeout(
    api: &DmdataApi,
    index: usize,
    socket_id: Option<i64>,
    pong_timeout: Duration,
) -> DmdataError {
    tracing::warn!(
        conn = index,
        timeout_secs = pong_timeout.as_secs(),
        "pong watchdog timed out; dropping session"
    );
    spawn_socket_close(api, index, socket_id);
    DmdataError::Ws(format!(
        "pong not received within {}s",
        pong_timeout.as_secs()
    ))
}

/// `deadline` が無ければ永久に待つ(= select! のそのアームを無効化する)。
async fn sleep_until_opt(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// WS接続タスク: 認可→接続→受信ループを繰り返す。切断時は指数バックオフで再接続。
pub async fn run_connection(
    index: usize,
    endpoint: String,
    tx: mpsc::Sender<Event>,
    state: SharedState,
) -> TaskExit {
    let _ws_guard = WsConnectionGuard::new(state.clone(), index);

    let cfg = &state.config.dmdata;
    let api = state.dmdata_api.clone();
    let app_name = format!("{}-{}", cfg.app_name, index + 1);

    let initial_backoff = Duration::from_secs(cfg.reconnect.initial_secs.max(1));
    let max_backoff = Duration::from_secs(cfg.reconnect.max_secs.max(1));
    let multiplier = cfg.reconnect.multiplier.max(1.0);
    let mut backoff = initial_backoff;

    // ローカルキューは接続タスク全体で1本。再接続バックオフ中も forwarder が
    // 走り続けるため、セッション終了時にキュー在中だった Event は捨てずに済む。
    let (local_tx, local_rx) = mpsc::channel::<Event>(LOCAL_QUEUE_CAPACITY);
    let _forwarder = ForwarderGuard::new(tokio::spawn(forward_events(index, local_rx, tx.clone())));

    loop {
        let session = run_session(index, &endpoint, &api, &app_name, &local_tx, &state).await;
        // フラグを落とす前に読む。start受信済み(=購読確立)なら、セッションの
        // 終わり方がOk/Errどちらでもバックオフをリセットする対象。
        let was_connected = state.readiness.is_ws_connected(index);
        state.readiness.mark_ws_disconnected(index);
        // `tx` は本物の event_tx クローン。aggregator の消滅はここで直接見る。
        // `local_tx` 側は forwarder が先に落ちたケース(同じ原因だが伝播が非同期)。
        if tx.is_closed() || local_tx.is_closed() {
            tracing::warn!(conn = index, "event channel closed; ws task exiting");
            return TaskExit::Done;
        }
        match session {
            Ok(()) => tracing::info!(conn = index, "ws session ended"),
            Err(e) => tracing::warn!(conn = index, error = %e, "ws session failed"),
        }
        // 接続確立まで到達したセッションの後はバックオフをリセット
        if was_connected {
            backoff = initial_backoff;
        }
        let jitter = Duration::from_millis(rand::random_range(0..1000));
        tracing::info!(conn = index, backoff = ?backoff, "ws reconnecting after backoff");
        tokio::time::sleep(backoff + jitter).await;
        backoff = std::cmp::min(backoff.mul_f64(multiplier), max_backoff);
    }
}

/// 1セッション分: (設定により)残存ソケット掃除 → socket_start → connect → 受信ループ。
async fn run_session(
    index: usize,
    endpoint: &str,
    api: &DmdataApi,
    app_name: &str,
    local_tx: &mpsc::Sender<Event>,
    state: &SharedState,
) -> Result<(), DmdataError> {
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
    let mut socket_id = start.websocket.as_ref().map(|ws| ws.id);
    let url = format!("{endpoint}?ticket={}", start.ticket);

    let (ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url.as_str()))
        .await
        .map_err(|_| DmdataError::Ws("connect timed out after 30s".into()))?
        .map_err(|e| DmdataError::Ws(format!("connect failed: {e}")))?;
    tracing::info!(conn = index, endpoint, "ws connected");

    let (mut sink, mut stream) = ws.split();

    let ping_every = Duration::from_secs(cfg.ws_ping_interval_secs.max(1));
    let pong_timeout = Duration::from_secs(cfg.ws_pong_timeout_secs.max(1));
    let mut ping_interval = tokio::time::interval(ping_every);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping_interval.tick().await;
    let mut watchdog = PingWatchdog::with_timeout(index, pong_timeout);
    let mut pending: Option<Event> = None;
    let mut stalled_since: Option<tokio::time::Instant> = None;

    // 受信ループ。WSプロトコルPingはtungsteniteが自動Pongするが、
    // そのためにもストリームをpollし続ける必要がある。
    loop {
        let deadline = watchdog.deadline();
        if let Some(event) = pending.take() {
            tokio::select! {
                permit = local_tx.reserve() => {
                    let permit = permit
                        .map_err(|_| DmdataError::Ws("event channel closed".into()))?;
                    permit.send(event);
                    if let Some(since) = stalled_since.take() {
                        tracing::info!(
                            conn = index,
                            stalled_ms = since.elapsed().as_millis() as u64,
                            "aggregator backpressure cleared; ws read resumed"
                        );
                    }
                }
                _ = ping_interval.tick() => {
                    // 同一 local を複数アームで move してよい(実行されるのは1アームのみ)
                    pending = Some(event);
                    send_ping(&mut sink, &mut watchdog, index).await?;
                }
                () = sleep_until_opt(deadline) => {
                    return Err(on_watchdog_timeout(api, index, socket_id, pong_timeout));
                }
            }
            continue;
        }

        tokio::select! {
            item = stream.next() => {
                let Some(item) = item else { break };
                let message = item.map_err(|e| DmdataError::Ws(format!("receive failed: {e}")))?;
                match message {
                    Message::Text(text) => match handle_ws_message(text.as_str(), index) {
                        WsAction::None => {}
                        WsAction::Started { socket_id: id } => {
                            if id.is_some() {
                                socket_id = id;
                            }
                            // start受信=購読確立。全断エピソード後ならcatch-up pollが通知される
                            state.readiness.mark_ws_connected(index);
                        }
                        WsAction::Reply(json) => {
                            send_text(&mut sink, json, "pong").await?;
                        }
                        WsAction::Pong { ping_id } => {
                            if watchdog.on_pong(ping_id.as_deref()) {
                                tracing::trace!(conn = index, ?ping_id, "ws pong received");
                            } else {
                                tracing::warn!(conn = index, ?ping_id, "pong ignored: unexpected or missing pingId");
                            }
                        }
                        WsAction::Publish(event) => {
                            match local_tx.try_reserve() {
                                Ok(permit) => permit.send(*event),
                                Err(mpsc::error::TrySendError::Full(())) => {
                                    tracing::warn!(
                                        conn = index,
                                        capacity = LOCAL_QUEUE_CAPACITY,
                                        "aggregator backpressure; ws read paused"
                                    );
                                    stalled_since = Some(tokio::time::Instant::now());
                                    pending = Some(*event);
                                }
                                Err(mpsc::error::TrySendError::Closed(())) => {
                                    return Err(DmdataError::Ws("event channel closed".into()));
                                }
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
            _ = ping_interval.tick() => {
                send_ping(&mut sink, &mut watchdog, index).await?;
            }
            () = sleep_until_opt(deadline) => {
                return Err(on_watchdog_timeout(api, index, socket_id, pong_timeout));
            }
        }
    }

    Ok(())
}

fn spawn_socket_close(api: &DmdataApi, index: usize, socket_id: Option<i64>) {
    let Some(socket_id) = socket_id else {
        tracing::warn!(
            conn = index,
            "socket id unknown; skipping dmdata socket close"
        );
        return;
    };
    let api = api.clone();
    tokio::spawn(async move {
        match tokio::time::timeout(SOCKET_CLOSE_TIMEOUT, api.socket_close(socket_id)).await {
            Ok(Ok(())) => tracing::info!(
                conn = index,
                socket_id,
                "closed dmdata socket after watchdog timeout"
            ),
            Ok(Err(e)) => tracing::warn!(
                conn = index,
                socket_id,
                error = %e,
                "failed to close dmdata socket after watchdog timeout"
            ),
            Err(_) => tracing::warn!(conn = index, socket_id, "dmdata socket close timed out"),
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// config/default.toml の既定値と同じ値。watchdogの算術を検証するためのもので、
    /// 実行時の値は `dmdata.ws_ping_interval_secs` / `ws_pong_timeout_secs` から来る。
    const PING_INTERVAL: Duration = Duration::from_secs(30);
    const PONG_TIMEOUT: Duration = Duration::from_secs(60);

    fn watchdog(conn: usize) -> PingWatchdog {
        PingWatchdog::with_timeout(conn, PONG_TIMEOUT)
    }

    const START_JSON: &str = include_str!("../../tests/fixtures/ws_start.json");
    const PING_JSON: &str = include_str!("../../tests/fixtures/ws_ping.json");
    const DATA_JSON: &str = include_str!("../../tests/fixtures/ws_data.json");
    const ERROR_JSON: &str = include_str!("../../tests/fixtures/ws_error.json");

    #[test]
    fn start_returns_started_with_socket_id() {
        let WsAction::Started { socket_id } = handle_ws_message(START_JSON, 0) else {
            panic!("expected started");
        };
        assert_eq!(socket_id, Some(12345));
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
    fn pong_returns_pong_action() {
        let WsAction::Pong { ping_id } =
            handle_ws_message(r#"{"type":"pong","pingId":"wd0-1"}"#, 0)
        else {
            panic!("expected pong");
        };
        assert_eq!(ping_id.as_deref(), Some("wd0-1"));
    }

    #[test]
    fn pong_without_ping_id_deserializes() {
        // deserializeは通るが、watchdog ACKとしては扱わない
        // (pong_without_ping_id_is_not_acked を参照)
        let WsAction::Pong { ping_id } = handle_ws_message(r#"{"type":"pong"}"#, 0) else {
            panic!("expected pong");
        };
        assert!(ping_id.is_none());
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
        let body = event.xml_body.as_ref().expect("ws event must carry a body");
        assert!(std::str::from_utf8(body).unwrap().contains("<Report"));
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
    fn next_ping_returns_none_while_pong_is_outstanding() {
        let now = tokio::time::Instant::now();
        let mut watchdog = watchdog(0);
        let ping = watchdog.next_ping(now).expect("first ping");
        assert_eq!(ping.ping_id, "wd0-1");
        assert!(watchdog.next_ping(now + PING_INTERVAL).is_none());
    }

    #[test]
    fn matching_pong_clears_pending() {
        let now = tokio::time::Instant::now();
        let mut watchdog = watchdog(0);
        watchdog.next_ping(now).expect("first ping");
        assert!(watchdog.on_pong(Some("wd0-1")));
        assert!(watchdog.deadline().is_none());
        let ping = watchdog
            .next_ping(now + PING_INTERVAL)
            .expect("second ping");
        assert_eq!(ping.ping_id, "wd0-2");
    }

    #[test]
    fn unknown_pong_id_does_not_clear_pending() {
        let now = tokio::time::Instant::now();
        let mut watchdog = watchdog(0);
        watchdog.next_ping(now).expect("first ping");
        assert!(!watchdog.on_pong(Some("bogus")));
        assert_eq!(watchdog.deadline(), Some(now + PONG_TIMEOUT));
    }

    #[test]
    fn pong_without_ping_id_is_not_acked() {
        let now = tokio::time::Instant::now();
        let mut watchdog = watchdog(0);
        watchdog.next_ping(now).expect("first ping");
        assert!(!watchdog.on_pong(None));
        assert_eq!(watchdog.deadline(), Some(now + PONG_TIMEOUT));
    }

    #[test]
    fn pong_without_pending_ping_is_not_matched() {
        let mut watchdog = watchdog(0);
        assert!(!watchdog.on_pong(None));
        assert!(watchdog.deadline().is_none());
    }

    #[test]
    fn deadline_is_ping_sent_at_plus_timeout() {
        // 生存判定の起点はping送信時刻。初回猶予が30秒に縮まないことの回帰テスト。
        let now = tokio::time::Instant::now();
        let mut watchdog = watchdog(3);
        let ping = watchdog.next_ping(now).expect("first ping");
        assert_eq!(ping.ping_id, "wd3-1");
        assert_eq!(watchdog.deadline(), Some(now + PONG_TIMEOUT));
    }

    fn dummy_event(id: &str) -> Event {
        Event {
            source: EventSource::DmdataPoll,
            dedup_key: DedupKey::TelegramId(id.into()),
            xml_body: None,
            meta: ItemMeta {
                id: id.into(),
                title: String::new(),
                updated: String::new(),
                author: String::new(),
                content: String::new(),
            },
        }
    }

    #[tokio::test]
    async fn forwarder_relays_in_order_through_a_narrow_downstream() {
        // 下流容量1 = 常に詰まる状態。それでも順序は保たれ1件も落ちない
        let (local_tx, local_rx) = mpsc::channel::<Event>(8);
        let (tx, mut rx) = mpsc::channel::<Event>(1);
        let handle = tokio::spawn(forward_events(0, local_rx, tx));

        for id in ["a", "b", "c"] {
            local_tx.send(dummy_event(id)).await.expect("send");
        }
        for id in ["a", "b", "c"] {
            assert_eq!(rx.recv().await.expect("relayed").meta.id, id);
        }

        // local_tx を drop すれば forwarder は自然終了する
        drop(local_tx);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("forwarder must finish")
            .expect("forwarder must not panic");
    }

    #[tokio::test]
    async fn downstream_close_propagates_to_local_tx() {
        // 「aggregator が消えた」の伝播経路: rx drop → forwarder 終了 →
        // local_rx drop → protocol 側の reserve() が Closed を返す
        let (local_tx, local_rx) = mpsc::channel::<Event>(8);
        let (tx, rx) = mpsc::channel::<Event>(1);
        tokio::spawn(forward_events(0, local_rx, tx));
        drop(rx);

        // 1件送ると forwarder がそれを下流へ送ろうとして閉鎖を検知する
        local_tx.send(dummy_event("a")).await.expect("first send");
        tokio::time::timeout(Duration::from_secs(1), local_tx.closed())
            .await
            .expect("local_tx must observe closure");
        assert!(local_tx.reserve().await.is_err());
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
