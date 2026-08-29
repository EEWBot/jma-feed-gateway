//! connect 成功後に `start` が来ないセッションを落とす回帰テスト。
//!
//! DMDATA は接続成功時に `start` を送る。修正前は `start` が来なくてもセッションが
//! 維持され、サーバが生きている限り watchdog も発火しないため、readiness が false の
//! まま(= poll fallback 頼み)で WS が永久に復旧しない状態になり得た。
//!
//! ここでは ping 周期を十分長く取って watchdog が発火し得ない状況を作り、
//! `ws_start_timeout_secs` だけが再接続の引き金になることを検証する。

use std::sync::Arc;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Format, Toml};
use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use jma_feed_gateway::config::{Config, DEFAULT_CONFIG_TOML};
use jma_feed_gateway::dmdata::api::DmdataApi;
use jma_feed_gateway::dmdata::ws;
use jma_feed_gateway::state::{AppState, SharedState};
use jma_feed_gateway::types::Event;

/// start 受信の猶予。テストを短く保つため既定(30秒)より小さくする。
const START_TIMEOUT_SECS: u64 = 1;
/// watchdog がテスト中に発火しないだけの長さ。
const PING_INTERVAL_SECS: u64 = 30;
const PONG_TIMEOUT_SECS: u64 = 60;

/// ダミーWSサーバ。接続は受け付けるが `start` を送らず、受信だけして黙り続ける。
/// TCP は張ったままなので、切断を促せるのは start デッドラインだけ。
async fn spawn_silent_ws_server() -> (String, mpsc::UnboundedReceiver<tokio::time::Instant>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let (accepted_tx, accepted_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            if accepted_tx.send(tokio::time::Instant::now()).is_err() {
                return;
            }
            let Ok(ws) = tokio_tungstenite::accept_async(socket).await else {
                continue;
            };
            // 接続ごとに読み捨て専用タスク。次の accept をブロックしない。
            tokio::spawn(async move {
                let (_sink, mut stream) = ws.split();
                while stream.next().await.is_some() {}
            });
        }
    });

    (format!("ws://{addr}/"), accepted_rx)
}

/// ダミーWSサーバへ向いた state を作る。socket_start は wiremock が返す。
async fn setup(server: &MockServer, ws_endpoint: String) -> (SharedState, mpsc::Receiver<Event>) {
    Mock::given(method("POST"))
        .and(path("/socket"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "ok",
            "ticket": "TICKET123",
            "websocket": { "id": 1 },
        })))
        .mount(server)
        .await;

    let mut config: Config = Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load");
    config.dmdata.api_base = server.uri();
    config.dmdata.data_api_base = format!("{}/v1", server.uri());
    config.dmdata.cleanup_stale_sockets = false;
    config.dmdata.ws_endpoints = vec![ws_endpoint];
    config.dmdata.ws_start_timeout_secs = START_TIMEOUT_SECS;
    config.dmdata.ws_ping_interval_secs = PING_INTERVAL_SECS;
    config.dmdata.ws_pong_timeout_secs = PONG_TIMEOUT_SECS;
    config.dmdata.reconnect.initial_secs = 1;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel::<Event>(64);
    let dmdata_api = DmdataApi::new(
        client,
        config.dmdata.api_base.clone(),
        config.dmdata.data_api_base.clone(),
        "test-api-key",
        None,
    );
    let config = Arc::new(config);
    (Arc::new(AppState::new(config, dmdata_api, tx)), rx)
}

/// start が来ないセッションは start デッドラインで捨てられ、再接続すること。
#[tokio::test]
async fn session_without_start_is_dropped_and_reconnects() {
    let mock = MockServer::start().await;
    let (endpoint, mut accepted) = spawn_silent_ws_server().await;
    let (state, _rx) = setup(&mock, endpoint).await;

    let task = tokio::spawn(ws::run_connection(
        0,
        state.config.dmdata.ws_endpoints[0].clone(),
        state.event_tx.clone(),
        state.clone(),
    ));

    let mut next_accept = async || {
        tokio::time::timeout(Duration::from_secs(20), accepted.recv())
            .await
            .expect("session must be dropped and reconnect without start")
            .expect("server task must stay alive")
    };
    let first = next_accept().await;
    let second = next_accept().await;

    // start 猶予(1秒)+ バックオフ(1秒)+ ジッタ(<1秒)。watchdog は 30 秒後まで
    // ping すら送らないので、この時間内の再接続は start デッドライン由来しかない。
    let gap = second - first;
    assert!(
        gap >= Duration::from_secs(START_TIMEOUT_SECS),
        "session must survive until the start deadline, but it reconnected after {gap:?}"
    );
    assert!(
        gap < Duration::from_secs(10),
        "session without start must be dropped by the start deadline, but the reconnect gap was {gap:?}"
    );
    // start 未受信なので readiness は一度も接続済みにならない。
    assert!(!state.readiness.is_ws_connected(0));

    task.abort();
}
