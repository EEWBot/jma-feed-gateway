//! WS control plane と aggregator backpressure の分離の統合テスト。
//!
//! 検証対象は「aggregator が詰まって Event を送れなくなっても、WS の
//! control plane(watchdog ping / pong)が止まらないこと」。
//!
//! これが崩れると:
//!   aggregator 停止 → mpsc 満杯 → `tx.send().await` で protocol task 停止
//!   → stream を poll しなくなる → server ping にも応答不能 → DMDATA が切断
//!   → gateway は send() の中なので切断を認識できず `ws_connected=true` のまま固着
//!   → poll fallback も動かず `/readyz` は 200 を返し続ける(無音故障)。
//!
//! backpressure 中は stream を読まない(取りこぼしを出さない)ため、サーバ側の
//! JSON ping には仕様上応答できない。代わりに **watchdog ping を撃ち続け、pong を
//! 読めないので必ずタイムアウトする** ことが観測可能性の担保になる。
//! したがってこのファイルは対になる2本で成立する。
//! 後者が無いと前者は「常に切れるだけ」の無意味なテストになる。
//!
//! - `saturated_downstream_trips_the_pong_watchdog` … 詰まったら必ず切れる
//! - `healthy_session_survives_several_ping_cycles` … 詰まっていなければ切れない

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Format, Toml};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use jma_feed_gateway::config::{Config, DEFAULT_CONFIG_TOML};
use jma_feed_gateway::dmdata::api::DmdataApi;
use jma_feed_gateway::dmdata::ws;
use jma_feed_gateway::state::{AppState, SharedState};
use jma_feed_gateway::types::Event;

/// テスト用の生存監視値。実時間で回すため既定(30/60秒)より大幅に短くする。
const PING_INTERVAL_SECS: u64 = 1;
const PONG_TIMEOUT_SECS: u64 = 3;

const START_JSON: &str = include_str!("fixtures/ws_start.json");
const DATA_JSON: &str = include_str!("fixtures/ws_data.json");

/// ローカルキュー(256)+ 下流チャネル + forwarder 在中分を確実に超える件数。
const BURST: usize = 400;

/// `id` だけ差し替えた data メッセージを作る。
fn data_message(seq: usize) -> String {
    let mut value: serde_json::Value = serde_json::from_str(DATA_JSON).expect("fixture is json");
    value["id"] = serde_json::Value::String(format!("TELEGRAM_{seq}"));
    value.to_string()
}

/// ダミーWSサーバ。接続を1本受けて `start` → data×`burst` を送り、
/// クライアントの watchdog ping には常に pong を返し続ける。
/// 戻り値は待ち受けアドレスと、「接続が閉じられた」の通知チャネル。
async fn spawn_ws_server(burst: usize) -> (String, tokio::sync::oneshot::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut ws = tokio_tungstenite::accept_async(socket)
            .await
            .expect("ws handshake");

        ws.send(Message::text(START_JSON))
            .await
            .expect("send start");
        for seq in 0..burst {
            if ws.send(Message::text(data_message(seq))).await.is_err() {
                break;
            }
        }

        // 以降は watchdog ping にひたすら pong を返す。
        // ストリームが終わる = クライアントが切った。
        while let Some(Ok(message)) = ws.next().await {
            let Message::Text(text) = message else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            if value["type"] == "ping" {
                let reply = serde_json::json!({
                    "type": "pong",
                    "pingId": value["pingId"].clone(),
                });
                if ws.send(Message::text(reply.to_string())).await.is_err() {
                    break;
                }
            }
        }
        let _ = closed_tx.send(());
    });

    (format!("ws://{addr}/"), closed_rx)
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
    config.dmdata.ws_ping_interval_secs = PING_INTERVAL_SECS;
    config.dmdata.ws_pong_timeout_secs = PONG_TIMEOUT_SECS;
    // 切断後に即再接続されると観測が難しいので、バックオフを長めに取る
    config.dmdata.reconnect.initial_secs = 30;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    // 下流は極小。受信しなければローカルキューごと即座に飽和する
    let (tx, rx) = mpsc::channel::<Event>(2);
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

/// `predicate` が真になるまで最大 `limit` 待つ。
async fn wait_until(limit: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    predicate()
}

/// 下流を一切読まずに詰まらせると、watchdog が発火してセッションが落ち、
/// `ws_connected` が false へ戻ること。
///
/// 修正前はここで protocol task が `tx.send().await` の中に居座り、
/// ping を1発も撃たないため `ws_connected` は true のまま固着した。
#[tokio::test]
async fn saturated_downstream_trips_the_pong_watchdog() {
    let mock = MockServer::start().await;
    let (endpoint, closed) = spawn_ws_server(BURST).await;
    // rx は保持するだけで recv しない = aggregator 停止の再現
    let (state, _rx) = setup(&mock, endpoint).await;

    let task = tokio::spawn(ws::run_connection(
        0,
        state.config.dmdata.ws_endpoints[0].clone(),
        state.event_tx.clone(),
        state.clone(),
    ));

    let connected = wait_until(Duration::from_secs(10), || {
        state.readiness.ws_connected[0].load(Ordering::Relaxed)
    })
    .await;
    assert!(connected, "ws must reach the connected state first");

    // 詰まった状態で watchdog が発火し、セッションが落ちること
    let dropped = wait_until(Duration::from_secs(20), || {
        !state.readiness.ws_connected[0].load(Ordering::Relaxed)
    })
    .await;
    assert!(
        dropped,
        "pong watchdog must drop the session while the aggregator is stalled"
    );

    // サーバ側から見ても実際に切断されていること(readinessだけの見せかけでない)
    tokio::time::timeout(Duration::from_secs(10), closed)
        .await
        .expect("server must observe the disconnect")
        .expect("server task must not panic");

    task.abort();
}

/// 対のテスト: 下流を読み続けていれば ping/pong が往復し、
/// 複数サイクル経ってもセッションは維持されること。
/// これが通ることで上のテストが「常に切れるだけ」でないと言える。
#[tokio::test]
async fn healthy_session_survives_several_ping_cycles() {
    let mock = MockServer::start().await;
    let (endpoint, _closed) = spawn_ws_server(BURST).await;
    let (state, mut rx) = setup(&mock, endpoint).await;

    let drain = tokio::spawn(async move {
        let mut seen = 0usize;
        while rx.recv().await.is_some() {
            seen += 1;
        }
        seen
    });

    let task = tokio::spawn(ws::run_connection(
        0,
        state.config.dmdata.ws_endpoints[0].clone(),
        state.event_tx.clone(),
        state.clone(),
    ));

    let connected = wait_until(Duration::from_secs(10), || {
        state.readiness.ws_connected[0].load(Ordering::Relaxed)
    })
    .await;
    assert!(connected, "ws must reach the connected state");

    // ping 数サイクル + pong timeout を十分に超える時間を経過させる
    tokio::time::sleep(Duration::from_secs(
        PING_INTERVAL_SECS * 3 + PONG_TIMEOUT_SECS,
    ))
    .await;
    assert!(
        state.readiness.ws_connected[0].load(Ordering::Relaxed),
        "a drained session must stay connected across ping cycles"
    );

    // 取りこぼしゼロ: 送った分がすべて下流へ届いていること
    task.abort();
    drop(state);
    let forwarded = tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("drain must finish")
        .expect("drain must not panic");
    assert_eq!(
        forwarded, BURST,
        "every event must reach the aggregator (no drops under backpressure)"
    );
}
