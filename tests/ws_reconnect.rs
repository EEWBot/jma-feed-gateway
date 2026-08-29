//! 再接続バックオフのreset条件の回帰テスト。
//!
//! 検証対象は「startを受信した(=購読が確立した)セッションの後は、その終わり方が
//! エラーであってもバックオフが初期値に戻ること」。
//!
//! 修正前は `run_session` が `Ok(true)` を返した場合(サーバからのclose等)しか
//! resetしていなかった。実運用で多い receive error / watchdog timeout は `Err` に
//! なるため、正常運用が続いた後に切れるたびにバックオフが 1→2→4→…→60秒 と
//! 蓄積し、健全なサーバに対しても最大60秒の再接続遅延が残っていた。

use std::sync::Arc;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Format, Toml};
use futures_util::SinkExt;
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

const START_JSON: &str = include_str!("fixtures/ws_start.json");

/// バックオフ初期値。`initial_secs` は実装側で `max(1)` されるため1秒が下限。
const INITIAL_SECS: u64 = 1;
/// resetされた場合(1〜2秒)とされない場合(8〜9秒)を明確に分離するための倍率。
const MULTIPLIER: f64 = 8.0;

/// ダミーWSサーバ。接続を順に受け付け、毎回 `start` を送った直後にTCPを即断して
/// クライアント側の `run_session` を **`Err`(receive failed)** で終わらせる。
/// 各acceptの時刻を呼び出し側へ流す。
async fn spawn_flapping_ws_server() -> (String, mpsc::UnboundedReceiver<tokio::time::Instant>) {
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
            let Ok(mut ws) = tokio_tungstenite::accept_async(socket).await else {
                continue;
            };
            if ws.send(Message::text(START_JSON)).await.is_err() {
                continue;
            }
            // クライアントがstartを処理する猶予を与えてから、closeフレーム無しで切断する
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(ws);
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
    config.dmdata.reconnect.initial_secs = INITIAL_SECS;
    config.dmdata.reconnect.multiplier = MULTIPLIER;

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

/// start受信後にエラーで終わったセッションでもバックオフがresetされること。
#[tokio::test]
async fn backoff_resets_after_a_session_that_reached_connected() {
    let mock = MockServer::start().await;
    let (endpoint, mut accepted) = spawn_flapping_ws_server().await;
    let (state, _rx) = setup(&mock, endpoint).await;

    let task = tokio::spawn(ws::run_connection(
        0,
        state.config.dmdata.ws_endpoints[0].clone(),
        state.event_tx.clone(),
        state.clone(),
    ));

    let mut next_accept = async || {
        tokio::time::timeout(Duration::from_secs(30), accepted.recv())
            .await
            .expect("server must accept another connection")
            .expect("server task must stay alive")
    };
    // 1本目は起動直後。2本目→3本目の間隔が「2回目の再接続待ち」にあたる。
    next_accept().await;
    let second = next_accept().await;
    let third = next_accept().await;

    let gap = third - second;
    // reset有り: initial(1秒)+ jitter(<1秒) → 2秒未満
    // reset無し: initial*8(8秒)+ jitter → 8秒以上
    assert!(
        gap < Duration::from_secs(4),
        "backoff must be reset after a connected session, but the reconnect gap was {gap:?}"
    );

    task.abort();
}
