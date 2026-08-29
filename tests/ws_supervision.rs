//! WSタスクのsupervision契約の統合テスト。
//!
//! 検証対象は「panicで死んだWSタスクを再起動したとき、readinessが前インスタンスの
//! 接続状態を引きずらない」こと。これが崩れると `ws_connected[i]` が `true` のまま
//! 固着し、`/readyz` が200を返し続け、pollerフォールバックも起動しない
//! (WSもpollも動いていないのに正常を主張する無音故障)。
//!
//! フラグを落とすのは `WsConnectionGuard` の Drop だけ、というのが現在の契約。
//! `run_connection` は冒頭で明示的なクリアをしない(それをするとコールドスタートが
//! 全断エピソード扱いになり、初回接続で不要なcatch-up pollが走る)。

use std::sync::Arc;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Format, Toml};
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use jma_feed_gateway::config::{Config, DEFAULT_CONFIG_TOML};
use jma_feed_gateway::dmdata::api::DmdataApi;
use jma_feed_gateway::dmdata::ws;
use jma_feed_gateway::state::{AppState, SharedState, WsConnectionGuard};
use jma_feed_gateway::supervisor::{RestartPolicy, Shutdown, TaskExit, spawn_supervised};
use jma_feed_gateway::types::Event;

/// socket_start が `delay` 後にエラーを返すモックへ向けた state を作る。
/// 遅延中は run_session が返らないため、readiness を観測する窓ができる。
async fn setup(server: &MockServer, delay: Duration) -> (SharedState, mpsc::Receiver<Event>) {
    Mock::given(method("POST"))
        .and(path("/socket"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_delay(delay)
                .set_body_json(serde_json::json!({"status": "error"})),
        )
        .mount(server)
        .await;

    let mut config: Config = Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load");
    config.dmdata.api_base = server.uri();
    config.dmdata.data_api_base = format!("{}/v1", server.uri());
    // 残存ソケット掃除は今回の検証に無関係なので黙らせる
    config.dmdata.cleanup_stale_sockets = false;

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
    (
        Arc::new(AppState::new(Arc::new(config), dmdata_api, tx)),
        rx,
    )
}

/// コールドスタートの初回接続を「全断エピソードからの復帰」と誤認しないこと。
///
/// `run_connection` が冒頭で `mark_ws_disconnected` を呼ぶと `fully_down` が立ち、
/// 最初の start で `ws_recovered` が通知されて不要なcatch-up pollが1回走る。
#[tokio::test]
async fn cold_start_does_not_signal_ws_recovered() {
    let server = MockServer::start().await;
    // socket_start の遅延中 = まだ一度も接続していない窓
    let (state, _rx) = setup(&server, Duration::from_secs(30)).await;

    let task_state = state.clone();
    let handle = tokio::spawn(async move {
        ws::run_connection(
            0,
            "ws://127.0.0.1:1".into(),
            task_state.event_tx.clone(),
            task_state,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 初回の start 相当。エピソードが無いので通知されてはならない
    state.readiness.mark_ws_connected(0);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            state.readiness.ws_recovered.notified(),
        )
        .await
        .is_err(),
        "コールドスタートの初回接続で ws_recovered を通知してはならない"
    );

    handle.abort();
}

#[tokio::test]
async fn ws_task_exits_as_done_when_event_channel_is_closed() {
    let server = MockServer::start().await;
    let (state, rx) = setup(&server, Duration::from_millis(10)).await;

    // aggregatorの死を模擬: 受信側をdropするとチャネルが閉じる
    drop(rx);

    let tx = state.event_tx.clone();
    let exit = tokio::time::timeout(
        Duration::from_secs(10),
        ws::run_connection(0, "ws://127.0.0.1:1".into(), tx, state.clone()),
    )
    .await
    .expect("チャネルが閉じていれば即座に終了すること");

    // 再起動しても送り先がないため Done(supervisorは再起動しない)
    assert!(matches!(exit, TaskExit::Done), "got {exit:?}");
    assert!(state.readiness.all_ws_down());
}

#[tokio::test]
async fn supervisor_does_not_restart_ws_task_that_returned_done() {
    let server = MockServer::start().await;
    let (state, rx) = setup(&server, Duration::from_millis(10)).await;
    drop(rx);

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = calls.clone();
    let task_state = state.clone();

    let handle = spawn_supervised(
        "dmdata-ws-0".into(),
        RestartPolicy {
            initial: Duration::from_millis(1),
            max: Duration::from_millis(5),
            multiplier: 2.0,
        },
        Shutdown::new(),
        move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let state = task_state.clone();
            let tx = state.event_tx.clone();
            async move { ws::run_connection(0, "ws://127.0.0.1:1".into(), tx, state).await }
        },
    );

    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("supervisorはDoneで終了すること")
        .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
}

/// panic した瞬間に `ws_connected` が落ちること — supervisor のバックオフを待たない。
///
/// `initial` を60秒に取っているのが要点: 再起動したタスクの後片付けが
/// 観測窓に紛れ込む可能性を排除し、フラグを落としたのが
/// `WsConnectionGuard` の Drop(panic unwind)だけだと確定させる。
#[tokio::test]
async fn ws_flag_is_cleared_on_panic_without_waiting_for_backoff() {
    let server = MockServer::start().await;
    let (state, _rx) = setup(&server, Duration::from_millis(10)).await;

    let task_state = state.clone();
    let handle = spawn_supervised(
        "dmdata-ws-panicky".into(),
        RestartPolicy {
            // 再起動は事実上起きない。観測窓で落ちていればガードの効果
            initial: Duration::from_secs(60),
            max: Duration::from_secs(60),
            multiplier: 2.0,
        },
        Shutdown::new(),
        move || {
            let state = task_state.clone();
            async move {
                let _guard = WsConnectionGuard::new(state.clone(), 0);
                state.readiness.mark_ws_connected(0);
                panic!("injected panic while ws is connected");
            }
        },
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        state.readiness.all_ws_down(),
        "panic unwind の時点で ws_connected は落ちていること(backoffを待たない)"
    );
    assert!(
        !state.readiness.is_ready(),
        "WSもpollも動いていない状態を readiness が正しく報告すること"
    );

    handle.abort();
}
