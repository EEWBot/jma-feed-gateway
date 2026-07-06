//! 起動シーケンス: 設定 → HTTPクライアント → 初期一覧 → Aggregator → WS×2 → Poller → HTTPサーバ。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use jma_relay::config::Config;
use jma_relay::error::AppError;
use jma_relay::state::AppState;
use jma_relay::types::Event;
use jma_relay::{aggregator, dmdata, fetcher, http, poller};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if let Err(e) = run().await {
        tracing::error!(error = %e, "fatal error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), AppError> {
    let config = Arc::new(Config::load()?);
    tracing::info!(bind_addr = %config.http.bind_addr, "configuration loaded");

    let client = reqwest::Client::builder()
        .user_agent(concat!("jma-relay/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(config.jma.fetch_timeout_secs))
        .build()?;

    // 初期一覧取得(HTTP公開前に完了必須)
    let initial_metas = fetcher::load_initial_feed(&client, &config).await?;

    let (event_tx, event_rx) = mpsc::channel::<Event>(1024);
    let state = Arc::new(AppState::new(config.clone(), client, event_tx.clone()));

    // Aggregator(唯一の書き込み点)。初期一覧を渡してスナップショット生成を任せる
    tokio::spawn(aggregator::run(initial_metas, event_rx, state.clone()));
    state
        .readiness
        .initial_feed_loaded
        .store(true, Ordering::Relaxed);

    // DMDATA WebSocket ×2(tokyo/osaka)。api_key未設定の場合はタスク内で無効化される
    for (index, endpoint) in config.dmdata.ws_endpoints.iter().enumerate() {
        tokio::spawn(dmdata::ws::run_connection(
            index,
            endpoint.clone(),
            event_tx.clone(),
            state.clone(),
        ));
    }

    // 全WS切断中のフォールバックpolling(enabled=falseならタスク内で終了)
    tokio::spawn(poller::run(state.clone()));

    let app = http::build_router(state);
    let listener = tokio::net::TcpListener::bind(&config.http.bind_addr).await?;
    tracing::info!(addr = %config.http.bind_addr, "http server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await?;

    Ok(())
}
