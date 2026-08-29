//! 起動シーケンス: 設定 → HTTPクライアント → 初期一覧 → Aggregator → WS×2 → Poller → HTTPサーバ。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use time::UtcOffset;
use time::macros::format_description;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::OffsetTime;

use jma_feed_gateway::config::Config;
use jma_feed_gateway::dmdata::api::DmdataApi;
use jma_feed_gateway::error::{AppError, ConfigError};
use jma_feed_gateway::state::AppState;
use jma_feed_gateway::supervisor::{RestartPolicy, Shutdown, spawn_critical, spawn_supervised};
use jma_feed_gateway::types::{DedupKey, Event};
use jma_feed_gateway::{aggregator, dmdata, fetcher, http, poller};

#[tokio::main]
async fn main() {
    let jst = UtcOffset::from_hms(9, 0, 0).expect("valid JST offset");
    let timer = OffsetTime::new(
        jst,
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"),
    );

    tracing_subscriber::fmt()
        .with_timer(timer)
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

    // warmup / ミス補充 / WS認可の全てがdmdataに依存するため、APIキーは必須
    let Some(api_key) = config.dmdata.api_key.as_ref() else {
        return Err(ConfigError::Invalid(
            "dmdata.api_key is required (set JMA_FEED_GATEWAY__DMDATA__API_KEY)".into(),
        )
        .into());
    };

    let client = reqwest::Client::builder()
        .user_agent(concat!("jma-feed-gateway/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(config.dmdata.fetch_timeout_secs))
        .build()?;

    // DMDATA APIクライアントは1個だけ構築し、warmup / fetch_entity / WS で共用する
    let dmdata_api = DmdataApi::new(
        client.clone(),
        config.dmdata.api_base.clone(),
        config.dmdata.data_api_base.clone(),
        api_key.expose(),
        config.dmdata.origin.clone(),
    );

    // 初期一覧取得(HTTP公開前に完了必須)
    let initial_metas = fetcher::load_initial_feed(&dmdata_api, &config).await?;

    let (event_tx, event_rx) = mpsc::channel::<Event>(1024);
    let state = Arc::new(AppState::new(config.clone(), dmdata_api, event_tx.clone()));

    // warmup済み電文IDをdedupへseed(spawn前に行い初回pollとの競合を排除)。
    // WS全断起動時に初回pollがリストページを丸ごと再fetch/再publishするのを防ぐ。
    // feed_ids(ミス時fetchのアローリスト)も同時にseedする
    for meta in &initial_metas {
        state.deduper.insert(DedupKey::TelegramId(meta.id.clone()));
        state.feed_ids.insert(meta.id.clone());
    }

    let shutdown = Shutdown::new();

    // Aggregator(唯一の書き込み点)。初期一覧を渡してスナップショット生成を任せる。
    spawn_critical(
        "aggregator".into(),
        shutdown.clone(),
        aggregator::run(initial_metas, event_rx, state.clone()),
    );
    state
        .readiness
        .initial_feed_loaded
        .store(true, Ordering::Relaxed);

    // DMDATA WebSocket ×2(tokyo/osaka)
    for (index, endpoint) in config.dmdata.ws_endpoints.iter().enumerate() {
        let endpoint = endpoint.clone();
        let tx = event_tx.clone();
        let task_state = state.clone();
        spawn_supervised(
            format!("dmdata-ws-{index}"),
            RestartPolicy::default(),
            shutdown.clone(),
            move || {
                dmdata::ws::run_connection(index, endpoint.clone(), tx.clone(), task_state.clone())
            },
        );
    }

    // 全WS切断中のフォールバックpolling(enabled=falseならタスク内で終了)
    {
        let task_state = state.clone();
        spawn_supervised(
            "poller".into(),
            RestartPolicy::default(),
            shutdown.clone(),
            move || poller::run(task_state.clone()),
        );
    }

    let app = http::build_router(state);
    let listener = tokio::net::TcpListener::bind(&config.http.bind_addr).await?;
    tracing::info!(addr = %config.http.bind_addr, "http server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown({
            let shutdown = shutdown.clone();
            async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("shutdown signal received");
                    }
                    _ = shutdown.wait() => {
                        tracing::error!("internal shutdown triggered; stopping http server");
                    }
                }
            }
        })
        .await?;

    if let Some(reason) = shutdown.reason() {
        return Err(AppError::CriticalTaskFailed(reason));
    }

    Ok(())
}
