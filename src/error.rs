//! アプリケーション共通のエラー型。

use thiserror::Error;

/// 設定読み込み・検証エラー。
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Figment(#[from] Box<figment::Error>),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        ConfigError::Figment(Box::new(e))
    }
}

/// 上流(JMA)へのHTTPアクセスエラー。
#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("upstream http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("upstream returned unexpected status: {0}")]
    Status(reqwest::StatusCode),
    #[error("failed to parse upstream feed: {0}")]
    Parse(String),
}

/// DMDATA API / WebSocket エラー。
/// NOTE(phase2): dmdata モジュール実装時にバリアントを拡充する。
#[derive(Debug, Error)]
pub enum DmdataError {
    #[error("dmdata http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("dmdata api error: {0}")]
    Api(String),
    #[error("dmdata websocket error: {0}")]
    Ws(String),
    #[error("failed to decode telegram body: {0}")]
    Body(String),
}

/// アプリ全体の起動時エラー。
#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
    #[error(transparent)]
    Dmdata(#[from] DmdataError),
    #[error("http client error: {0}")]
    Client(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
