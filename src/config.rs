//! 設定型と figment による TOML + 環境変数読み込み。

use std::fmt;

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::Deserialize;

use crate::error::ConfigError;

/// 埋め込みデフォルト設定(`config/default.toml` と同一内容)。
pub const DEFAULT_CONFIG_TOML: &str = include_str!("../config/default.toml");

/// 環境変数プレフィクス。`JMA_RELAY__HTTP__BIND_ADDR` のように `__` 区切りでネストする。
pub const ENV_PREFIX: &str = "JMA_RELAY__";

/// Debug 出力で中身を秘匿する秘密文字列(APIキー等)。
#[derive(Clone, Deserialize)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub http: HttpConfig,
    pub jma: JmaConfig,
    pub dmdata: DmdataConfig,
    pub cache: CacheConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    /// HTTPサーバのバインドアドレス。
    pub bind_addr: String,
    /// 生成Atomフィードの entry id / link に使う自サーバの公開ベースURL。
    pub public_base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JmaConfig {
    /// 初期一覧のAtomフィードURL。
    pub feed_url: String,
    /// 長期フィード(eqvol_l.xml)のURL。ウォームアップ時のバックフィルに使う。
    pub long_feed_url: String,
    /// ウォームアップ時に採用する電文種別コード。空なら全通過。
    #[serde(default)]
    pub telegram_types: Vec<String>,
    /// 実体XMLのベースURL(キャッシュミス時の307先)。
    pub data_base_url: String,
    pub fetch_timeout_secs: u64,
    pub retry_attempts: u32,
    pub retry_initial_backoff_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DmdataConfig {
    /// DMDATA APIキー。環境変数 `JMA_RELAY__DMDATA__API_KEY` で設定する。
    pub api_key: Option<Secret>,
    pub api_base: String,
    /// WebSocketエンドポイント(最大2系統)。
    pub ws_endpoints: Vec<String>,
    pub classifications: Vec<String>,
    /// 電文type絞り込み。空なら全type。
    #[serde(default)]
    pub types: Vec<String>,
    pub app_name: String,
    /// Originヘッダ(任意)。
    pub origin: Option<String>,
    pub cleanup_stale_sockets: bool,
    pub reconnect: ReconnectConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReconnectConfig {
    pub initial_secs: u64,
    pub max_secs: u64,
    pub multiplier: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    pub feed_entries: usize,
    pub entity_capacity: u64,
    pub entity_ttl_secs: u64,
    pub seen_ttl_secs: u64,
}

impl Config {
    /// 埋め込みデフォルト → `config/default.toml`(あれば)→ 環境変数、の順で読み込む。
    pub fn load() -> Result<Self, ConfigError> {
        Self::from_figment(Self::figment())
    }

    /// デフォルトの Figment 構成。
    pub fn figment() -> Figment {
        Figment::from(Toml::string(DEFAULT_CONFIG_TOML))
            .merge(Toml::file("config/default.toml"))
            .merge(Env::prefixed(ENV_PREFIX).split("__"))
    }

    /// 任意の Figment から読み込み・検証する(テスト用にも公開)。
    pub fn from_figment(figment: Figment) -> Result<Self, ConfigError> {
        let config: Config = figment.extract()?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.http.bind_addr.is_empty() {
            return Err(ConfigError::Invalid(
                "http.bind_addr must not be empty".into(),
            ));
        }
        if self.jma.feed_url.is_empty() {
            return Err(ConfigError::Invalid(
                "jma.feed_url must not be empty".into(),
            ));
        }
        if self.jma.long_feed_url.is_empty() {
            return Err(ConfigError::Invalid(
                "jma.long_feed_url must not be empty".into(),
            ));
        }
        if self.jma.data_base_url.is_empty() {
            return Err(ConfigError::Invalid(
                "jma.data_base_url must not be empty".into(),
            ));
        }
        if self.dmdata.ws_endpoints.is_empty() || self.dmdata.ws_endpoints.len() > 2 {
            return Err(ConfigError::Invalid(
                "dmdata.ws_endpoints must contain 1 or 2 endpoints".into(),
            ));
        }
        if self.cache.feed_entries == 0 {
            return Err(ConfigError::Invalid(
                "cache.feed_entries must be > 0".into(),
            ));
        }
        if self.cache.entity_capacity == 0 {
            return Err(ConfigError::Invalid(
                "cache.entity_capacity must be > 0".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
// figment::Jail のクロージャは figment::Error を返す規約のため大きなErr型を許容する
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    #[test]
    fn defaults_load() {
        figment::Jail::expect_with(|_jail| {
            let config = Config::from_figment(
                Figment::from(Toml::string(DEFAULT_CONFIG_TOML))
                    .merge(Env::prefixed(ENV_PREFIX).split("__")),
            )
            .expect("default config must load");
            assert_eq!(config.http.bind_addr, "127.0.0.1:8080");
            assert_eq!(
                config.jma.long_feed_url,
                "https://www.data.jma.go.jp/developer/xml/feed/eqvol_l.xml"
            );
            assert_eq!(config.jma.telegram_types.len(), 15);
            assert!(config.jma.telegram_types.iter().any(|t| t == "VXSE53"));
            assert_eq!(config.dmdata.types, config.jma.telegram_types);
            assert_eq!(config.dmdata.ws_endpoints.len(), 2);
            assert_eq!(config.dmdata.classifications, vec!["telegram.earthquake"]);
            assert!(config.dmdata.api_key.is_none());
            Ok(())
        });
    }

    #[test]
    fn toml_file_overrides_defaults() {
        figment::Jail::expect_with(|jail| {
            jail.create_dir("config")?;
            jail.create_file(
                "config/default.toml",
                r#"
                [http]
                bind_addr = "0.0.0.0:9999"
                "#,
            )?;
            let config = Config::from_figment(Config::figment()).expect("config must load");
            assert_eq!(config.http.bind_addr, "0.0.0.0:9999");
            // 上書きしていない値はデフォルトのまま
            assert_eq!(config.cache.feed_entries, 100);
            Ok(())
        });
    }

    #[test]
    fn env_overrides_toml() {
        figment::Jail::expect_with(|jail| {
            jail.create_dir("config")?;
            jail.create_file(
                "config/default.toml",
                r#"
                [http]
                bind_addr = "0.0.0.0:9999"
                "#,
            )?;
            jail.set_env("JMA_RELAY__HTTP__BIND_ADDR", "127.0.0.1:7777");
            jail.set_env("JMA_RELAY__DMDATA__API_KEY", "test-key-123");
            jail.set_env("JMA_RELAY__CACHE__FEED_ENTRIES", "42");
            let config = Config::from_figment(Config::figment()).expect("config must load");
            assert_eq!(config.http.bind_addr, "127.0.0.1:7777");
            assert_eq!(config.cache.feed_entries, 42);
            let key = config.dmdata.api_key.as_ref().expect("api_key set via env");
            assert_eq!(key.expose(), "test-key-123");
            // Debug出力に秘密が漏れないこと
            let debug = format!("{:?}", config);
            assert!(!debug.contains("test-key-123"));
            Ok(())
        });
    }

    #[test]
    fn invalid_ws_endpoints_rejected() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("JMA_RELAY__DMDATA__WS_ENDPOINTS", r#"["a", "b", "c"]"#);
            let result = Config::from_figment(
                Figment::from(Toml::string(DEFAULT_CONFIG_TOML))
                    .merge(Env::prefixed(ENV_PREFIX).split("__")),
            );
            assert!(matches!(result, Err(ConfigError::Invalid(_))));
            Ok(())
        });
    }
}
