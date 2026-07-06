//! jma-feed-gateway: 気象庁XML(地震火山系)キャッシュ・高速配信サーバ。
//!
//! 設計原則: 更新時のみXML生成し、HTTPは完成済みBytesを返却する。
//!
//! # モジュール
//! - [`config`][]: TOML + 環境変数(`JMA_FEED_GATEWAY__` プレフィクス)設定
//! - [`jma`][]: フィードのパース/生成、ID導出(純粋関数)
//! - [`http`][]: axumルーター・ハンドラ(読み取り専用)
//! - [`fetcher`][]: 初期一覧取得とキャッシュミス時のsingleflight実体取得
//! - [`dmdata`][]: DMDATA.JP REST API / WebSocket連携
//! - [`poller`][]: 全WS切断中のJMAフィードpollingフォールバック
//! - [`aggregator`][]: 唯一の書き込み点となる単一タスク

pub mod aggregator;
pub mod config;
pub mod dmdata;
pub mod error;
pub mod fetcher;
pub mod http;
pub mod jma;
pub mod poller;
pub mod state;
pub mod types;
