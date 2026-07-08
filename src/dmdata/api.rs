//! DMDATA REST API クライアント(socket_start / socket_list / socket_close /
//! telegram_list / telegram_get)。
//! 参照: docs/gateway/DmdataAPI.java

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::dmdata::protocol::{DataHead, XmlReport};
use crate::error::DmdataError;

const USER_AGENT: &str = concat!("jma-feed-gateway/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketStartRequest {
    pub classifications: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<String>>,
    /// "no" 固定(テスト電文を受信しない)。
    pub test: String,
    pub app_name: String,
    /// "raw" 固定(XML実体をそのまま受け取る)。
    pub format_mode: String,
}

impl SocketStartRequest {
    pub fn new(classifications: Vec<String>, types: Option<Vec<String>>, app_name: String) -> Self {
        Self {
            classifications,
            types,
            test: "no".into(),
            app_name,
            format_mode: "raw".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketStartResponse {
    /// 使い捨てチケット。再接続ごとに socket_start で取り直すこと。
    pub ticket: String,
    #[serde(default)]
    pub websocket: Option<WebSocketInfo>,
    #[serde(default)]
    pub classifications: Vec<String>,
    #[serde(default)]
    pub types: Option<Vec<String>>,
    #[serde(default)]
    pub app_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketInfo {
    pub id: i64,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub protocol: Vec<String>,
    #[serde(default)]
    pub expiration: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketList {
    #[serde(default)]
    pub items: Vec<SocketListItem>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketListItem {
    pub id: i64,
    #[serde(default)]
    pub app_name: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub server: Option<String>,
}

/// GET /telegram(telegram.list)のレスポンス。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelegramListResponse {
    #[serde(default)]
    pub items: Vec<TelegramListItem>,
    /// ページネーション: 次回の `cursorToken` に渡す。
    #[serde(default)]
    pub next_token: Option<String>,
}

/// telegram.list の1アイテム。`head` / `xmlReport` はWS dataメッセージと同形。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelegramListItem {
    /// DMDATA電文一意ID(384bitハッシュ)。
    pub id: String,
    #[serde(default)]
    pub serial: Option<i64>,
    #[serde(default)]
    pub classification: Option<String>,
    pub head: DataHead,
    #[serde(default)]
    pub received_time: Option<String>,
    /// `xmlReport=true` 指定時のみ付加されるControl/Head相当のメタ情報。
    #[serde(default)]
    pub xml_report: Option<XmlReport>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiErrorResponse {
    #[serde(default)]
    error: Option<ApiErrorDetail>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiErrorDetail {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    code: Option<i64>,
}

#[derive(Clone)]
pub struct DmdataApi {
    client: reqwest::Client,
    api_base: String,
    /// telegram.data v1 のベースURL(api_baseとホストが異なる)。
    data_api_base: String,
    /// `Basic base64(api_key + ":")`(末尾コロン必須)。
    auth_header: String,
    origin: Option<String>,
}

impl DmdataApi {
    pub fn new(
        client: reqwest::Client,
        api_base: impl Into<String>,
        data_api_base: impl Into<String>,
        api_key: &str,
        origin: Option<String>,
    ) -> Self {
        let auth_header = format!("Basic {}", BASE64.encode(format!("{api_key}:")));
        Self {
            client,
            api_base: api_base.into().trim_end_matches('/').to_string(),
            data_api_base: data_api_base.into().trim_end_matches('/').to_string(),
            auth_header,
            origin,
        }
    }

    /// 任意URLへの認証付きリクエストビルダ(api_base / data_api_base 共通)。
    fn request_url(&self, method: Method, url: String) -> reqwest::RequestBuilder {
        let mut builder = self
            .client
            .request(method, url)
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .header("User-Agent", USER_AGENT);
        if let Some(origin) = &self.origin {
            builder = builder.header("Origin", origin);
        }
        builder
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.request_url(method, format!("{}{}", self.api_base, path))
    }

    async fn api_error(response: reqwest::Response) -> DmdataError {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let detail = serde_json::from_str::<ApiErrorResponse>(&body)
            .ok()
            .and_then(|e| e.error)
            .map(|e| {
                format!(
                    "code={} message={}",
                    e.code.unwrap_or_default(),
                    e.message.unwrap_or_default()
                )
            })
            .unwrap_or_else(|| body.chars().take(200).collect());
        DmdataError::Api(format!("http {status}: {detail}"))
    }

    /// POST /socket — WebSocket認可。レスポンスの ticket は使い捨て。
    pub async fn socket_start(
        &self,
        request: &SocketStartRequest,
    ) -> Result<SocketStartResponse, DmdataError> {
        let response = self
            .request(Method::POST, "/socket")
            .json(request)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(Self::api_error(response).await);
        }
        response
            .json::<SocketStartResponse>()
            .await
            .map_err(DmdataError::Http)
    }

    /// GET /socket?status=open — 開いているソケット一覧。
    pub async fn socket_list_open(&self) -> Result<SocketList, DmdataError> {
        let response = self
            .request(Method::GET, "/socket?status=open")
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(Self::api_error(response).await);
        }
        response
            .json::<SocketList>()
            .await
            .map_err(DmdataError::Http)
    }

    /// DELETE /socket/{id} — ソケット切断。404(既に閉じている)は成功扱い。
    pub async fn socket_close(&self, socket_id: i64) -> Result<(), DmdataError> {
        let response = self
            .request(Method::DELETE, &format!("/socket/{socket_id}"))
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => {
                tracing::debug!(socket_id, "dmdata socket already closed");
                Ok(())
            }
            _ => Err(Self::api_error(response).await),
        }
    }

    /// GET /telegram(telegram.list)— 電文リスト1ページ取得。
    /// `xmlReport=true`(Control/Head相当を付加)・`test=no` 固定。
    /// `cursor_token` に前ページの `next_token` を渡すと続きを取得する。
    pub async fn telegram_list(
        &self,
        classification: &str,
        cursor_token: Option<&str>,
        limit: usize,
    ) -> Result<TelegramListResponse, DmdataError> {
        let mut builder = self.request(Method::GET, "/telegram").query(&[
            ("classification", classification),
            ("xmlReport", "true"),
            ("test", "no"),
            ("limit", &limit.to_string()),
        ]);
        if let Some(token) = cursor_token {
            builder = builder.query(&[("cursorToken", token)]);
        }
        let response = builder.send().await?;
        if response.status() != StatusCode::OK {
            return Err(Self::api_error(response).await);
        }
        response
            .json::<TelegramListResponse>()
            .await
            .map_err(DmdataError::Http)
    }

    /// GET {data_api_base}/{id}(telegram.data v1)— 電文本文の取得。
    /// gzip配信はreqwestのgzip機能で透過解凍される。
    pub async fn telegram_get(&self, id: &str) -> Result<Bytes, DmdataError> {
        let response = self
            .request_url(Method::GET, format!("{}/{id}", self.data_api_base))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }
        response.bytes().await.map_err(DmdataError::Http)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn api(server: &MockServer) -> DmdataApi {
        DmdataApi::new(
            reqwest::Client::new(),
            server.uri(),
            format!("{}/v1", server.uri()),
            "test-api-key",
            Some("https://example.com".into()),
        )
    }

    /// base64("test-api-key:") — 末尾コロン込み。
    const EXPECTED_AUTH: &str = "Basic dGVzdC1hcGkta2V5Og==";

    #[tokio::test]
    async fn socket_start_sends_auth_and_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/socket"))
            .and(header("Authorization", EXPECTED_AUTH))
            .and(header("Content-Type", "application/json"))
            .and(header("Origin", "https://example.com"))
            .and(body_partial_json(serde_json::json!({
                "classifications": ["telegram.earthquake"],
                "test": "no",
                "appName": "jma-feed-gateway-1",
                "formatMode": "raw"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "responseId": "r1",
                "status": "ok",
                "ticket": "TICKET123",
                "websocket": {"id": 555, "url": "wss://ws-tokyo.api.dmdata.jp/v2/websocket", "protocol": ["dmdata.v2"], "expiration": 300},
                "classifications": ["telegram.earthquake"],
                "appName": "jma-feed-gateway-1"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let request = SocketStartRequest::new(
            vec!["telegram.earthquake".into()],
            None,
            "jma-feed-gateway-1".into(),
        );
        let response = api(&server)
            .socket_start(&request)
            .await
            .expect("must succeed");
        assert_eq!(response.ticket, "TICKET123");
        assert_eq!(response.websocket.unwrap().id, 555);
    }

    #[tokio::test]
    async fn socket_start_maps_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/socket"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "responseId": "r2",
                "status": "error",
                "error": {"message": "unauthorized", "code": 401}
            })))
            .mount(&server)
            .await;

        let request =
            SocketStartRequest::new(vec!["telegram.earthquake".into()], None, "app".into());
        let error = api(&server).socket_start(&request).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("401"), "unexpected: {message}");
        assert!(message.contains("unauthorized"), "unexpected: {message}");
    }

    #[tokio::test]
    async fn socket_list_queries_open_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/socket"))
            .and(query_param("status", "open"))
            .and(header("Authorization", EXPECTED_AUTH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "responseId": "r3",
                "status": "ok",
                "items": [
                    {"id": 101, "appName": "jma-feed-gateway-1", "status": "open", "server": "ws-tokyo"},
                    {"id": 102, "appName": "other-app", "status": "open"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let list = api(&server).socket_list_open().await.expect("must succeed");
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].id, 101);
        assert_eq!(
            list.items[0].app_name.as_deref(),
            Some("jma-feed-gateway-1")
        );
    }

    #[tokio::test]
    async fn socket_close_tolerates_404() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/socket/101"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/socket/999"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "status": "error",
                "error": {"message": "not found", "code": 404}
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/socket/500"))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "status": "error",
                "error": {"message": "internal", "code": 500}
            })))
            .mount(&server)
            .await;

        let api = api(&server);
        api.socket_close(101).await.expect("200 is ok");
        api.socket_close(999).await.expect("404 is tolerated");
        assert!(api.socket_close(500).await.is_err());
    }

    const TELEGRAM_LIST_JSON: &str = include_str!("../../tests/fixtures/telegram_list.json");

    #[tokio::test]
    async fn telegram_list_sends_auth_and_query_and_parses() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .and(header("Authorization", EXPECTED_AUTH))
            .and(query_param("classification", "telegram.earthquake"))
            .and(query_param("xmlReport", "true"))
            .and(query_param("test", "no"))
            .and(query_param("limit", "100"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(TELEGRAM_LIST_JSON, "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let list = api(&server)
            .telegram_list("telegram.earthquake", None, 100)
            .await
            .expect("must succeed");
        assert_eq!(list.items.len(), 3);
        assert_eq!(list.next_token.as_deref(), Some("NEXT_TOKEN_1"));

        let item = &list.items[0];
        assert_eq!(item.id.len(), 96, "id must be a 384bit hex hash");
        assert_eq!(item.head.telegram_type, "VXSE53");
        assert!(!item.head.test);
        assert_eq!(item.format.as_deref(), Some("xml"));
        assert_eq!(
            item.received_time.as_deref(),
            Some("2026-07-08T10:00:05+09:00")
        );
        let report = item.xml_report.as_ref().expect("xmlReport");
        assert_eq!(
            report.control.as_ref().unwrap().title.as_deref(),
            Some("震源・震度情報")
        );
        assert_eq!(
            report.head.as_ref().unwrap().report_date_time.as_deref(),
            Some("2026-07-08T10:00:00+09:00")
        );
        // 3件目はテスト電文(head.test=true)がそのままパースされること
        assert!(list.items[2].head.test);
    }

    #[tokio::test]
    async fn telegram_list_includes_cursor_token_when_set() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .and(query_param("cursorToken", "CURSOR_1"))
            .and(query_param("limit", "50"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "responseId": "r10",
                "status": "ok",
                "items": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let list = api(&server)
            .telegram_list("telegram.earthquake", Some("CURSOR_1"), 50)
            .await
            .expect("must succeed");
        assert!(list.items.is_empty());
        assert!(list.next_token.is_none());
    }

    #[tokio::test]
    async fn telegram_list_maps_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "responseId": "r11",
                "status": "error",
                "error": {"message": "forbidden", "code": 403}
            })))
            .mount(&server)
            .await;

        let error = api(&server)
            .telegram_list("telegram.earthquake", None, 100)
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("403"), "unexpected: {message}");
        assert!(message.contains("forbidden"), "unexpected: {message}");
    }

    #[tokio::test]
    async fn telegram_get_returns_bytes_with_auth() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/TELEGRAM_ID_1"))
            .and(header("Authorization", EXPECTED_AUTH))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw("<Report>body</Report>", "application/xml"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let body = api(&server)
            .telegram_get("TELEGRAM_ID_1")
            .await
            .expect("must succeed");
        assert_eq!(body.as_ref(), b"<Report>body</Report>");
    }

    #[tokio::test]
    async fn telegram_get_maps_non_2xx_to_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/MISSING_ID"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "responseId": "r12",
                "status": "error",
                "error": {"message": "not found", "code": 404}
            })))
            .mount(&server)
            .await;

        let error = api(&server).telegram_get("MISSING_ID").await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("404"), "unexpected: {message}");
        assert!(message.contains("not found"), "unexpected: {message}");
    }
}
