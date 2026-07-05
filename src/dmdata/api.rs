//! DMDATA REST API クライアント(socket_start / socket_list / socket_close)。
//! 参照: docs/gateway/DmdataAPI.java

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::error::DmdataError;

const USER_AGENT: &str = concat!("jma-relay/", env!("CARGO_PKG_VERSION"));

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

pub struct DmdataApi {
    client: reqwest::Client,
    api_base: String,
    /// `Basic base64(api_key + ":")`(末尾コロン必須)。
    auth_header: String,
    origin: Option<String>,
}

impl DmdataApi {
    pub fn new(
        client: reqwest::Client,
        api_base: impl Into<String>,
        api_key: &str,
        origin: Option<String>,
    ) -> Self {
        let auth_header = format!("Basic {}", BASE64.encode(format!("{api_key}:")));
        Self {
            client,
            api_base: api_base.into().trim_end_matches('/').to_string(),
            auth_header,
            origin,
        }
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let mut builder = self
            .client
            .request(method, format!("{}{}", self.api_base, path))
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .header("User-Agent", USER_AGENT);
        if let Some(origin) = &self.origin {
            builder = builder.header("Origin", origin);
        }
        builder
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
        response.json::<SocketList>().await.map_err(DmdataError::Http)
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
                "appName": "jma-relay-1",
                "formatMode": "raw"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "responseId": "r1",
                "status": "ok",
                "ticket": "TICKET123",
                "websocket": {"id": 555, "url": "wss://ws-tokyo.api.dmdata.jp/v2/websocket", "protocol": ["dmdata.v2"], "expiration": 300},
                "classifications": ["telegram.earthquake"],
                "appName": "jma-relay-1"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let request = SocketStartRequest::new(
            vec!["telegram.earthquake".into()],
            None,
            "jma-relay-1".into(),
        );
        let response = api(&server).socket_start(&request).await.expect("must succeed");
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
                    {"id": 101, "appName": "jma-relay-1", "status": "open", "server": "ws-tokyo"},
                    {"id": 102, "appName": "other-app", "status": "open"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let list = api(&server).socket_list_open().await.expect("must succeed");
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].id, 101);
        assert_eq!(list.items[0].app_name.as_deref(), Some("jma-relay-1"));
    }

    #[tokio::test]
    async fn socket_close_tolerates_404() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/socket/101"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"})))
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
}
