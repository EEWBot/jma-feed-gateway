//! axum統合テスト(tower::ServiceExt::oneshot、ネットワーク不要。
//! singleflightテストのみ wiremock でローカル上流を模擬)。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use figment::Figment;
use figment::providers::{Format, Toml};
use http_body_util::BodyExt;
use tower::ServiceExt;

use jma_relay::config::{Config, DEFAULT_CONFIG_TOML};
use jma_relay::http::build_router;
use jma_relay::state::{AppState, SharedState};
use jma_relay::types::{EntityEntry, Event, FeedSnapshot, ItemMeta};
use tokio::sync::mpsc;

const FEED_PATH: &str = "/developer/xml/feed/eqvol.xml";
const UUID_A: &str = "ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b";
const UUID_MISS: &str = "0af03cd5-25a9-3ba5-b73b-c9b7ce0f8a55";
const JMA_MISS: &str = "20260705050045_0_VXSE99_010000";
/// setup() のフィードに設定する Last-Modified(2026-07-05T04:10:12+09:00 のUTC)。
const FEED_LAST_MODIFIED: &str = "Sat, 04 Jul 2026 19:10:12 GMT";

fn test_config() -> Config {
    Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load")
}

fn make_state(
    mut config: Config,
    mutate: impl FnOnce(&mut Config),
) -> (SharedState, mpsc::Receiver<Event>) {
    mutate(&mut config);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel::<Event>(64);
    (Arc::new(AppState::new(Arc::new(config), client, tx)), rx)
}

async fn setup() -> (SharedState, Router) {
    let (state, rx) = make_state(test_config(), |_| {});
    // これらのテストではaggregatorを起動しない。チャネルを開いたままにする
    std::mem::forget(rx);
    // フィードスナップショットを設定
    let body = Bytes::from_static(b"<?xml version=\"1.0\"?><feed>test</feed>");
    let last_modified = time::OffsetDateTime::parse(
        "2026-07-05T04:10:12+09:00",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    state.feed.store(Arc::new(FeedSnapshot::new(
        body,
        "2026-07-05T04:10:12+09:00".into(),
        Some(last_modified),
    )));
    // 実体キャッシュに1件投入
    let entry = EntityEntry::new(
        Bytes::from_static(b"<Report>cached entity</Report>"),
        ItemMeta {
            id: UUID_A.into(),
            ..ItemMeta::default()
        },
    );
    state.entities.insert(UUID_A.into(), Arc::new(entry)).await;
    let router = build_router(state.clone());
    (state, router)
}

async fn get(router: &Router, uri: &str, if_none_match: Option<&str>) -> axum::response::Response {
    let mut builder = Request::builder().uri(uri);
    if let Some(inm) = if_none_match {
        builder = builder.header(header::IF_NONE_MATCH, inm);
    }
    router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// 任意ヘッダ付きGET(If-Modified-Since テスト用)。
async fn get_with_headers(
    router: &Router,
    uri: &str,
    extra: &[(header::HeaderName, &str)],
) -> axum::response::Response {
    let mut builder = Request::builder().uri(uri);
    for (name, value) in extra {
        builder = builder.header(name, *value);
    }
    router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn body_bytes(response: axum::response::Response) -> Bytes {
    response.into_body().collect().await.unwrap().to_bytes()
}

/// レスポンスヘッダの値を文字列で取り出す。
fn header_str<'a>(response: &'a axum::response::Response, name: &str) -> Option<&'a str> {
    response.headers().get(name).and_then(|v| v.to_str().ok())
}

#[tokio::test]
async fn feed_200_then_304() {
    let (_state, router) = setup().await;

    let response = get(&router, FEED_PATH, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response
        .headers()
        .get(header::ETAG)
        .expect("ETag must be present")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(etag.starts_with('"'));
    let content_type = response.headers().get(header::CONTENT_TYPE).unwrap();
    assert!(content_type.to_str().unwrap().contains("atom+xml"));
    let body = body_bytes(response).await;
    assert!(!body.is_empty());

    // If-None-Match 一致 → 304、body無し、ETag再送
    let response = get(&router, FEED_PATH, Some(&etag)).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap(),
        etag
    );
    let body = body_bytes(response).await;
    assert!(body.is_empty(), "304 must have empty body");

    // 不一致 → 200
    let response = get(&router, FEED_PATH, Some("\"stale\"")).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn feed_304_with_multiple_and_weak_etags() {
    let (state, router) = setup().await;
    let etag = state.feed.load().etag.clone();

    let header_value = format!("\"other\", W/{etag}");
    let response = get(&router, FEED_PATH, Some(&header_value)).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);

    let response = get(&router, FEED_PATH, Some("*")).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn data_hit_returns_200_and_304() {
    let (_state, router) = setup().await;
    let uri = format!("/developer/xml/data/{UUID_A}.xml");

    let response = get(&router, &uri, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"<Report>cached entity</Report>");

    let response = get(&router, &uri, Some(&etag)).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert!(body_bytes(response).await.is_empty());
}

#[tokio::test]
async fn data_non_jma_id_miss_returns_404() {
    // 非JMA形式ID(UUID・DMDATA電文ID等)は上流に存在しないため404。
    // 無駄なバックグラウンド取得(inflight)も起動しない
    let (state, router) = setup().await;
    let uri = format!("/developer/xml/data/{UUID_MISS}.xml");

    let response = get(&router, &uri, None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        state.inflight.is_empty(),
        "404 miss must not start background fetch"
    );
}

#[tokio::test]
async fn data_jma_style_id_miss_returns_307() {
    // 実JMAフィードのID形式(datetime_serial_TYPE_officecode)はミス時に上流へ307
    let (state, router) = setup().await;
    let response = get(
        &router,
        "/developer/xml/data/20260705050045_0_VFVO53_010000.xml",
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    let location = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(
        location,
        format!(
            "{}/20260705050045_0_VFVO53_010000.xml",
            state.config.jma.data_base_url.trim_end_matches('/')
        )
    );
}

#[tokio::test]
async fn data_without_xml_suffix_returns_404() {
    let (_state, router) = setup().await;
    let response = get(&router, &format!("/developer/xml/data/{UUID_A}"), None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn healthz_is_always_ok() {
    let (_state, router) = setup().await;
    let response = get(&router, "/healthz", None).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn readyz_503_then_200() {
    let (state, router) = setup().await;

    let response = get(&router, "/readyz", None).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["feed"], false);
    assert_eq!(json["ws"], serde_json::json!([false, false]));

    state
        .readiness
        .initial_feed_loaded
        .store(true, Ordering::Relaxed);
    state
        .readiness
        .aggregator_running
        .store(true, Ordering::Relaxed);
    let response = get(&router, "/readyz", None).await;
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "ws not connected yet"
    );

    state.readiness.ws_connected[1].store(true, Ordering::Relaxed);
    let response = get(&router, "/readyz", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["feed"], true);
    assert_eq!(json["aggregator"], true);
    assert_eq!(json["ws"], serde_json::json!([false, true]));
}

#[tokio::test]
async fn singleflight_hits_upstream_once() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{JMA_MISS}.xml")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("<Report>fetched entity</Report>", "application/xml")
                // 全リクエストが singleflight 判定を通過するまで完了を遅らせる
                .set_delay(Duration::from_millis(200)),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (state, rx) = make_state(test_config(), |c| {
        c.jma.data_base_url = mock_server.uri();
    });
    // fetch_entity はEvent経由でaggregatorに渡すため、aggregatorを起動する
    tokio::spawn(jma_relay::aggregator::run(Vec::new(), rx, state.clone()));
    for _ in 0..100 {
        if state.readiness.aggregator_running.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let router = build_router(state.clone());

    // 並行32リクエスト → 全員 307、上流ヒットは先着の1回のみ
    let uri = format!("/developer/xml/data/{JMA_MISS}.xml");
    let futures: Vec<_> = (0..32).map(|_| get(&router, &uri, None)).collect();
    for response in futures_util::future::join_all(futures).await {
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    }

    // バックグラウンド取得の完了(キャッシュ格納 + inflight解除)を待つ
    let mut cached = None;
    for _ in 0..100 {
        if let Some(entry) = state.entities.get(JMA_MISS).await {
            cached = Some(entry);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let entry = cached.expect("entity must be cached after background fetch");
    assert_eq!(&entry.body[..], b"<Report>fetched entity</Report>");
    assert!(state.inflight.is_empty(), "inflight guard must be removed");

    // キャッシュ済みなので次は200
    let response = get(&router, &uri, None).await;
    assert_eq!(response.status(), StatusCode::OK);

    // MockServer drop 時に expect(1) が検証される
}

#[tokio::test]
async fn data_served_from_pinned_with_etag_roundtrip() {
    let (state, router) = setup().await;
    // dmdata電文ID風のentryをpinnedへ投入(aggregatorの役割を模擬)
    let entry = EntityEntry::new(
        Bytes::from_static(b"<Report>pinned entity</Report>"),
        ItemMeta {
            id: "TELEGRAM_ID_PINNED".into(),
            ..ItemMeta::default()
        },
    );
    state
        .pinned
        .insert("TELEGRAM_ID_PINNED".into(), Arc::new(entry));

    let uri = "/developer/xml/data/TELEGRAM_ID_PINNED.xml";
    let response = get(&router, uri, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = header_str(&response, "etag").expect("ETag must be present").to_owned();
    assert!(etag.starts_with('"'));
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"<Report>pinned entity</Report>");

    // If-None-Match 一致 → 304
    let response = get(&router, uri, Some(&etag)).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert!(body_bytes(response).await.is_empty());
}

#[tokio::test]
async fn feed_carries_last_modified_and_instance_started_on_200_and_304() {
    let (state, router) = setup().await;

    let response = get(&router, FEED_PATH, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, "last-modified"),
        Some(FEED_LAST_MODIFIED)
    );
    assert_eq!(
        header_str(&response, "x-instance-started"),
        Some(state.started_at.as_str())
    );
    let etag = header_str(&response, "etag").unwrap().to_owned();

    // 304にも両ヘッダが付く
    let response = get(&router, FEED_PATH, Some(&etag)).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        header_str(&response, "last-modified"),
        Some(FEED_LAST_MODIFIED)
    );
    assert_eq!(
        header_str(&response, "x-instance-started"),
        Some(state.started_at.as_str())
    );
}

#[tokio::test]
async fn feed_if_modified_since_equal_returns_304() {
    let (_state, router) = setup().await;
    let response = get_with_headers(
        &router,
        FEED_PATH,
        &[(header::IF_MODIFIED_SINCE, FEED_LAST_MODIFIED)],
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    // 304にもETag・Last-Modified・X-Instance-Startedが付く
    assert!(header_str(&response, "etag").is_some());
    assert_eq!(
        header_str(&response, "last-modified"),
        Some(FEED_LAST_MODIFIED)
    );
    assert!(header_str(&response, "x-instance-started").is_some());
    assert!(body_bytes(response).await.is_empty());
}

#[tokio::test]
async fn feed_if_modified_since_older_returns_200() {
    let (_state, router) = setup().await;
    let response = get_with_headers(
        &router,
        FEED_PATH,
        &[(header::IF_MODIFIED_SINCE, "Sat, 04 Jul 2026 19:10:11 GMT")],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn feed_if_none_match_wins_over_if_modified_since() {
    // RFC 9110 §13.1.3: If-None-Match があれば If-Modified-Since は無視される。
    // INMが不一致なら、IMSが「未更新」を示していても200を返す
    let (_state, router) = setup().await;
    let response = get_with_headers(
        &router,
        FEED_PATH,
        &[
            (header::IF_NONE_MATCH, "\"mismatching\""),
            (header::IF_MODIFIED_SINCE, FEED_LAST_MODIFIED),
        ],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn feed_garbage_if_modified_since_returns_200() {
    let (_state, router) = setup().await;
    let response = get_with_headers(
        &router,
        FEED_PATH,
        &[(header::IF_MODIFIED_SINCE, "not a valid http date")],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}
