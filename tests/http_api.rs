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

use jma_feed_gateway::config::{Config, DEFAULT_CONFIG_TOML};
use jma_feed_gateway::dmdata::api::DmdataApi;
use jma_feed_gateway::http::build_router;
use jma_feed_gateway::state::{AppState, SharedState};
use jma_feed_gateway::types::{EntityEntry, Event, FeedSnapshot, ItemMeta};
use tokio::sync::mpsc;

const FEED_PATH: &str = "/developer/xml/feed/eqvol.xml";
const UUID_A: &str = "ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b";
const MISS_ID: &str = "20260705050045_0_VXSE99_010000";
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
    let dmdata_api = DmdataApi::new(
        client.clone(),
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

/// inflight ガードの解除を待つ(fetch_entity の完了はレスポンスより僅かに遅れうる)。
async fn wait_inflight_empty(state: &SharedState) {
    for _ in 0..100 {
        if state.inflight.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("inflight guard must be removed");
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
async fn data_invalid_id_miss_returns_404_without_inflight() {
    // 電文IDとしてありえない形式(`.` 等を含むゴミID)は即404。
    // 無駄なバックグラウンド取得(inflight)も起動しない
    let (state, router) = setup().await;

    // "a%20b" はPath抽出で "a b"(空白入り)にデコードされる
    for garbage in ["foo.bar", "..", "a%20b"] {
        let response = get(&router, &format!("/developer/xml/data/{garbage}.xml"), None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "id={garbage}");
    }
    assert!(
        state.inflight.is_empty(),
        "404 miss must not start background fetch"
    );
}

#[tokio::test]
async fn data_miss_holds_and_serves_200() {
    // 電文ID形式のミスはレスポンスを保留し、取得完了後に200で返す
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{MISS_ID}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("<Report>held entity</Report>", "application/xml"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // ミス補充はdmdata telegram.data(.xmlサフィックス無し)から取得する
    let (state, rx) = make_state(test_config(), |c| {
        c.dmdata.data_api_base = mock_server.uri();
    });
    // 待機者はwatch経由で直接entryを受け取るため、aggregatorなしでも200になる
    std::mem::forget(rx);
    // feed在中IDのみアウトバウンドfetch可(アローリスト)
    state.feed_ids.insert(MISS_ID.into());
    let router = build_router(state.clone());

    let response = get(&router, &format!("/developer/xml/data/{MISS_ID}.xml"), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        header_str(&response, "etag").is_some_and(|e| e.starts_with('"')),
        "ETag must be present"
    );
    assert!(
        response.headers().get(header::LOCATION).is_none(),
        "200 must not carry Location"
    );
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"<Report>held entity</Report>");

    // fetch_entityの完了(InflightGuard解除)はレスポンスより僅かに遅れうるため待つ
    wait_inflight_empty(&state).await;
}

#[tokio::test]
async fn data_without_xml_suffix_returns_404() {
    let (_state, router) = setup().await;
    let response = get(&router, &format!("/developer/xml/data/{UUID_A}"), None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn eqvol_l_serves_same_feed_as_eqvol() {
    // 長期版パスは現行フィード(eqvol.xml)と同一のボディ・ETagを返す
    let (_state, router) = setup().await;
    const EQVOL_L_PATH: &str = "/developer/xml/feed/eqvol_l.xml";

    let eqvol = get(&router, FEED_PATH, None).await;
    let eqvol_etag = header_str(&eqvol, "etag").unwrap().to_owned();
    let eqvol_body = body_bytes(eqvol).await;

    let response = get(&router, EQVOL_L_PATH, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        header_str(&response, "content-type")
            .unwrap()
            .contains("atom+xml")
    );
    assert_eq!(header_str(&response, "etag"), Some(eqvol_etag.as_str()));
    // fallback に食われず 307 のLocationが付かないこと
    assert!(response.headers().get(header::LOCATION).is_none());
    assert_eq!(body_bytes(response).await, eqvol_body);

    // 304 ラウンドトリップも効く
    let response = get(&router, EQVOL_L_PATH, Some(&eqvol_etag)).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn unmatched_path_redirects_to_upstream() {
    // 未対応パスは 307 で上流JMAへ転送される
    let (_state, router) = setup().await;
    let response = get(&router, "/developer/xml/feed/extra.xml", None).await;
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        header_str(&response, "location"),
        Some("https://www.data.jma.go.jp/developer/xml/feed/extra.xml")
    );
}

#[tokio::test]
async fn upstream_redirect_preserves_query() {
    let (_state, router) = setup().await;
    let response = get(&router, "/foo?a=1&b=2", None).await;
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        header_str(&response, "location"),
        Some("https://www.data.jma.go.jp/foo?a=1&b=2")
    );
}

#[tokio::test]
async fn upstream_redirect_never_leaves_jma_host() {
    // オープンリダイレクト不成立: どんな細工パスでも Location は必ずJMAホスト始まり
    let (_state, router) = setup().await;
    for evil in ["//evil.com/x", "/%2F%2Fevil.com/x", "/\\evil.com"] {
        let response = get(&router, evil, None).await;
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "uri={evil}");
        let location = header_str(&response, "location").unwrap();
        assert!(
            location.starts_with("https://www.data.jma.go.jp/"),
            "uri={evil} leaked to {location}"
        );
    }
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
    assert_eq!(json["ws"], serde_json::json!([false]));
    assert_eq!(json["poll"], false);

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

    state.readiness.ws_connected[0].store(true, Ordering::Relaxed);
    let response = get(&router, "/readyz", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["feed"], true);
    assert_eq!(json["aggregator"], true);
    assert_eq!(json["ws"], serde_json::json!([true]));

    // WS全断でもフォールバックpolling稼働中はready
    state.readiness.ws_connected[0].store(false, Ordering::Relaxed);
    let response = get(&router, "/readyz", None).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    state.readiness.poll_active.store(true, Ordering::Relaxed);
    let response = get(&router, "/readyz", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["poll"], true);
}

#[tokio::test]
async fn singleflight_hits_upstream_once() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{MISS_ID}")))
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
        c.dmdata.data_api_base = mock_server.uri();
    });
    // feed在中IDのみアウトバウンドfetch可(アローリスト)
    state.feed_ids.insert(MISS_ID.into());
    // fetch_entity はEvent経由でaggregatorに渡すため、aggregatorを起動する
    tokio::spawn(jma_feed_gateway::aggregator::run(
        Vec::new(),
        rx,
        state.clone(),
    ));
    for _ in 0..100 {
        if state.readiness.aggregator_running.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let router = build_router(state.clone());

    // 並行32リクエスト → 全員が同じwatchで待機し、完成entryを200で受け取る。
    // 上流ヒットは先着の1回のみ
    let uri = format!("/developer/xml/data/{MISS_ID}.xml");
    let futures: Vec<_> = (0..32).map(|_| get(&router, &uri, None)).collect();
    let mut first_etag: Option<String> = None;
    for response in futures_util::future::join_all(futures).await {
        assert_eq!(response.status(), StatusCode::OK);
        let etag = header_str(&response, "etag")
            .expect("ETag must be present")
            .to_owned();
        match &first_etag {
            Some(expected) => assert_eq!(&etag, expected, "all waiters must share one ETag"),
            None => first_etag = Some(etag),
        }
        let body = body_bytes(response).await;
        assert_eq!(&body[..], b"<Report>fetched entity</Report>");
    }

    // 取得完了後の後処理(キャッシュ格納 + inflight解除)を待つ
    let mut cached = None;
    for _ in 0..100 {
        if let Some(entry) = state.entities.get(MISS_ID).await {
            cached = Some(entry);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let entry = cached.expect("entity must be cached after fetch");
    assert_eq!(&entry.body[..], b"<Report>fetched entity</Report>");
    wait_inflight_empty(&state).await;

    // キャッシュ済みなので次は200
    let response = get(&router, &uri, None).await;
    assert_eq!(response.status(), StatusCode::OK);

    // MockServer drop 時に expect(1) が検証される
}

#[tokio::test]
async fn data_miss_upstream_error_returns_404() {
    // 上流が5xxで取得失敗(Sender drop)した場合は404(dmdata data APIは
    // 認証必須でリダイレクト先にならないため、旧JMA上流への307は廃止)
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{MISS_ID}")))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let (state, rx) = make_state(test_config(), |c| {
        c.dmdata.data_api_base = mock_server.uri();
    });
    std::mem::forget(rx);
    // feed在中IDのみアウトバウンドfetch可(アローリスト)
    state.feed_ids.insert(MISS_ID.into());
    let router = build_router(state.clone());

    let response = get(&router, &format!("/developer/xml/data/{MISS_ID}.xml"), None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        response.headers().get(header::LOCATION).is_none(),
        "404 must not carry Location"
    );
    wait_inflight_empty(&state).await;
}

#[tokio::test]
async fn data_miss_fetch_timeout_returns_404() {
    // 上流応答が待機予算(fetch_timeout_secs + 1秒)を超える場合も404
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{MISS_ID}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("<Report>too late</Report>", "application/xml")
                // 待機予算(0 + 1秒)を確実に超える遅延
                .set_delay(Duration::from_secs(3)),
        )
        .mount(&mock_server)
        .await;

    let (state, rx) = make_state(test_config(), |c| {
        c.dmdata.data_api_base = mock_server.uri();
        c.dmdata.fetch_timeout_secs = 0;
    });
    std::mem::forget(rx);
    // feed在中IDのみアウトバウンドfetch可(アローリスト)
    state.feed_ids.insert(MISS_ID.into());
    let router = build_router(state.clone());

    let response = get(&router, &format!("/developer/xml/data/{MISS_ID}.xml"), None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        response.headers().get(header::LOCATION).is_none(),
        "404 must not carry Location"
    );
}

#[tokio::test]
async fn data_miss_outside_feed_returns_404_without_outbound() {
    // feed外ID(整形済みでもアローリスト不在)は404、アウトバウンド呼び出しなし
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("<Report>never served</Report>", "application/xml"),
        )
        .expect(0)
        .mount(&mock_server)
        .await;

    let (state, rx) = make_state(test_config(), |c| {
        c.dmdata.data_api_base = mock_server.uri();
    });
    std::mem::forget(rx);
    // feed_ids は seed しない = feed外ID
    let router = build_router(state.clone());

    let response = get(&router, &format!("/developer/xml/data/{MISS_ID}.xml"), None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        state.inflight.is_empty(),
        "allowlist miss must not start background fetch"
    );
    // MockServer drop 時に expect(0) が検証される
}

#[tokio::test]
async fn data_miss_rate_limited_returns_503() {
    // レートリミット超過(3件目)は503、アウトバウンドは上限の2回のみ
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw("<Report>fetched</Report>", "application/xml"),
        )
        .expect(2)
        .mount(&mock_server)
        .await;

    let (state, rx) = make_state(test_config(), |c| {
        c.dmdata.data_api_base = mock_server.uri();
        c.rate_limit.max_requests = 2;
    });
    std::mem::forget(rx);

    let ids = [
        "20260705050001_0_VXSE53_010000",
        "20260705050002_0_VXSE53_010000",
        "20260705050003_0_VXSE53_010000",
    ];
    for id in ids {
        state.feed_ids.insert(id.into());
    }
    let router = build_router(state.clone());

    for id in &ids[..2] {
        let response = get(&router, &format!("/developer/xml/data/{id}.xml"), None).await;
        assert_eq!(response.status(), StatusCode::OK, "id={id}");
    }
    let response = get(
        &router,
        &format!("/developer/xml/data/{}.xml", ids[2]),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        !state.inflight.contains_key(ids[2]),
        "rate-limited request must not start background fetch"
    );
    wait_inflight_empty(&state).await;
    // MockServer drop 時に expect(2) が検証される
}

#[tokio::test]
async fn data_evicted_from_feed_but_cached_returns_200() {
    // feedからevict済み(feed_ids不在)でもentitiesキャッシュ在中なら配信される
    let (state, router) = setup().await;
    assert!(!state.feed_ids.contains(UUID_A), "premise: not in feed_ids");

    let response = get(&router, &format!("/developer/xml/data/{UUID_A}.xml"), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"<Report>cached entity</Report>");
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
    let etag = header_str(&response, "etag")
        .expect("ETag must be present")
        .to_owned();
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
