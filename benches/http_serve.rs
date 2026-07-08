//! HTTP配信パスのベースラインベンチ(tower::ServiceExt::oneshot、ネットワーク不要)。
//! criterion による時間計測に加え、カウンティングアロケータで allocs/req を出力する。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use criterion::Criterion;
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

// ---- カウンティンググローバルアロケータ ----

struct CountingAllocator;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

// ---- セットアップ(tests/http_api.rs と同等) ----

const FEED_PATH: &str = "/developer/xml/feed/eqvol.xml";
const PINNED_ID: &str = "TELEGRAM_ID_PINNED";

fn test_config() -> Config {
    Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load")
}

fn make_state(config: Config) -> SharedState {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel::<Event>(64);
    // aggregatorは起動しない。チャネルを開いたままにする
    std::mem::forget(rx);
    let dmdata_api = DmdataApi::new(
        client.clone(),
        config.dmdata.api_base.clone(),
        config.dmdata.data_api_base.clone(),
        "bench-api-key",
        None,
    );
    Arc::new(AppState::new(Arc::new(config), dmdata_api, tx))
}

async fn setup() -> (SharedState, Router) {
    let state = make_state(test_config());
    // フィードスナップショットを設定
    let body = Bytes::from_static(b"<?xml version=\"1.0\"?><feed>bench</feed>");
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
    // pinned に1件投入(aggregatorの役割を模擬)
    let entry = EntityEntry::new(
        Bytes::from_static(b"<Report>pinned entity</Report>"),
        ItemMeta {
            id: PINNED_ID.into(),
            ..ItemMeta::default()
        },
    );
    state.pinned.insert(PINNED_ID.into(), Arc::new(entry));
    let router = build_router(state.clone());
    (state, router)
}

fn build_request(uri: &str, if_none_match: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(uri);
    if let Some(inm) = if_none_match {
        builder = builder.header(header::IF_NONE_MATCH, inm);
    }
    builder.body(Body::empty()).unwrap()
}

async fn one_request(router: &Router, uri: &str, if_none_match: Option<&str>, expect: StatusCode) {
    let response = router
        .clone()
        .oneshot(build_request(uri, if_none_match))
        .await
        .unwrap();
    assert_eq!(response.status(), expect, "uri={uri} inm={if_none_match:?}");
    let _ = response.into_body().collect().await.unwrap();
}

/// シナリオ定義: (名前, URI, If-None-Match, 期待ステータス)
struct Scenario {
    name: &'static str,
    uri: String,
    if_none_match: Option<String>,
    expect: StatusCode,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (state, router) = runtime.block_on(setup());

    // etag を state / 実レスポンスから取得
    let feed_etag = state.feed.load().etag.clone();
    let data_uri = format!("/developer/xml/data/{PINNED_ID}.xml");
    let data_etag = runtime.block_on(async {
        let response = router
            .clone()
            .oneshot(build_request(&data_uri, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .expect("data ETag must be present")
            .to_str()
            .unwrap()
            .to_owned();
        let _ = response.into_body().collect().await.unwrap();
        etag
    });

    let scenarios = [
        Scenario {
            name: "feed_200",
            uri: FEED_PATH.to_owned(),
            if_none_match: None,
            expect: StatusCode::OK,
        },
        Scenario {
            name: "feed_304",
            uri: FEED_PATH.to_owned(),
            if_none_match: Some(feed_etag),
            expect: StatusCode::NOT_MODIFIED,
        },
        Scenario {
            name: "data_pinned_200",
            uri: data_uri.clone(),
            if_none_match: None,
            expect: StatusCode::OK,
        },
        Scenario {
            name: "data_304",
            uri: data_uri.clone(),
            if_none_match: Some(data_etag),
            expect: StatusCode::NOT_MODIFIED,
        },
    ];

    // ---- allocs/req 計測(criterion 実行前に手動で) ----
    const WARMUP: u64 = 500;
    const N: u64 = 10_000;
    println!("== allocation counts (N={N} requests per scenario) ==");
    for s in &scenarios {
        let inm = s.if_none_match.as_deref();
        // ウォームアップ(計数開始前)
        runtime.block_on(async {
            for _ in 0..WARMUP {
                one_request(&router, &s.uri, inm, s.expect).await;
            }
        });
        let before = ALLOC_COUNT.load(Ordering::Relaxed);
        runtime.block_on(async {
            for _ in 0..N {
                one_request(&router, &s.uri, inm, s.expect).await;
            }
        });
        let allocs = ALLOC_COUNT.load(Ordering::Relaxed) - before;
        println!(
            "{:<16} {:>8.1} allocs/req  (total {allocs})",
            s.name,
            allocs as f64 / N as f64
        );
    }
    println!();

    // ---- criterion 時間計測 ----
    let mut criterion = Criterion::default().configure_from_args();
    for s in &scenarios {
        let inm = s.if_none_match.as_deref();
        criterion.bench_function(s.name, |bencher| {
            bencher.to_async(&runtime).iter(|| {
                let router = router.clone();
                let request = build_request(&s.uri, inm);
                async move {
                    let response = router.oneshot(request).await.unwrap();
                    assert_eq!(response.status(), s.expect);
                    let _ = response.into_body().collect().await.unwrap();
                }
            });
        });
    }
    criterion.final_summary();
}
