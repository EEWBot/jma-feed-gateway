//! Pollerの統合テスト: wiremockで上流JMAを模擬し、poll_once の
//! conditional GET / watermark / リトライ / readiness遷移を検証する。
//! 壁時計ループ(run)はテスト対象外(tokio::time::pause は now_utc を動かせない)。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use figment::Figment;
use figment::providers::{Format, Toml};
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use jma_relay::config::{Config, DEFAULT_CONFIG_TOML};
use jma_relay::poller::{PollOutcome, Poller};
use jma_relay::state::{AppState, SharedState};
use jma_relay::types::{Event, EventSource, FeedSnapshot};

const FEED_PATH: &str = "/developer/xml/feed/eqvol.xml";
const OLD_ID: &str = "20260705035000_0_VXSE53_010000";
const NEW_ID: &str = "20260705041100_0_VXSE53_010000";
const LAST_MODIFIED: &str = "Sat, 04 Jul 2026 19:11:00 GMT";

/// JMA形式ID(実フィードはUUIDではない)でモックフィードXMLを組み立てる。
fn feed_xml(base: &str, entries: &[(&str, &str)]) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
<title>高頻度(地震火山)</title>
<updated>2026-07-05T04:11:00+09:00</updated>
"#,
    );
    for (id, updated) in entries {
        xml.push_str(&format!(
            r#"<entry>
<title>震源・震度に関する情報</title>
<id>{base}/developer/xml/data/{id}.xml</id>
<updated>{updated}</updated>
<author><name>気象庁</name></author>
<link type="application/xml" href="{base}/developer/xml/data/{id}.xml"/>
<content type="text">テスト本文</content>
</entry>
"#
        ));
    }
    xml.push_str("</feed>");
    xml
}

fn entity_xml(id: &str) -> String {
    format!("<Report>{id}</Report>")
}

/// モックサーバへ向けた state / event受信口 / Poller を作る。
async fn setup(server: &MockServer) -> (SharedState, mpsc::Receiver<Event>, Poller) {
    let mut config: Config = Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load");
    config.jma.feed_url = format!("{}{FEED_PATH}", server.uri());
    config.jma.data_base_url = format!("{}/developer/xml/data", server.uri());

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel::<Event>(64);
    let state = Arc::new(AppState::new(Arc::new(config), client, tx));
    let poller = Poller::new(state.clone());
    (state, rx, poller)
}

/// 実体エンドポイントのモックを登録する。
async fn mount_entity(server: &MockServer, id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/developer/xml/data/{id}.xml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(entity_xml(id)))
        .mount(server)
        .await;
}

#[tokio::test]
async fn first_poll_publishes_then_conditional_get_304() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // 2回目以降: 保存したLast-Modified生値のIf-Modified-Sinceを送ってきたら304
    // (値にカンマを含むため matchers::header は使えない — 生値の完全一致で判定)
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .and(|req: &wiremock::Request| {
            req.headers
                .get("if-modified-since")
                .and_then(|v| v.to_str().ok())
                == Some(LAST_MODIFIED)
        })
        .respond_with(ResponseTemplate::new(304))
        .with_priority(1)
        .mount(&server)
        .await;
    // 初回(IMS無し): 200 + Last-Modified
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("last-modified", LAST_MODIFIED)
                .set_body_string(feed_xml(
                    &server.uri(),
                    &[
                        (NEW_ID, "2026-07-05T04:11:00+09:00"),
                        (OLD_ID, "2026-07-05T03:50:00+09:00"),
                    ],
                )),
        )
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;
    mount_entity(&server, OLD_ID).await;

    // 初回poll: watermark無し(空スナップショット)→ 2件ともupdated昇順でpublish
    let outcome = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(2));

    let first = rx.try_recv().expect("oldest entry must be published first");
    assert_eq!(first.meta.id, OLD_ID);
    assert_eq!(first.source, EventSource::JmaPoll);
    assert_eq!(&first.xml_body[..], entity_xml(OLD_ID).as_bytes());
    let second = rx
        .try_recv()
        .expect("newest entry must be published second");
    assert_eq!(second.meta.id, NEW_ID);
    assert_eq!(second.source, EventSource::JmaPoll);
    assert!(rx.try_recv().is_err());

    // 2回目poll: 保存した生値でconditional GET → 304、何もpublishしない
    let outcome = poller.poll_once().await.expect("second poll must succeed");
    assert_eq!(outcome, PollOutcome::NotModified);
    assert!(rx.try_recv().is_err());
    assert!(state.readiness.poll_active.load(Ordering::Relaxed));
}

#[tokio::test]
async fn entries_older_than_watermark_are_not_fetched() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // watermark = 2026-07-05T04:10:00+09:00(aggregatorが設定する想定の値を模擬)
    let watermark = time::macros::datetime!(2026-07-04 19:10:00 UTC);
    state.feed.store(Arc::new(FeedSnapshot::new(
        Bytes::new(),
        "2026-07-05T04:10:00+09:00".into(),
        Some(watermark),
    )));

    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed_xml(
            &server.uri(),
            &[
                (NEW_ID, "2026-07-05T04:11:00+09:00"),
                // watermark - slack(600s) より古い → 既配信としてfetchしない
                (OLD_ID, "2026-07-05T03:50:00+09:00"),
            ],
        )))
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;
    // 古いentryの実体は一切リクエストされないこと
    Mock::given(method("GET"))
        .and(path(format!("/developer/xml/data/{OLD_ID}.xml")))
        .respond_with(ResponseTemplate::new(200).set_body_string(entity_xml(OLD_ID)))
        .expect(0)
        .mount(&server)
        .await;

    let outcome = poller.poll_once().await.expect("poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(1));
    let event = rx.try_recv().unwrap();
    assert_eq!(event.meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());
    // MockServer drop時に expect(0) が検証される
}

#[tokio::test]
async fn failed_entity_fetch_is_retried_on_next_poll() {
    let server = MockServer::start().await;
    let (_state, mut rx, mut poller) = setup(&server).await;

    // フィードはLast-Modifiedヘッダ無し → 2回目も無条件GETで200
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed_xml(
            &server.uri(),
            &[(NEW_ID, "2026-07-05T04:11:00+09:00")],
        )))
        .mount(&server)
        .await;
    // 実体: 初回だけ500、以降200
    Mock::given(method("GET"))
        .and(path(format!("/developer/xml/data/{NEW_ID}.xml")))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;

    // 初回: 実体fetch失敗 → publishゼロだがpoll自体は成功
    let outcome = poller.poll_once().await.expect("poll itself must succeed");
    assert_eq!(outcome, PollOutcome::Published(0));
    assert!(rx.try_recv().is_err());

    // 2回目: seen未登録なので再fetchされpublishされる
    let outcome = poller.poll_once().await.expect("retry poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(1));
    let event = rx.try_recv().unwrap();
    assert_eq!(event.meta.id, NEW_ID);
}

#[tokio::test]
async fn poll_active_transitions_with_poll_result() {
    let server = MockServer::start().await;
    let (state, _rx, mut poller) = setup(&server).await;
    assert!(!state.readiness.poll_active.load(Ordering::Relaxed));

    // 初回だけ200(空フィード)、以降500
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed_xml(&server.uri(), &[])))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    // 成功(publishゼロでも)→ poll_active = true
    poller.poll_once().await.expect("first poll must succeed");
    assert!(state.readiness.poll_active.load(Ordering::Relaxed));
    assert!(state.readiness.snapshot().poll);

    // 失敗 → poll_active = false
    poller.poll_once().await.expect_err("second poll must fail");
    assert!(!state.readiness.poll_active.load(Ordering::Relaxed));
    assert!(!state.readiness.snapshot().poll);
}
