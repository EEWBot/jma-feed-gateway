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

use jma_feed_gateway::config::{Config, DEFAULT_CONFIG_TOML};
use jma_feed_gateway::poller::{PollOutcome, Poller};
use jma_feed_gateway::state::{AppState, SharedState};
use jma_feed_gateway::types::{Event, EventSource, FeedSnapshot};

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
    setup_with(server, |_| {}).await
}

/// setup の設定調整版(entry_fetch_limit 等を上書きするテスト用)。
async fn setup_with(
    server: &MockServer,
    mutate: impl FnOnce(&mut Config),
) -> (SharedState, mpsc::Receiver<Event>, Poller) {
    let mut config: Config = Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load");
    config.jma.feed_url = format!("{}{FEED_PATH}", server.uri());
    config.jma.data_base_url = format!("{}/developer/xml/data", server.uri());
    mutate(&mut config);

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
    mount_304_on_ims(&server).await;
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

/// IMSがLAST_MODIFIEDに一致するリクエストへ304を返すモック
/// (値にカンマを含むため matchers::header は使えない — 生値の完全一致で判定)。
async fn mount_304_on_ims(server: &MockServer) {
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
        .mount(server)
        .await;
}

/// aggregatorによるwatermark前進を模擬する(NEW_IDのpublish後の値)。
fn advance_watermark_to_new(state: &SharedState) {
    state.feed.store(Arc::new(FeedSnapshot::new(
        Bytes::new(),
        "2026-07-05T04:11:00+09:00".into(),
        // = NEW_IDのupdated。OLD_ID(03:50)はこれよりslack(600s)以上古い
        Some(time::macros::datetime!(2026-07-04 19:11:00 UTC)),
    )));
}

#[tokio::test]
async fn failed_entity_survives_watermark_advance_and_is_retried() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // IMS一致なら304(未処理を残したtickでLast-Modifiedがコミットされて
    // いたら2回目が304になり再試行できない — その退行を検出する)
    mount_304_on_ims(&server).await;
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
    // 古い側の実体: 初回だけ500、以降200
    Mock::given(method("GET"))
        .and(path(format!("/developer/xml/data/{OLD_ID}.xml")))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_entity(&server, OLD_ID).await;
    mount_entity(&server, NEW_ID).await;

    // 初回: OLDのfetch失敗(→pending)、NEWはpublish。Last-Modified未コミット
    let outcome = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(1));
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());

    // NEWのpublishによりaggregatorがwatermarkを前進させた状況を模擬。
    // OLDはwatermark-slackより古い → watermark再選別に通すと永久喪失する
    advance_watermark_to_new(&state);

    // 2回目: pendingはwatermarkをバイパスして再fetchされpublish。ここでコミット
    let outcome = poller.poll_once().await.expect("retry poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(1));
    assert_eq!(rx.try_recv().unwrap().meta.id, OLD_ID);

    // 3回目: pending空なのでIMSが送られ304
    let outcome = poller.poll_once().await.expect("third poll must succeed");
    assert_eq!(outcome, PollOutcome::NotModified);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn deferred_candidates_survive_watermark_advance() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup_with(&server, |c| c.poll.entry_fetch_limit = 1).await;

    // IMS一致なら304(持ち越しを残したtickでコミットされていたら退行)
    mount_304_on_ims(&server).await;
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

    // 初回: 上限1件 → 最新のみpublish、古い側はpendingへ。Last-Modified未コミット
    let outcome = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(1));
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());

    // watermark前進を模擬(OLDはwatermark-slackより古い)
    advance_watermark_to_new(&state);

    // 2回目: pendingがwatermarkをバイパスしてpublishされ、ここでコミット
    let outcome = poller.poll_once().await.expect("second poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(1));
    assert_eq!(rx.try_recv().unwrap().meta.id, OLD_ID);

    // 3回目: pending空なのでIMSが送られ304
    let outcome = poller.poll_once().await.expect("third poll must succeed");
    assert_eq!(outcome, PollOutcome::NotModified);
}

#[tokio::test]
async fn pending_is_drained_while_ws_connected() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

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

    // アウトエージ中の初回poll: fetch失敗 → pending
    let outcome = poller.poll_once().await.expect("poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(0));
    assert!(rx.try_recv().is_err());

    // WS復帰(run()がpollをスキップするtick)でもpendingは処理し切る —
    // WSは切断中に発行された電文を再配信しないため
    state.readiness.ws_connected[0].store(true, Ordering::Relaxed);
    poller.drain_pending().await;
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);

    // pending空なら何もしない
    poller.drain_pending().await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn pending_gone_from_feed_is_dropped_and_commit_proceeds() {
    let server = MockServer::start().await;
    let (_state, mut rx, mut poller) = setup(&server).await;

    mount_304_on_ims(&server).await;
    // 初回フィード: OLD(恒久500)+ NEW
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("last-modified", "Sat, 04 Jul 2026 19:10:00 GMT")
                .set_body_string(feed_xml(
                    &server.uri(),
                    &[
                        (NEW_ID, "2026-07-05T04:11:00+09:00"),
                        (OLD_ID, "2026-07-05T03:50:00+09:00"),
                    ],
                )),
        )
        .up_to_n_times(1)
        .with_priority(2)
        .mount(&server)
        .await;
    // 2回目以降のフィード: OLDは一覧から消えた
    Mock::given(method("GET"))
        .and(path(FEED_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("last-modified", LAST_MODIFIED)
                .set_body_string(feed_xml(
                    &server.uri(),
                    &[(NEW_ID, "2026-07-05T04:11:00+09:00")],
                )),
        )
        .mount(&server)
        .await;
    // OLDの実体は恒久的に500
    Mock::given(method("GET"))
        .and(path(format!("/developer/xml/data/{OLD_ID}.xml")))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;

    // 初回: NEW publish、OLDは失敗しpendingへ(コミット保留)
    let outcome = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(1));
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);

    // 2回目: OLDがフィード一覧から消えた → pendingから破棄され、コミットが進む
    let outcome = poller.poll_once().await.expect("second poll must succeed");
    assert_eq!(outcome, PollOutcome::Published(0));
    assert!(rx.try_recv().is_err());

    // 3回目: pending空+コミット済みなのでIMSが送られ304(永久全量fetchに陥らない)
    let outcome = poller.poll_once().await.expect("third poll must succeed");
    assert_eq!(outcome, PollOutcome::NotModified);
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
