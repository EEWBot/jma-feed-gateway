//! Pollerの統合テスト: wiremockでdmdata(telegram.list / telegram.data)を模擬し、
//! poll_once の watermark / pending持ち越し / dedup / readiness遷移を検証する。
//! 壁時計ループ(run)はテスト対象外(tokio::time::pause は now_utc を動かせない)。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use figment::Figment;
use figment::providers::{Format, Toml};
use tokio::sync::mpsc;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use jma_feed_gateway::aggregator;
use jma_feed_gateway::config::{Config, DEFAULT_CONFIG_TOML};
use jma_feed_gateway::dmdata::api::DmdataApi;
use jma_feed_gateway::poller::Poller;
use jma_feed_gateway::state::{AppState, SharedState};
use jma_feed_gateway::types::{DedupKey, Event, EventSource, FeedSnapshot, ItemMeta};

const LIST_PATH: &str = "/telegram";
const OLD_ID: &str = "TELEGRAM_OLD_VXSE53_0000000000000000000000000000000000000000";
const NEW_ID: &str = "TELEGRAM_NEW_VXSE53_0000000000000000000000000000000000000000";

/// telegram.list のitem JSONを組み立てる(xmlReport付き・format=xml)。
fn list_item(id: &str, updated: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "head": {"type": "VXSE53", "test": false, "author": "JPOS"},
        "receivedTime": updated,
        "xmlReport": {
            "control": {"title": "震源・震度に関する情報", "publishingOffice": "気象庁"},
            "head": {"reportDateTime": updated, "headline": "テスト本文"}
        },
        "format": "xml"
    })
}

/// telegram.list のレスポンスJSONを組み立てる。
fn list_body(entries: &[(&str, &str)]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = entries
        .iter()
        .map(|(id, updated)| list_item(id, updated))
        .collect();
    serde_json::json!({"status": "ok", "items": items, "nextToken": null})
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
    config.dmdata.api_base = server.uri();
    config.dmdata.data_api_base = format!("{}/v1", server.uri());
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
    let state = Arc::new(AppState::new(Arc::new(config), dmdata_api, tx));
    let poller = Poller::new(state.clone());
    (state, rx, poller)
}

/// telegram.list エンドポイントのモックを登録する。
async fn mount_list(server: &MockServer, entries: &[(&str, &str)]) {
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(list_body(entries)))
        .mount(server)
        .await;
}

/// telegram.data(実体)エンドポイントのモックを登録する。
async fn mount_entity(server: &MockServer, id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/v1/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(entity_xml(id)))
        .mount(server)
        .await;
}

#[tokio::test]
async fn first_poll_publishes_then_seen_dedups_next_tick() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .and(query_param("classification", "telegram.earthquake"))
        .and(query_param("xmlReport", "true"))
        .and(query_param("test", "no"))
        .respond_with(ResponseTemplate::new(200).set_body_json(list_body(&[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ])))
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;
    mount_entity(&server, OLD_ID).await;

    // 初回poll: watermark無し(空スナップショット)→ 2件ともupdated昇順でpublish
    let published = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(published, 2);

    let first = rx.try_recv().expect("oldest entry must be published first");
    assert_eq!(first.meta.id, OLD_ID);
    assert_eq!(first.source, EventSource::DmdataPoll);
    assert_eq!(first.dedup_key, DedupKey::TelegramId(OLD_ID.into()));
    assert_eq!(&first.xml_body[..], entity_xml(OLD_ID).as_bytes());
    let second = rx
        .try_recv()
        .expect("newest entry must be published second");
    assert_eq!(second.meta.id, NEW_ID);
    assert_eq!(second.source, EventSource::DmdataPoll);
    assert!(rx.try_recv().is_err());

    // 2回目poll: 同じリストだがseen済み → 何もpublishしない
    let published = poller.poll_once().await.expect("second poll must succeed");
    assert_eq!(published, 0);
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

    mount_list(
        &server,
        &[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            // watermark - slack(600s) より古い → 既配信としてfetchしない
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ],
    )
    .await;
    mount_entity(&server, NEW_ID).await;
    // 古いentryの実体は一切リクエストされないこと
    Mock::given(method("GET"))
        .and(path(format!("/v1/{OLD_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(entity_xml(OLD_ID)))
        .expect(0)
        .mount(&server)
        .await;

    let published = poller.poll_once().await.expect("poll must succeed");
    assert_eq!(published, 1);
    let event = rx.try_recv().unwrap();
    assert_eq!(event.meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());
    // MockServer drop時に expect(0) が検証される
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

    mount_list(
        &server,
        &[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ],
    )
    .await;
    // 古い側の実体: 初回だけ500、以降200
    Mock::given(method("GET"))
        .and(path(format!("/v1/{OLD_ID}")))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_entity(&server, OLD_ID).await;
    mount_entity(&server, NEW_ID).await;

    // 初回: OLDのfetch失敗(→pending)、NEWはpublish
    let published = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(published, 1);
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());

    // NEWのpublishによりaggregatorがwatermarkを前進させた状況を模擬。
    // OLDはwatermark-slackより古い → watermark再選別に通すと永久喪失する
    advance_watermark_to_new(&state);

    // 2回目: pendingはwatermarkをバイパスして再fetchされpublish
    let published = poller.poll_once().await.expect("retry poll must succeed");
    assert_eq!(published, 1);
    assert_eq!(rx.try_recv().unwrap().meta.id, OLD_ID);

    // 3回目: 全件seen済み → 何もpublishしない
    let published = poller.poll_once().await.expect("third poll must succeed");
    assert_eq!(published, 0);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn deferred_candidates_survive_watermark_advance() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup_with(&server, |c| c.poll.entry_fetch_limit = 1).await;

    mount_list(
        &server,
        &[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ],
    )
    .await;
    mount_entity(&server, NEW_ID).await;
    mount_entity(&server, OLD_ID).await;

    // 初回: 上限1件 → 最新のみpublish、古い側はpendingへ
    let published = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(published, 1);
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());

    // watermark前進を模擬(OLDはwatermark-slackより古い)
    advance_watermark_to_new(&state);

    // 2回目: pendingがwatermarkをバイパスしてpublishされる
    let published = poller.poll_once().await.expect("second poll must succeed");
    assert_eq!(published, 1);
    assert_eq!(rx.try_recv().unwrap().meta.id, OLD_ID);

    // 3回目: 全件seen済み → 何もpublishしない
    let published = poller.poll_once().await.expect("third poll must succeed");
    assert_eq!(published, 0);
}

#[tokio::test]
async fn pending_is_drained_while_ws_connected() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    mount_list(&server, &[(NEW_ID, "2026-07-05T04:11:00+09:00")]).await;
    // 実体: 初回だけ500、以降200
    Mock::given(method("GET"))
        .and(path(format!("/v1/{NEW_ID}")))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;

    // アウトエージ中の初回poll: fetch失敗 → pending
    let published = poller.poll_once().await.expect("poll must succeed");
    assert_eq!(published, 0);
    assert!(rx.try_recv().is_err());

    // WS復帰(run()がpollをスキップするtick)でもpendingは処理し切る —
    // WSは切断中に発行された電文を再配信しない
    state.readiness.ws_connected[0].store(true, Ordering::Relaxed);
    poller.drain_pending().await;
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);

    // pending空なら何もしない
    poller.drain_pending().await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn pending_gone_from_list_is_dropped() {
    let server = MockServer::start().await;
    let (_state, mut rx, mut poller) = setup(&server).await;

    // 初回リスト: OLD(恒久500)+ NEW
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(list_body(&[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ])))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    // 2回目以降のリスト: OLDは一覧から消えた
    mount_list(&server, &[(NEW_ID, "2026-07-05T04:11:00+09:00")]).await;
    // OLDの実体は恒久的に500
    Mock::given(method("GET"))
        .and(path(format!("/v1/{OLD_ID}")))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;

    // 初回: NEW publish、OLDは失敗しpendingへ
    let published = poller.poll_once().await.expect("first poll must succeed");
    assert_eq!(published, 1);
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);

    // 2回目: OLDがリスト一覧から消えた → pendingから破棄され再fetchされない
    let published = poller.poll_once().await.expect("second poll must succeed");
    assert_eq!(published, 0);
    assert!(rx.try_recv().is_err());

    // 3回目: pending空 + 全件seen済み → 何もpublishしない
    let published = poller.poll_once().await.expect("third poll must succeed");
    assert_eq!(published, 0);
}

#[tokio::test]
async fn poll_active_transitions_with_poll_result() {
    let server = MockServer::start().await;
    let (state, _rx, mut poller) = setup(&server).await;
    assert!(!state.readiness.poll_active.load(Ordering::Relaxed));

    // 初回だけ200(空リスト)、以降500
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(list_body(&[])))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
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

/// フィードのetagが `from` から変わるまで待ち、新しいsnapshot本文を返す。
async fn wait_for_feed_change(state: &SharedState, from: &str) -> String {
    for _ in 0..100 {
        let snapshot = state.feed.load_full();
        if snapshot.etag != from {
            return String::from_utf8(snapshot.body.to_vec()).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("feed snapshot did not change");
}

#[tokio::test]
async fn poll_delivered_id_already_seen_from_ws_is_deduped() {
    let server = MockServer::start().await;
    let (state, rx, mut poller) = setup(&server).await;

    // aggregatorを起動してWS由来 → poll由来のdedupを通しで検証する
    tokio::spawn(aggregator::run(Vec::new(), rx, state.clone()));
    for _ in 0..100 {
        if state.readiness.aggregator_running.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let etag0 = state.feed.load_full().etag.clone();

    // WSから先に同じ電文IDが配信済み
    let ws_meta = ItemMeta {
        id: NEW_ID.into(),
        title: "WS先行配信".into(),
        updated: "2026-07-05T04:11:00+09:00".into(),
        author: "気象庁".into(),
        content: "本文".into(),
    };
    state
        .event_tx
        .send(Event {
            source: EventSource::Dmdata {
                telegram_id: NEW_ID.into(),
                conn: 0,
            },
            dedup_key: DedupKey::TelegramId(NEW_ID.into()),
            xml_body: Bytes::from_static(b"<Report>ws body</Report>"),
            meta: ws_meta,
        })
        .await
        .unwrap();
    let etag1 = {
        wait_for_feed_change(&state, &etag0).await;
        state.feed.load_full().etag.clone()
    };

    // pollが同じ電文IDを配信(タイトルを変えて到達検知できるようにする)
    let mut body = list_body(&[(NEW_ID, "2026-07-05T04:11:00+09:00")]);
    body["items"][0]["xmlReport"]["control"]["title"] = serde_json::json!("poll再送(重複)");
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;
    mount_entity(&server, NEW_ID).await;

    let published = poller.poll_once().await.expect("poll must succeed");
    // pollerはpublishするが、aggregatorが同一TelegramIdとしてdropする
    assert_eq!(published, 1);

    // sentinel: 後続の別イベントが処理された時点で重複イベントは処理済みのはず
    state
        .event_tx
        .send(Event {
            source: EventSource::Dmdata {
                telegram_id: "TELEGRAM_SENTINEL".into(),
                conn: 0,
            },
            dedup_key: DedupKey::TelegramId("TELEGRAM_SENTINEL".into()),
            xml_body: Bytes::from_static(b"<Report>sentinel</Report>"),
            meta: ItemMeta {
                id: "TELEGRAM_SENTINEL".into(),
                title: "番兵".into(),
                updated: "2026-07-05T04:12:00+09:00".into(),
                author: "気象庁".into(),
                content: "本文".into(),
            },
        })
        .await
        .unwrap();
    let feed_body = wait_for_feed_change(&state, &etag1).await;
    assert!(feed_body.contains("番兵"));
    assert!(
        !feed_body.contains("poll再送(重複)"),
        "poll event with ws-seen telegram id must be dropped"
    );
    // pinned本文もWS由来のまま置き換わらない
    let entry = state
        .pinned
        .get(NEW_ID)
        .map(|e| Arc::clone(e.value()))
        .expect("ws entry must stay pinned");
    assert_eq!(&entry.body[..], b"<Report>ws body</Report>");
}
