//! Pollerの統合テスト: wiremockでdmdata(telegram.list / telegram.data)を模擬し、
//! poll_once の候補フィルタ / meta-only publish / backlog契約 / readiness遷移 /
//! catch-upを検証する。pollerは実体(telegram.data)を一切fetchしない —
//! 実体は初回HTTPアクセス時にCacheFill経路で遅延取得される。
//! pollerはseen登録しないため、tick間で受信IDを `state.deduper` へ挿入して
//! aggregatorを模擬する。壁時計ループ(run)はテスト対象外
//! (tokio::time::pause は now_utc を動かせない)。

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
use jma_feed_gateway::types::{DedupKey, Event, EventSource, ItemMeta};

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
    let mut config: Config = Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load");
    config.dmdata.api_base = server.uri();
    config.dmdata.data_api_base = format!("{}/v1", server.uri());

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

/// telegram.data(実体)エンドポイントのモックを登録する(遅延取得の検証用に
/// ちょうど1回のfetchを期待する)。
async fn mount_entity_once(server: &MockServer, id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/v1/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(entity_xml(id)))
        .expect(1)
        .mount(server)
        .await;
}

/// 実体が一切fetchされないことを検証するモックを登録する。
async fn mount_entity_never(server: &MockServer, id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/v1/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(entity_xml(id)))
        .expect(0)
        .mount(server)
        .await;
}

/// aggregatorによるseen登録を模擬する(pollerはseen登録しない)。
fn mark_delivered(state: &SharedState, id: &str) {
    state.deduper.insert(DedupKey::TelegramId(id.into()));
}

#[tokio::test]
async fn first_poll_publishes_then_deduper_skips_next_tick() {
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
    mount_entity_never(&server, NEW_ID).await;
    mount_entity_never(&server, OLD_ID).await;

    // 初回poll: 2件ともupdated昇順でmeta-only publish(実体fetchなし)
    let published = poller
        .poll_once(true)
        .await
        .expect("first poll must succeed");
    assert_eq!(published, 2);
    assert!(!poller.has_backlog());

    let first = rx.try_recv().expect("oldest entry must be published first");
    assert_eq!(first.meta.id, OLD_ID);
    assert_eq!(first.source, EventSource::DmdataPoll);
    assert_eq!(first.dedup_key, DedupKey::TelegramId(OLD_ID.into()));
    assert!(first.xml_body.is_none(), "polled event must be meta-only");
    let second = rx
        .try_recv()
        .expect("newest entry must be published second");
    assert_eq!(second.meta.id, NEW_ID);
    assert_eq!(second.source, EventSource::DmdataPoll);
    assert!(rx.try_recv().is_err());

    // aggregatorがseen登録した状況を模擬
    mark_delivered(&state, OLD_ID);
    mark_delivered(&state, NEW_ID);

    // 2回目poll: 同じリストだがdeduper既知 → 何もpublishしない
    let published = poller
        .poll_once(true)
        .await
        .expect("second poll must succeed");
    assert_eq!(published, 0);
    assert!(rx.try_recv().is_err());
    assert!(state.readiness.poll_active.load(Ordering::Relaxed));
}

#[tokio::test]
async fn deduper_known_id_is_not_published() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // OLD_IDはpublish済み(WS/warmup由来を模擬)
    mark_delivered(&state, OLD_ID);

    mount_list(
        &server,
        &[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ],
    )
    .await;
    mount_entity_never(&server, NEW_ID).await;
    mount_entity_never(&server, OLD_ID).await;

    let published = poller.poll_once(true).await.expect("poll must succeed");
    assert_eq!(published, 1);
    let event = rx.try_recv().unwrap();
    assert_eq!(event.meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());
    // MockServer drop時に expect(0) が検証される
}

#[tokio::test]
async fn feed_resident_id_is_not_published() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // OLD_IDはfeed在中(feed_ids)— seen TTL失効後も再publishしない
    state.feed_ids.insert(OLD_ID.into());

    mount_list(
        &server,
        &[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ],
    )
    .await;
    mount_entity_never(&server, NEW_ID).await;
    mount_entity_never(&server, OLD_ID).await;

    let published = poller.poll_once(true).await.expect("poll must succeed");
    assert_eq!(published, 1);
    assert_eq!(rx.try_recv().unwrap().meta.id, NEW_ID);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn backlog_drain_while_ws_connected_does_not_set_poll_active() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // list取得: 初回だけ500、以降200
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_list(&server, &[(NEW_ID, "2026-07-05T04:11:00+09:00")]).await;
    mount_entity_never(&server, NEW_ID).await;

    // WS接続中(fallback=false)のtickでlist失敗 → backlog=true、poll_activeは不変
    state.readiness.ws_connected[0].store(true, Ordering::Relaxed);
    poller
        .poll_once(false)
        .await
        .expect_err("first poll must fail");
    assert!(poller.has_backlog());
    assert!(!state.readiness.poll_active.load(Ordering::Relaxed));

    // WS接続中のbacklog消化 — WSは切断中に発行された電文を再配信しない
    let published = poller
        .poll_once(false)
        .await
        .expect("drain poll must succeed");
    assert_eq!(published, 1);
    assert!(!poller.has_backlog());
    let event = rx.try_recv().unwrap();
    assert_eq!(event.meta.id, NEW_ID);
    assert!(event.xml_body.is_none(), "polled event must be meta-only");
    assert!(
        !state.readiness.poll_active.load(Ordering::Relaxed),
        "backlog drain while ws connected must not set poll_active"
    );
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
    poller
        .poll_once(true)
        .await
        .expect("first poll must succeed");
    assert!(state.readiness.poll_active.load(Ordering::Relaxed));
    assert!(state.readiness.snapshot().poll);

    // 失敗 → poll_active = false + backlog=true(次tickがリトライを担う)
    poller
        .poll_once(true)
        .await
        .expect_err("second poll must fail");
    assert!(!state.readiness.poll_active.load(Ordering::Relaxed));
    assert!(!state.readiness.snapshot().poll);
    assert!(poller.has_backlog());
}

#[tokio::test]
async fn catch_up_after_recovery_publishes_only_missed_entry() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // 切断前にWSが配信済みの電文(aggregatorのseen登録を模擬)
    mark_delivered(&state, OLD_ID);

    // 全断エピソード → 復帰。通知が積まれていること(run loopのselect相当)
    state.readiness.mark_ws_connected(0);
    state.readiness.mark_ws_disconnected(0);
    state.readiness.mark_ws_connected(0);
    tokio::time::timeout(
        Duration::from_millis(10),
        state.readiness.ws_recovered.notified(),
    )
    .await
    .expect("recovery must be notified");

    // 切断中に発行された電文NEWがリストに現れている
    mount_list(
        &server,
        &[
            (NEW_ID, "2026-07-05T04:11:00+09:00"),
            (OLD_ID, "2026-07-05T03:50:00+09:00"),
        ],
    )
    .await;
    mount_entity_never(&server, NEW_ID).await;
    mount_entity_never(&server, OLD_ID).await;

    // catch-up poll(WS接続中なので fallback=false)— meta-onlyでpublish
    let published = poller
        .poll_once(false)
        .await
        .expect("catch-up poll must succeed");
    assert_eq!(published, 1);
    let event = rx.try_recv().unwrap();
    assert_eq!(event.meta.id, NEW_ID);
    assert!(event.xml_body.is_none(), "polled event must be meta-only");
    assert!(rx.try_recv().is_err());
    assert!(
        !state.readiness.poll_active.load(Ordering::Relaxed),
        "catch-up must not set poll_active"
    );
}

#[tokio::test]
async fn catch_up_list_failure_sets_backlog_for_retry() {
    let server = MockServer::start().await;
    let (state, mut rx, mut poller) = setup(&server).await;

    // catch-upのlist取得: 初回だけ500、以降200
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_list(&server, &[(NEW_ID, "2026-07-05T04:11:00+09:00")]).await;
    mount_entity_never(&server, NEW_ID).await;

    state.readiness.ws_connected[0].store(true, Ordering::Relaxed);

    // catch-up失敗 → backlog=true(毎分tickがWS接続中でもリトライする根拠)
    poller
        .poll_once(false)
        .await
        .expect_err("catch-up poll must fail");
    assert!(poller.has_backlog());
    assert!(!state.readiness.poll_active.load(Ordering::Relaxed));

    // 次tick(backlogがあるのでWS接続中でもpoll)で取り逃し分がpublishされる
    let published = poller
        .poll_once(false)
        .await
        .expect("retry poll must succeed");
    assert_eq!(published, 1);
    assert!(!poller.has_backlog());
    let event = rx.try_recv().unwrap();
    assert_eq!(event.meta.id, NEW_ID);
    assert!(event.xml_body.is_none(), "polled event must be meta-only");
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
async fn poll_skips_id_already_delivered_via_ws() {
    let server = MockServer::start().await;
    let (state, rx, mut poller) = setup(&server).await;

    // aggregatorを起動してWS由来 → poll候補フィルタを通しで検証する
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
            xml_body: Some(Bytes::from_static(b"<Report>ws body</Report>")),
            meta: ws_meta,
        })
        .await
        .unwrap();
    wait_for_feed_change(&state, &etag0).await;

    // pollは共有deduper(aggregatorが登録済み)の事前フィルタでskipし、
    // 実体fetchもpublishもしない
    let mut body = list_body(&[(NEW_ID, "2026-07-05T04:11:00+09:00")]);
    body["items"][0]["xmlReport"]["control"]["title"] = serde_json::json!("poll再送(重複)");
    Mock::given(method("GET"))
        .and(path(LIST_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;
    mount_entity_never(&server, NEW_ID).await;

    let published = poller.poll_once(true).await.expect("poll must succeed");
    assert_eq!(published, 0);

    // pinned本文もWS由来のまま置き換わらない
    let entry = state
        .pinned
        .get(NEW_ID)
        .map(|e| Arc::clone(e.value()))
        .expect("ws entry must stay pinned");
    assert_eq!(&entry.body[..], b"<Report>ws body</Report>");
}

#[tokio::test]
async fn polled_entry_is_meta_only_until_first_access() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let server = MockServer::start().await;
    let (state, rx, mut poller) = setup(&server).await;

    // aggregatorを起動してpoll publish → feed反映 → 遅延取得まで通しで検証する
    tokio::spawn(aggregator::run(Vec::new(), rx, state.clone()));
    for _ in 0..100 {
        if state.readiness.aggregator_running.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let etag0 = state.feed.load_full().etag.clone();

    // poll中は実体エンドポイントを一切mountしない = pollerがfetchすれば失敗する
    mount_list(&server, &[(NEW_ID, "2026-07-05T04:11:00+09:00")]).await;
    let published = poller.poll_once(true).await.expect("poll must succeed");
    assert_eq!(published, 1);

    // feedへ反映されるが、meta-only: アローリスト在中・未pin
    let feed_body = wait_for_feed_change(&state, &etag0).await;
    assert!(feed_body.contains(NEW_ID));
    assert!(state.feed_ids.contains(NEW_ID));
    assert!(
        state.pinned.get(NEW_ID).is_none(),
        "polled entry must not be pinned before first access"
    );

    // 初回HTTPアクセス: CacheFill経路でちょうど1回だけ実体をfetchして200
    mount_entity_once(&server, NEW_ID).await;
    let router = jma_feed_gateway::http::build_router(state.clone());
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!("/developer/xml/data/{NEW_ID}.xml"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], entity_xml(NEW_ID).as_bytes());

    // CacheFill Eventがaggregatorに処理され、feed在中IDとしてpinnedへ昇格する
    let mut pinned = None;
    for _ in 0..100 {
        if let Some(entry) = state.pinned.get(NEW_ID).map(|e| Arc::clone(e.value())) {
            pinned = Some(entry);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let entry = pinned.expect("polled entry must be pinned after first access");
    assert_eq!(&entry.body[..], entity_xml(NEW_ID).as_bytes());
    // MockServer drop時に expect(1) が検証される(poller由来のfetchゼロの証明)
}
