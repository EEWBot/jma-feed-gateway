//! Aggregatorの統合テスト: Event投入 → snapshot / entities への反映を検証。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use figment::Figment;
use figment::providers::{Format, Toml};
use tokio::sync::mpsc;

use jma_relay::aggregator;
use jma_relay::config::{Config, DEFAULT_CONFIG_TOML};
use jma_relay::state::{AppState, SharedState};
use jma_relay::types::{DedupKey, Event, EventSource, ItemMeta};

fn meta(id: &str, title: &str, updated: &str) -> ItemMeta {
    ItemMeta {
        id: id.into(),
        title: title.into(),
        updated: updated.into(),
        author: "気象庁".into(),
        content: format!("{title} の本文"),
        link: String::new(),
    }
}

fn dmdata_event(telegram_id: &str, item: ItemMeta) -> Event {
    Event {
        source: EventSource::Dmdata {
            telegram_id: telegram_id.into(),
            conn: 0,
        },
        dedup_key: DedupKey::TelegramId(telegram_id.into()),
        xml_body: Bytes::from(format!("<Report>{}</Report>", item.id)),
        meta: item,
    }
}

async fn setup(feed_entries: usize, initial: Vec<ItemMeta>) -> (SharedState, mpsc::Sender<Event>) {
    let mut config: Config = Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
        .expect("default config must load");
    config.cache.feed_entries = feed_entries;

    let client = reqwest::Client::new();
    let (tx, rx) = mpsc::channel::<Event>(64);
    let state = Arc::new(AppState::new(Arc::new(config), client, tx.clone()));
    tokio::spawn(aggregator::run(initial, rx, state.clone()));

    // aggregator起動(初期スナップショット生成済み)を待つ
    for _ in 0..100 {
        if state.readiness.aggregator_running.load(Ordering::Relaxed) {
            return (state, tx);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("aggregator did not start");
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
async fn initial_metas_are_published_on_start() {
    let (state, _tx) = setup(
        10,
        vec![meta(
            "id-initial",
            "初期エントリ",
            "2026-07-05T04:00:00+09:00",
        )],
    )
    .await;
    let snapshot = state.feed.load_full();
    let body = String::from_utf8(snapshot.body.to_vec()).unwrap();
    assert!(body.contains("id-initial"));
    assert!(body.contains("初期エントリ"));
    assert_eq!(snapshot.last_updated, "2026-07-05T04:00:00+09:00");
}

#[tokio::test]
async fn dmdata_event_updates_feed_and_is_pinned() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    let item = meta(
        "20260705041000_2_VXSE53_E1",
        "震源・震度に関する情報",
        "2026-07-05T04:10:00+09:00",
    );
    tx.send(dmdata_event("t-1", item)).await.unwrap();

    let body = wait_for_feed_change(&state, &etag0).await;
    assert!(body.contains("20260705041000_2_VXSE53_E1"));
    assert!(body.contains("震源・震度に関する情報"));

    // dmdata由来はpinnedに載る(entitiesではない)
    let entry = state
        .pinned
        .get("20260705041000_2_VXSE53_E1")
        .map(|e| Arc::clone(e.value()))
        .expect("dmdata entry must be pinned");
    assert_eq!(
        &entry.body[..],
        b"<Report>20260705041000_2_VXSE53_E1</Report>"
    );
    assert!(entry.etag.starts_with('"'));
}

#[tokio::test]
async fn duplicate_dedup_key_is_dropped() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    tx.send(dmdata_event(
        "t-dup",
        meta("id-1", "一通目", "2026-07-05T04:10:00+09:00"),
    ))
    .await
    .unwrap();
    let etag1 = {
        wait_for_feed_change(&state, &etag0).await;
        state.feed.load_full().etag.clone()
    };

    // 同じdedupキー(電文ID)で内容を変えて再送 → 反映されない
    tx.send(dmdata_event(
        "t-dup",
        meta("id-2", "二通目(重複)", "2026-07-05T04:11:00+09:00"),
    ))
    .await
    .unwrap();
    // 後続の別イベントが処理された時点で、重複イベントは処理済みのはず
    tx.send(dmdata_event(
        "t-next",
        meta("id-3", "三通目", "2026-07-05T04:12:00+09:00"),
    ))
    .await
    .unwrap();
    let body = wait_for_feed_change(&state, &etag1).await;
    assert!(body.contains("id-3"));
    assert!(
        !body.contains("id-2"),
        "duplicate event must not be published"
    );
    assert!(state.entities.get("id-2").await.is_none());
}

#[tokio::test]
async fn same_entry_id_is_replaced_and_moved_to_front() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    tx.send(dmdata_event(
        "t-a",
        meta("id-a", "更新前", "2026-07-05T04:10:00+09:00"),
    ))
    .await
    .unwrap();
    wait_for_feed_change(&state, &etag0).await;
    let etag1 = state.feed.load_full().etag.clone();

    tx.send(dmdata_event(
        "t-b",
        meta("id-b", "別entry", "2026-07-05T04:11:00+09:00"),
    ))
    .await
    .unwrap();
    wait_for_feed_change(&state, &etag1).await;
    let etag2 = state.feed.load_full().etag.clone();

    // id-a を更新 → 置換され先頭へ、重複entryなし
    // (本文ハッシュdedupeがあるため、更新は実際の内容更新どおり別本文にする)
    let mut event = dmdata_event("t-c", meta("id-a", "更新後", "2026-07-05T04:12:00+09:00"));
    event.xml_body = Bytes::from_static(b"<Report>id-a v2</Report>");
    tx.send(event).await.unwrap();
    let body = wait_for_feed_change(&state, &etag2).await;
    assert!(body.contains("更新後"));
    assert!(!body.contains("更新前"));
    assert_eq!(
        body.matches("id-a.xml").count(),
        2,
        "id + link で2回のみ(1entry)"
    );
    let first_a = body.find("id-a").unwrap();
    let first_b = body.find("id-b").unwrap();
    assert!(first_a < first_b, "updated entry must be at front");
}

#[tokio::test]
async fn feed_is_capped_at_capacity() {
    let (state, tx) = setup(2, Vec::new()).await;
    let mut etag = state.feed.load_full().etag.clone();

    for i in 1..=3 {
        tx.send(dmdata_event(
            &format!("t-{i}"),
            meta(
                &format!("id-{i}"),
                &format!("entry {i}"),
                "2026-07-05T04:10:00+09:00",
            ),
        ))
        .await
        .unwrap();
        let _ = wait_for_feed_change(&state, &etag).await;
        etag = state.feed.load_full().etag.clone();
    }

    let body = String::from_utf8(state.feed.load_full().body.to_vec()).unwrap();
    assert!(body.contains("id-3"));
    assert!(body.contains("id-2"));
    assert!(!body.contains("id-1"), "oldest entry must be evicted");
}

#[tokio::test]
async fn jma_feed_event_caches_entity_without_feed_rebuild() {
    let initial = vec![meta("id-initial", "初期", "2026-07-05T04:00:00+09:00")];
    let (state, tx) = setup(10, initial).await;
    let etag0 = state.feed.load_full().etag.clone();

    let item = meta(
        "ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b",
        "補充された実体",
        "2026-07-05T04:05:00+09:00",
    );
    let body = Bytes::from_static(b"<Report>backfill</Report>");
    tx.send(Event {
        source: EventSource::JmaFeed,
        dedup_key: DedupKey::composite(item.id.clone(), item.updated.clone(), &body),
        xml_body: body,
        meta: item,
    })
    .await
    .unwrap();

    // entitiesには入る
    let mut cached = None;
    for _ in 0..100 {
        if let Some(entry) = state
            .entities
            .get("ca7203bd-93b1-3f3e-b3f0-b6d4be3b7a5b")
            .await
        {
            cached = Some(entry);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let entry = cached.expect("entity must be cached");
    assert_eq!(&entry.body[..], b"<Report>backfill</Report>");

    // フィードは再生成されない
    let snapshot = state.feed.load_full();
    assert_eq!(snapshot.etag, etag0);
    let feed_body = String::from_utf8(snapshot.body.to_vec()).unwrap();
    assert!(!feed_body.contains("補充された実体"));
}

#[tokio::test]
async fn trim_demotes_pinned_entry_to_entities() {
    let (state, tx) = setup(2, Vec::new()).await;
    let mut etag = state.feed.load_full().etag.clone();

    for i in 1..=3 {
        tx.send(dmdata_event(
            &format!("t-{i}"),
            meta(
                &format!("id-{i}"),
                &format!("entry {i}"),
                &format!("2026-07-05T04:1{i}:00+09:00"),
            ),
        ))
        .await
        .unwrap();
        let _ = wait_for_feed_change(&state, &etag).await;
        etag = state.feed.load_full().etag.clone();
    }

    // feedから溢れたid-1はpinnedから外れ、entities(moka)へ降格して配信継続
    assert!(
        state.pinned.get("id-1").is_none(),
        "evicted entry must be unpinned"
    );
    let entry = state
        .entities
        .get("id-1")
        .await
        .expect("evicted entry must be demoted to entities");
    assert_eq!(&entry.body[..], b"<Report>id-1</Report>");
    // 在中の2件はpinnedのまま
    assert!(state.pinned.get("id-2").is_some());
    assert!(state.pinned.get("id-3").is_some());
    assert_eq!(state.pinned.len(), 2);
}

#[tokio::test]
async fn warmup_entries_are_never_pinned() {
    // ウォームアップ(初期一覧)のmetaは実体を持たずpinnedに載らない。
    // trimで溢れても何も起きない(上流307でカバー)
    let initial = vec![
        meta(
            "20260705040000_0_VXSE53_A",
            "warmup-1",
            "2026-07-05T04:00:00+09:00",
        ),
        meta(
            "20260705035900_0_VXSE53_B",
            "warmup-2",
            "2026-07-05T03:59:00+09:00",
        ),
    ];
    let (state, tx) = setup(2, initial).await;
    assert!(state.pinned.is_empty(), "warmup metas must not be pinned");
    let etag = state.feed.load_full().etag.clone();

    // dmdataイベントでwarmup-2が溢れる
    tx.send(dmdata_event(
        "t-new",
        meta("id-new", "新着", "2026-07-05T04:10:00+09:00"),
    ))
    .await
    .unwrap();
    wait_for_feed_change(&state, &etag).await;

    // 溢れたウォームアップ由来IDはpinnedにもentitiesにも入らない
    assert!(state.pinned.get("20260705035900_0_VXSE53_B").is_none());
    assert!(
        state
            .entities
            .get("20260705035900_0_VXSE53_B")
            .await
            .is_none()
    );
    // 新着のみピン済み
    assert_eq!(state.pinned.len(), 1);
    assert!(state.pinned.get("id-new").is_some());
}

#[tokio::test]
async fn same_id_resend_replaces_pin() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    tx.send(dmdata_event(
        "t-first",
        meta("id-same", "更新前", "2026-07-05T04:10:00+09:00"),
    ))
    .await
    .unwrap();
    wait_for_feed_change(&state, &etag0).await;
    let etag1 = state.feed.load_full().etag.clone();

    // 同一entry idの再送(dedupキーは別)→ ピンはArc置換され1件のまま
    let mut item = meta("id-same", "更新後", "2026-07-05T04:12:00+09:00");
    item.content = "更新後の本文".into();
    let mut event = dmdata_event("t-second", item);
    event.xml_body = bytes::Bytes::from_static(b"<Report>updated</Report>");
    tx.send(event).await.unwrap();
    wait_for_feed_change(&state, &etag1).await;

    assert_eq!(state.pinned.len(), 1, "same-id resend must replace the pin");
    let entry = state
        .pinned
        .get("id-same")
        .map(|e| Arc::clone(e.value()))
        .unwrap();
    assert_eq!(&entry.body[..], b"<Report>updated</Report>");
}

fn jma_poll_event(item: ItemMeta, body: &'static [u8]) -> Event {
    Event {
        source: EventSource::JmaPoll,
        dedup_key: DedupKey::composite(item.id.clone(), item.updated.clone(), body),
        xml_body: Bytes::from_static(body),
        meta: item,
    }
}

#[tokio::test]
async fn jma_poll_event_updates_feed_and_is_pinned() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    let item = meta(
        "20260705041000_0_VXSE53_010000",
        "poll由来の電文",
        "2026-07-05T04:10:00+09:00",
    );
    tx.send(jma_poll_event(item, b"<Report>polled</Report>"))
        .await
        .unwrap();

    let body = wait_for_feed_change(&state, &etag0).await;
    assert!(body.contains("20260705041000_0_VXSE53_010000"));
    assert!(body.contains("poll由来の電文"));

    // poll由来もdmdata同様pinnedに載る
    let entry = state
        .pinned
        .get("20260705041000_0_VXSE53_010000")
        .map(|e| Arc::clone(e.value()))
        .expect("polled entry must be pinned");
    assert_eq!(&entry.body[..], b"<Report>polled</Report>");
}

#[tokio::test]
async fn dmdata_event_with_same_body_as_polled_is_dropped() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    // pollで先に配信された電文(実JMA ID)
    let polled = meta(
        "20260705041000_0_VXSE53_010000",
        "poll先行",
        "2026-07-05T04:10:00+09:00",
    );
    tx.send(jma_poll_event(polled, b"<Report>same body</Report>"))
        .await
        .unwrap();
    wait_for_feed_change(&state, &etag0).await;
    let etag1 = state.feed.load_full().etag.clone();

    // WS復帰後に同一本文がdmdataから届く(別telegram_id・別entry ID)→ drop
    let mut event = dmdata_event(
        "t-ws-dup",
        meta(
            "WS_TELEGRAM_ID",
            "WS再送(重複)",
            "2026-07-05T04:10:00+09:00",
        ),
    );
    event.xml_body = Bytes::from_static(b"<Report>same body</Report>");
    tx.send(event).await.unwrap();

    // sentinel: 後続の別イベントが処理された時点で重複イベントは処理済みのはず
    tx.send(dmdata_event(
        "t-sentinel",
        meta("id-sentinel", "番兵", "2026-07-05T04:12:00+09:00"),
    ))
    .await
    .unwrap();
    let body = wait_for_feed_change(&state, &etag1).await;
    assert!(body.contains("id-sentinel"));
    assert!(
        !body.contains("WS_TELEGRAM_ID"),
        "same-body dmdata event must be dropped by cross-source dedupe"
    );
    assert!(state.pinned.get("WS_TELEGRAM_ID").is_none());
}

#[tokio::test]
async fn jma_feed_backfill_does_not_pollute_body_dedupe() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    // キャッシュミス補充(JmaFeed)で本文Bが先に流れる
    let backfill = meta(
        "20260705041000_0_VXSE53_010000",
        "補充",
        "2026-07-05T04:10:00+09:00",
    );
    let body = Bytes::from_static(b"<Report>shared body</Report>");
    tx.send(Event {
        source: EventSource::JmaFeed,
        dedup_key: DedupKey::composite(backfill.id.clone(), backfill.updated.clone(), &body),
        xml_body: body,
        meta: backfill,
    })
    .await
    .unwrap();

    // 補充がentitiesへ入るまで待つ(処理完了の同期)
    for _ in 0..100 {
        if state
            .entities
            .get("20260705041000_0_VXSE53_010000")
            .await
            .is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // 同一本文のdmdataイベントは汚染されずpublishされる
    let mut event = dmdata_event(
        "t-real",
        meta("WS_TELEGRAM_ID", "正規配信", "2026-07-05T04:10:00+09:00"),
    );
    event.xml_body = Bytes::from_static(b"<Report>shared body</Report>");
    tx.send(event).await.unwrap();

    let feed_body = wait_for_feed_change(&state, &etag0).await;
    assert!(
        feed_body.contains("WS_TELEGRAM_ID"),
        "backfill must not pollute publish dedupe"
    );
}

#[tokio::test]
async fn last_modified_is_monotonic_under_out_of_order_updated() {
    let (state, tx) = setup(10, Vec::new()).await;
    let etag0 = state.feed.load_full().etag.clone();

    tx.send(dmdata_event(
        "t-late",
        meta("id-late", "後発", "2026-07-05T04:20:00+09:00"),
    ))
    .await
    .unwrap();
    wait_for_feed_change(&state, &etag0).await;
    let snapshot1 = state.feed.load_full();
    let lm1 = snapshot1.last_modified.expect("last_modified must be set");
    let etag1 = snapshot1.etag.clone();

    // より古いupdatedのイベント(訂正報のReportDateTime逆順を模擬)が先頭に来ても
    // Last-Modified は後退しない
    tx.send(dmdata_event(
        "t-early",
        meta("id-early", "先発(遅延受信)", "2026-07-05T04:15:00+09:00"),
    ))
    .await
    .unwrap();
    let body = wait_for_feed_change(&state, &etag1).await;
    assert!(body.contains("id-early"));

    let snapshot2 = state.feed.load_full();
    let lm2 = snapshot2.last_modified.expect("last_modified must be set");
    assert_eq!(lm2, lm1, "last_modified must not regress");
    // feed本文の<updated>は従来どおり先頭entryの値(=古い方)のまま
    assert_eq!(snapshot2.last_updated, "2026-07-05T04:15:00+09:00");
    // HTTP用文字列も事前計算されている
    assert_eq!(
        snapshot2.last_modified_http.as_deref(),
        Some("Sat, 04 Jul 2026 19:20:00 GMT")
    );
}
