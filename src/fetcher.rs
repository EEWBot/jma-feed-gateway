//! 初期一覧取得(dmdata telegram.list)+ キャッシュミス時のバックグラウンド実体取得
//! (telegram.data v1 / singleflight)。

use std::cmp::min;
use std::time::Duration;

use crate::config::Config;
use crate::dmdata::api::{DmdataApi, TelegramListItem, TelegramListResponse};
use crate::error::DmdataError;
use crate::jma::entity_parse;
use crate::state::{InflightTx, SharedState};
use crate::types::{
    DedupKey, EntityEntry, Event, EventSource, ItemMeta, LATEST_EARTHQUAKE_TYPES,
    LATEST_TSUNAMI_TYPES, LatestEntries, normalize_rfc3339_to_jst,
};

const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// ウォームアップで辿る最大ページ数(安全弁)。
const MAX_WARMUP_PAGES: usize = 10;
/// telegram.list の1ページあたり取得件数(APIの上限値)。poller も共有する。
pub(crate) const LIST_PAGE_LIMIT: usize = 100;
/// `/latest` 補完で telegram.list(type絞り込み)から取得する件数。
/// テスト電文・非XMLのskipや訂正報の前後を吸収できれば十分。
const LATEST_FALLBACK_LIMIT: usize = 20;

/// dmdata telegram.list をページングして初期一覧を取得する。
/// `cache.feed_entries` 件集まるか、`nextToken` 枯渇か、ページ上限で停止する。
/// 各ページはリトライ付きで取得し、リトライが尽きたら致命(Err)。
/// HTTP公開前に完了必須。実体のプリフェッチは行わない。
pub async fn load_initial_feed(
    api: &DmdataApi,
    config: &Config,
) -> Result<Vec<ItemMeta>, DmdataError> {
    let classification = config.dmdata.classifications.join(",");
    let target = config.cache.feed_entries;

    let mut items: Vec<ItemMeta> = Vec::with_capacity(target);
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    let mut scanned = 0usize;

    loop {
        pages += 1;
        let page =
            list_page_with_retry(api, config, &classification, cursor.as_deref(), pages).await?;
        scanned += page.items.len();
        items.extend(
            page.items
                .iter()
                .filter_map(|item| select_item(item, &config.dmdata.types)),
        );
        cursor = page.next_token;
        if items.len() >= target || cursor.is_none() || pages >= MAX_WARMUP_PAGES {
            break;
        }
    }

    // updated は select_item で+09:00へ正規化済みのRFC3339なので辞書順比較=時系列比較
    items.sort_by(|a, b| b.updated.cmp(&a.updated));
    items.truncate(target);

    tracing::info!(
        pages,
        scanned,
        after_filter = items.len(),
        "initial feeds loaded"
    );
    Ok(items)
}

/// `/latest` の初期値を作る。初期一覧に対象種別が無い場合のみ、telegram.list を
/// 電文種別で絞って最新電文のIDを補完する(メタデータのみ。実体は初回アクセス時に
/// CacheFill経路で遅延取得する)。補完の失敗は致命にしない(新着が来るまで404のまま)。
pub async fn load_initial_latest(
    api: &DmdataApi,
    config: &Config,
    initial_metas: &[ItemMeta],
) -> LatestEntries {
    let mut latest = LatestEntries::from_metas(initial_metas);
    if latest.earthquake.is_none()
        && let Some(meta) = find_latest_by_types(api, config, LATEST_EARTHQUAKE_TYPES).await
    {
        latest.offer(&meta, None);
    }
    if latest.tsunami.is_none()
        && let Some(meta) = find_latest_by_types(api, config, LATEST_TSUNAMI_TYPES).await
    {
        latest.offer(&meta, None);
    }
    latest
}

/// telegram.list(type絞り込み)を1ページ取得し、updated最大の電文を返す。
/// `dmdata.types` で受信対象外にした種別は問い合わせない(WSで更新されず古いまま残るため)。
async fn find_latest_by_types(
    api: &DmdataApi,
    config: &Config,
    latest_types: &[&str],
) -> Option<ItemMeta> {
    let types: Vec<String> = latest_types
        .iter()
        .filter(|t| type_matches(t, &config.dmdata.types))
        .map(|t| t.to_string())
        .collect();
    if types.is_empty() {
        return None;
    }
    let joined = types.join(",");
    let page = match api
        .telegram_list_by_types(&joined, LATEST_FALLBACK_LIMIT)
        .await
    {
        Ok(page) => page,
        Err(e) => {
            tracing::warn!(error = %e, types = %joined, "latest fallback lookup failed");
            return None;
        }
    };
    // 一覧は受信順(新しい順)のため、ReportDateTime基準のupdatedで最大を取り直す
    let found = page
        .items
        .iter()
        .filter_map(|item| select_item(item, &types))
        .max_by(|a, b| a.updated.cmp(&b.updated));
    match &found {
        Some(meta) => tracing::info!(types = %joined, id = %meta.id, "latest fallback found"),
        None => tracing::info!(types = %joined, "latest fallback found nothing"),
    }
    found
}

/// telegram.list を1ページ、リトライ付きで取得する。
/// attempts = retry_attempts.max(1)、backoffは初期値から倍々(上限 MAX_BACKOFF)。
async fn list_page_with_retry(
    api: &DmdataApi,
    config: &Config,
    classification: &str,
    cursor_token: Option<&str>,
    page: usize,
) -> Result<TelegramListResponse, DmdataError> {
    let attempts = config.dmdata.retry_attempts.max(1);
    let mut backoff = Duration::from_millis(config.dmdata.retry_initial_backoff_ms);
    let mut last_err = None;

    for attempt in 1..=attempts {
        match api
            .telegram_list(classification, cursor_token, LIST_PAGE_LIMIT)
            .await
        {
            Ok(response) => {
                tracing::info!(
                    entries = response.items.len(),
                    attempt,
                    page,
                    "warmup page loaded"
                );
                return Ok(response);
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, max = attempts, page, "warmup page fetch failed");
                last_err = Some(e);
                if attempt < attempts {
                    tokio::time::sleep(backoff).await;
                    backoff = min(backoff.saturating_mul(2), MAX_BACKOFF);
                }
            }
        }
    }
    Err(last_err.expect("at least one attempt was made"))
}

/// 電文種別コードがフィルタを通過するか。空リストは全通過、大小文字無視。
pub(crate) fn type_matches(telegram_type: &str, types: &[String]) -> bool {
    types.is_empty() || types.iter().any(|t| t.eq_ignore_ascii_case(telegram_type))
}

/// telegram.list item のスキップ判定 + ItemMeta マッピング。poller も共有する。
/// スキップ条件: テスト電文 / format != "xml" / id空 / typeフィルタ不一致。
/// マッピングは ws.rs build_event のJSONフォールバック順に合わせる:
/// - title: xmlReport.control.title → head.type
/// - updated: xmlReport.head.reportDateTime → head.time → receivedTime
///   (フォールバック値はZ表記UTCが混ざるため+09:00へ正規化する)
/// - author: xmlReport.control.publishingOffice、content: xmlReport.head.headline
pub(crate) fn select_item(item: &TelegramListItem, types: &[String]) -> Option<ItemMeta> {
    if item.head.test {
        tracing::debug!(id = %item.id, "test telegram skipped");
        return None;
    }
    if item.format.as_deref() != Some("xml") {
        tracing::debug!(id = %item.id, format = ?item.format, "non-xml telegram skipped");
        return None;
    }
    if item.id.is_empty() {
        tracing::debug!("telegram without id skipped");
        return None;
    }
    if !type_matches(&item.head.telegram_type, types) {
        return None;
    }

    let report = item.xml_report.clone().unwrap_or_default();
    let control = report.control.unwrap_or_default();
    let xml_head = report.head.unwrap_or_default();

    let title = control
        .title
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| item.head.telegram_type.clone());
    let updated = xml_head
        .report_date_time
        .filter(|u| !u.is_empty())
        .or_else(|| item.head.time.clone().filter(|u| !u.is_empty()))
        .or_else(|| item.received_time.clone())
        .unwrap_or_default();
    // オフセット混在で辞書順ソートが壊れないよう、ここで+09:00に統一する
    let updated = normalize_rfc3339_to_jst(&updated);

    Some(ItemMeta {
        id: item.id.clone(),
        title,
        updated,
        author: control.publishing_office.unwrap_or_default(),
        content: xml_head.headline.unwrap_or_default(),
        telegram_type: item.head.telegram_type.clone(),
    })
}

/// inflight マップから必ず remove するための Drop ガード。
/// panic 時も含めあらゆる経路で singleflight を解除する。
struct InflightGuard {
    state: SharedState,
    id: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.state.inflight.remove(&self.id);
    }
}

/// キャッシュミスした実体XMLをdmdata telegram.data v1から取得する。
/// 呼び出し側(dataハンドラ)が `state.inflight` に watch Receiver を登録して
/// 先着ガードを取得済みであることが前提。完了・失敗いずれでも必ずガードを解除する。
///
/// 成功時は完成entryを `result_tx` でsendし、待機中のハンドラへ直接配る。
/// 失敗時はsendせずreturnし、Sender dropが失敗シグナルとなる。
/// 取得結果は直接mokaに入れず、メタ抽出(Control/Head)のうえ
/// `Event { source: CacheFill, .. }` として mpsc で aggregator へ送る(single-writer 維持)。
/// CacheFill 由来のEventは entities 挿入のみで一覧は再生成されない。
pub async fn fetch_entity(state: SharedState, id: String, result_tx: InflightTx) {
    let _guard = InflightGuard {
        state: state.clone(),
        id: id.clone(),
    };

    let body = match state.dmdata_api.telegram_get(&id).await {
        Ok(body) => body,
        Err(e) => {
            tracing::warn!(error = %e, id = %id, "entity fetch failed");
            return;
        }
    };

    // メタ抽出(失敗しても本文はキャッシュ対象とする)
    let entity_meta = std::str::from_utf8(&body)
        .ok()
        .and_then(|xml| entity_parse::parse_entity_meta(xml).ok())
        .unwrap_or_default();
    let meta = ItemMeta {
        id: id.clone(),
        title: entity_meta.title,
        updated: entity_meta.report_date_time,
        author: entity_meta.publishing_office,
        content: entity_meta.headline_text,
        // 実体XMLは電文種別コードを持たない。CacheFillは一覧(metas)を更新しないため
        // /latest の判定には影響しない
        telegram_type: String::new(),
    };

    // 待機中のハンドラへ完成entryを即配布する(Bytes cloneは安価)。
    // mokaへの格納は従来どおりEvent経由でaggregatorが行う(single-writer維持)
    let entry = std::sync::Arc::new(EntityEntry::new(body.clone(), meta.clone()));
    let _ = result_tx.send(Some(entry)); // 待機者ゼロなら送信失敗でよい

    let event = Event {
        source: EventSource::CacheFill,
        dedup_key: DedupKey::composite(id.clone(), meta.updated.clone(), &body),
        xml_body: Some(body),
        meta,
    };
    if state.event_tx.send(event).await.is_err() {
        tracing::warn!(id = %id, "aggregator channel closed; fetched entity dropped");
    } else {
        tracing::debug!(id = %id, "entity event sent to aggregator");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_CONFIG_TOML;
    use crate::dmdata::protocol::DataHead;
    use figment::Figment;
    use figment::providers::{Format, Toml};
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn codes() -> Vec<String> {
        ["VXSE51", "VXSE52", "VXSE53"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn type_matches_empty_list_passes_everything() {
        assert!(type_matches("VXSE53", &[]));
        assert!(type_matches("", &[]));
    }

    #[test]
    fn type_matches_is_case_insensitive() {
        let types = vec!["vxse53".to_string()];
        assert!(type_matches("VXSE53", &types));
        assert!(type_matches("vxse53", &codes()));
    }

    #[test]
    fn type_matches_rejects_unlisted_type() {
        assert!(!type_matches("VXSE41", &codes()));
        assert!(!type_matches("", &codes()));
    }

    /// テスト用の telegram.list item(xmlReport付きの正常形)。
    fn list_item(id: &str) -> TelegramListItem {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "head": {"type": "VXSE53", "test": false, "time": "2026-07-08T01:00:00Z"},
            "receivedTime": "2026-07-08T10:00:05+09:00",
            "xmlReport": {
                "control": {"title": "震源・震度情報", "publishingOffice": "気象庁本庁"},
                "head": {
                    "reportDateTime": "2026-07-08T10:00:00+09:00",
                    "headline": "地震がありました。"
                }
            },
            "format": "xml"
        }))
        .expect("item must deserialize")
    }

    #[test]
    fn select_item_maps_from_xml_report() {
        let meta = select_item(&list_item("ID1"), &codes()).expect("must pass");
        assert_eq!(meta.id, "ID1");
        assert_eq!(meta.title, "震源・震度情報");
        assert_eq!(meta.updated, "2026-07-08T10:00:00+09:00");
        assert_eq!(meta.author, "気象庁本庁");
        assert_eq!(meta.content, "地震がありました。");
        assert_eq!(meta.telegram_type, "VXSE53");
    }

    #[test]
    fn select_item_falls_back_without_xml_report() {
        let mut item = list_item("ID1");
        item.xml_report = None;
        let meta = select_item(&item, &codes()).expect("must pass");
        // title は head.type、updated は head.time へフォールバック(+09:00へ正規化)
        assert_eq!(meta.title, "VXSE53");
        assert_eq!(meta.updated, "2026-07-08T10:00:00+09:00");
        assert!(meta.author.is_empty());
        assert!(meta.content.is_empty());
    }

    #[test]
    fn select_item_falls_back_to_received_time() {
        let mut item = list_item("ID1");
        item.xml_report = None;
        item.head = DataHead {
            telegram_type: "VXSE53".into(),
            author: None,
            time: None,
            test: false,
            xml: true,
        };
        let meta = select_item(&item, &codes()).expect("must pass");
        assert_eq!(meta.updated, "2026-07-08T10:00:05+09:00");
    }

    #[test]
    fn select_item_skips_test_format_empty_id_and_type_mismatch() {
        let mut test_item = list_item("ID1");
        test_item.head.test = true;
        assert!(select_item(&test_item, &codes()).is_none());

        let mut json_item = list_item("ID2");
        json_item.format = Some("json".into());
        assert!(select_item(&json_item, &codes()).is_none());
        let mut no_format = list_item("ID2");
        no_format.format = None;
        assert!(select_item(&no_format, &codes()).is_none());

        assert!(select_item(&list_item(""), &codes()).is_none());

        let mut wrong_type = list_item("ID3");
        wrong_type.head.telegram_type = "VXSE41".into();
        assert!(select_item(&wrong_type, &codes()).is_none());
        // 空リストなら全type通過
        assert!(select_item(&wrong_type, &[]).is_some());
    }

    #[test]
    fn select_item_normalizes_mixed_offsets_for_sorting() {
        // Z表記フォールバック(head.time)の item と +09:00 の item が混在しても、
        // 正規化後は辞書順ソート=時系列になる
        let mut z_item = list_item("ID_Z");
        z_item.xml_report = None;
        z_item.head.time = Some("2026-07-08T01:30:00Z".into()); // 10:30 JST

        let jst_item = list_item("ID_JST"); // reportDateTime = 10:00 JST

        let mut metas: Vec<ItemMeta> = [&jst_item, &z_item]
            .iter()
            .map(|i| select_item(i, &codes()).expect("must pass"))
            .collect();
        // load_initial_feed と同じ降順ソート
        metas.sort_by(|a, b| b.updated.cmp(&a.updated));
        let ids: Vec<&str> = metas.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["ID_Z", "ID_JST"]);
        assert_eq!(metas[0].updated, "2026-07-08T10:30:00+09:00");
    }

    /// テスト用の Config(api_base をモックサーバへ向け、リトライを高速化)。
    fn test_config(server: &MockServer, mutate: impl FnOnce(&mut Config)) -> Config {
        let mut config: Config =
            Config::from_figment(Figment::from(Toml::string(DEFAULT_CONFIG_TOML)))
                .expect("default config must load");
        config.dmdata.api_base = server.uri();
        config.dmdata.data_api_base = format!("{}/v1", server.uri());
        config.dmdata.retry_initial_backoff_ms = 1;
        mutate(&mut config);
        config
    }

    fn api(config: &Config) -> DmdataApi {
        DmdataApi::new(
            reqwest::Client::new(),
            config.dmdata.api_base.clone(),
            config.dmdata.data_api_base.clone(),
            "test-api-key",
            None,
        )
    }

    /// telegram.list レスポンスJSONを組み立てる(items は (id, reportDateTime))。
    fn list_body(items: &[(&str, &str)], next_token: Option<&str>) -> serde_json::Value {
        let items: Vec<serde_json::Value> = items
            .iter()
            .map(|(id, updated)| {
                serde_json::json!({
                    "id": id,
                    "head": {"type": "VXSE53", "test": false},
                    "receivedTime": updated,
                    "xmlReport": {
                        "control": {"title": "震源・震度情報", "publishingOffice": "気象庁本庁"},
                        "head": {"reportDateTime": updated, "headline": "本文"}
                    },
                    "format": "xml"
                })
            })
            .collect();
        serde_json::json!({"status": "ok", "items": items, "nextToken": next_token})
    }

    #[tokio::test]
    async fn warmup_pages_until_enough_entries_and_sorts_descending() {
        let server = MockServer::start().await;
        let config = test_config(&server, |c| c.cache.feed_entries = 3);

        // 1ページ目(cursorToken無し): 2件 + nextToken
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .and(query_param_is_missing("cursorToken"))
            .and(query_param("classification", "telegram.earthquake"))
            .and(query_param("limit", "100"))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(
                &[
                    ("ID_B", "2026-07-08T10:00:00+09:00"),
                    ("ID_D", "2026-07-08T08:00:00+09:00"),
                ],
                Some("TOKEN_P2"),
            )))
            .expect(1)
            .mount(&server)
            .await;
        // 2ページ目: nextToken を cursorToken にエコーして続き取得
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .and(query_param("cursorToken", "TOKEN_P2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(
                &[
                    ("ID_A", "2026-07-08T11:00:00+09:00"),
                    ("ID_C", "2026-07-08T09:00:00+09:00"),
                ],
                Some("TOKEN_P3"),
            )))
            .expect(1)
            .mount(&server)
            .await;

        let items = load_initial_feed(&api(&config), &config)
            .await
            .expect("warmup must succeed");
        // feed_entries=3 で停止(3ページ目は取得しない)、updated降順で切り詰め
        let ids: Vec<&str> = items.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["ID_A", "ID_B", "ID_C"]);
    }

    #[tokio::test]
    async fn warmup_stops_at_next_token_exhaustion() {
        let server = MockServer::start().await;
        let config = test_config(&server, |_| {});

        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(list_body(&[("ID_1", "2026-07-08T10:00:00+09:00")], None)),
            )
            .expect(1)
            .mount(&server)
            .await;

        let items = load_initial_feed(&api(&config), &config)
            .await
            .expect("warmup must succeed");
        assert_eq!(items.len(), 1);
    }

    #[tokio::test]
    async fn warmup_stops_at_page_cap() {
        let server = MockServer::start().await;
        let config = test_config(&server, |_| {});

        // 常に nextToken を返し続ける(1ページ1件 < feed_entries=100)
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(
                &[("ID_LOOP", "2026-07-08T10:00:00+09:00")],
                Some("TOKEN_MORE"),
            )))
            .expect(10)
            .mount(&server)
            .await;

        let items = load_initial_feed(&api(&config), &config)
            .await
            .expect("warmup must succeed");
        // MAX_WARMUP_PAGES=10 で停止(MockServer drop時に expect(10) が検証される)
        assert_eq!(items.len(), 10);
    }

    #[tokio::test]
    async fn warmup_filters_test_format_and_type() {
        let server = MockServer::start().await;
        let config = test_config(&server, |c| c.dmdata.types = vec!["VXSE53".into()]);

        let mut body = list_body(
            &[
                ("ID_OK", "2026-07-08T10:00:00+09:00"),
                ("ID_TEST", "2026-07-08T09:00:00+09:00"),
                ("ID_JSON", "2026-07-08T08:00:00+09:00"),
                ("ID_TYPE", "2026-07-08T07:00:00+09:00"),
            ],
            None,
        );
        body["items"][1]["head"]["test"] = serde_json::json!(true);
        body["items"][2]["format"] = serde_json::json!("json");
        body["items"][3]["head"]["type"] = serde_json::json!("VXSE41");

        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let items = load_initial_feed(&api(&config), &config)
            .await
            .expect("warmup must succeed");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, "ID_OK");
    }

    #[tokio::test]
    async fn warmup_retries_then_succeeds() {
        let server = MockServer::start().await;
        let config = test_config(&server, |_| {});

        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(2)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(list_body(&[("ID_1", "2026-07-08T10:00:00+09:00")], None)),
            )
            .mount(&server)
            .await;

        let items = load_initial_feed(&api(&config), &config)
            .await
            .expect("warmup must succeed after retries");
        assert_eq!(items.len(), 1);
    }

    #[tokio::test]
    async fn warmup_fails_after_retries_exhausted() {
        let server = MockServer::start().await;
        let config = test_config(&server, |c| c.dmdata.retry_attempts = 2);

        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(ResponseTemplate::new(500))
            .expect(2)
            .mount(&server)
            .await;

        let error = load_initial_feed(&api(&config), &config)
            .await
            .expect_err("warmup must fail after retries");
        assert!(error.to_string().contains("500"), "unexpected: {error}");
    }

    fn typed_meta(id: &str, telegram_type: &str, updated: &str) -> ItemMeta {
        ItemMeta {
            id: id.into(),
            telegram_type: telegram_type.into(),
            updated: updated.into(),
            ..ItemMeta::default()
        }
    }

    fn latest_id(latest: &Option<crate::types::LatestEntry>) -> Option<&str> {
        latest.as_ref().map(|l| l.meta.id.as_str())
    }

    #[tokio::test]
    async fn initial_latest_skips_lookup_when_initial_feed_has_both() {
        let server = MockServer::start().await;
        let config = test_config(&server, |_| {});
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let initial = [
            typed_meta("ID_TS", "VTSE41", "2026-07-08T10:10:00+09:00"),
            typed_meta("ID_EQ", "VXSE53", "2026-07-08T10:00:00+09:00"),
        ];
        let latest = load_initial_latest(&api(&config), &config, &initial).await;
        assert_eq!(latest_id(&latest.earthquake), Some("ID_EQ"));
        assert_eq!(latest_id(&latest.tsunami), Some("ID_TS"));
    }

    #[tokio::test]
    async fn initial_latest_looks_up_missing_type_by_report_time() {
        let server = MockServer::start().await;
        let config = test_config(&server, |_| {});

        // 受信順(新しい順)。テスト電文はskip、ReportDateTime最大の電文を採る
        let mut body = list_body(
            &[
                ("ID_TEST", "2026-07-08T09:00:00+09:00"),
                ("ID_OLDER", "2026-07-08T07:00:00+09:00"),
                ("ID_NEWEST", "2026-07-08T08:00:00+09:00"),
            ],
            None,
        );
        for item in body["items"].as_array_mut().unwrap() {
            item["head"]["type"] = serde_json::json!("VTSE41");
        }
        body["items"][0]["head"]["test"] = serde_json::json!(true);
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .and(query_param("type", "VTSE41"))
            .and(query_param_is_missing("classification"))
            .and(query_param("limit", "20"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;

        let initial = [typed_meta("ID_EQ", "VXSE51", "2026-07-08T10:00:00+09:00")];
        let latest = load_initial_latest(&api(&config), &config, &initial).await;
        assert_eq!(latest_id(&latest.earthquake), Some("ID_EQ"));
        let tsunami = latest.tsunami.expect("must be filled by lookup");
        assert_eq!(tsunami.meta.id, "ID_NEWEST");
        assert_eq!(tsunami.meta.telegram_type, "VTSE41");
        // 実体は遅延取得
        assert!(tsunami.entry.is_none());
    }

    #[tokio::test]
    async fn initial_latest_queries_earthquake_types_exactly() {
        let server = MockServer::start().await;
        let config = test_config(&server, |_| {});
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .and(query_param("type", "VXSE51,VXSE52,VXSE53"))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(
                &[("ID_EQ_FALLBACK", "2026-07-08T08:00:00+09:00")],
                None,
            )))
            .expect(1)
            .mount(&server)
            .await;

        let initial = [typed_meta("ID_TS", "VTSE41", "2026-07-08T10:00:00+09:00")];
        let latest = load_initial_latest(&api(&config), &config, &initial).await;
        assert_eq!(latest_id(&latest.earthquake), Some("ID_EQ_FALLBACK"));
        assert_eq!(latest_id(&latest.tsunami), Some("ID_TS"));
    }

    #[tokio::test]
    async fn initial_latest_lookup_failure_is_not_fatal() {
        let server = MockServer::start().await;
        let config = test_config(&server, |_| {});
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .respond_with(ResponseTemplate::new(500))
            .expect(2)
            .mount(&server)
            .await;

        let latest = load_initial_latest(&api(&config), &config, &[]).await;
        assert!(latest.earthquake.is_none());
        assert!(latest.tsunami.is_none());
    }

    #[tokio::test]
    async fn initial_latest_skips_types_excluded_by_config() {
        let server = MockServer::start().await;
        // VTSE41 は受信対象外、地震はVXSE53のみ受信
        let config = test_config(&server, |c| c.dmdata.types = vec!["VXSE53".into()]);
        Mock::given(method("GET"))
            .and(path("/telegram"))
            .and(query_param("type", "VXSE53"))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(&[], None)))
            .expect(1)
            .mount(&server)
            .await;

        let latest = load_initial_latest(&api(&config), &config, &[]).await;
        assert!(latest.earthquake.is_none());
        assert!(latest.tsunami.is_none());
        // MockServer drop 時に expect(1)(=津波は問い合わせない)が検証される
    }
}
