//! 唯一の書き込み点となる単一タスク。
//! dedup → entities(moka)更新 → 一覧(VecDeque)更新 → Atom再生成 → ArcSwap store。
//! single-writer のためロック不要。

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use crate::jma::feed_render;
use crate::state::SharedState;
use crate::types::{
    DedupKey, EntityEntry, Event, EventSource, FeedSnapshot, ItemMeta, LatestEntries,
    truncate_to_second,
};

/// Aggregatorタスク本体。`initial_metas` は起動時の初期一覧(新しい順)。
pub async fn run(initial_metas: Vec<ItemMeta>, mut rx: mpsc::Receiver<Event>, state: SharedState) {
    let capacity = state.config.cache.feed_entries;
    let base_url = state.config.http.public_base_url.clone();

    let mut metas: VecDeque<ItemMeta> = initial_metas.into_iter().take(capacity).collect();
    // /latest の保管場所。feedとは独立に保持する(押し出されても新着が来るまで残る)。
    // 起動時に state.latest へseed済みの値(初期一覧に無い種別の補完)に初期一覧を重ねる。
    // 初期一覧由来・補完由来とも実体未取得のため、初回アクセス時にCacheFill経路で埋まる
    let mut latest = LatestEntries::clone(&state.latest.load());
    latest.offer_metas(metas.make_contiguous());
    state.latest.store(Arc::new(latest.clone()));
    publish(&state, &mut metas, &base_url);

    state
        .readiness
        .aggregator_running
        .store(true, Ordering::Relaxed);
    tracing::info!(entries = metas.len(), "aggregator started");

    while let Some(event) = rx.recv().await {
        // dedupは共有Deduperで判定する(書き込みはここのみ、pollerは事前フィルタで読むだけ)
        if !state.deduper.check_and_insert(&event.dedup_key) {
            tracing::debug!(id = %event.meta.id, "duplicate event dropped");
            continue;
        }

        // 実体キャッシュ更新(ETagは事前生成)。poll由来はmeta-only(実体なし)のためNone
        let entry = event
            .xml_body
            .map(|body| Arc::new(EntityEntry::new(body, event.meta.clone())));

        // キャッシュミス補充(CacheFill)由来は一覧を再生成しない。
        // fetch開始時はアローリスト通過済みだがその後evictされた可能性があるため、
        // 権威である自前の `metas` を確認してから格納先を決める(単一writerなので
        // TOCTOUなし): feed在中ならpinnedへ(不変条件「pinnedキー集合 ⊆ feed内ID」)、
        // feed外ならentities(moka)へ。
        if event.source == EventSource::CacheFill {
            // 不変条件: CacheFillのEventはfetch_entityが常に実体を載せる
            let entry = entry.expect("cache fill event must carry a body");
            // /latest が実体未取得で指している電文なら保管場所も埋める
            // (feedから押し出された後でも配信できるように)
            if latest.fill(&event.meta.id, &entry) {
                state.latest.store(Arc::new(latest.clone()));
            }
            if metas.iter().any(|m| m.id == event.meta.id) {
                // or_insert: WS/poll経路で先に新しいbodyがpin済みなら上書きしない
                state.pinned.entry(event.meta.id.clone()).or_insert(entry);
            } else {
                state.entities.insert(event.meta.id.clone(), entry).await;
            }
            tracing::debug!(id = %event.meta.id, "entity cached (feed unchanged)");
            continue;
        }

        // 同一entry idの再着: TelegramIdキーなら同一電文の重複で確定
        // (dmdata電文IDは電文ごとに一意・訂正報は新ID)なのでdrop。
        // seen TTL失効後もリストページに残る古い電文の再publishによる
        // 順序破壊を防ぐ。Compositeキー(CacheFill経路)のみ正当な
        // 「同一entry id・別本文」更新として置換する。CacheFillは上の分岐で
        // continueされ通常ここには到達しないが、防御としてこの分岐を残す。
        if let Some(pos) = metas.iter().position(|m| m.id == event.meta.id) {
            if matches!(event.dedup_key, DedupKey::TelegramId(_)) {
                tracing::debug!(id = %event.meta.id, "stale duplicate entry id dropped");
                continue;
            }
            metas.remove(pos);
        }

        // 対象種別の新着なら /latest を差し替える(publishより前 — feedに載った時点で
        // /latest からも引けるように)。poll由来は実体なしで載り、CacheFillで埋まる
        if latest.offer(&event.meta, entry.as_ref()) {
            state.latest.store(Arc::new(latest.clone()));
        }

        // 実体を持つWS由来はpinnedへ(publishより前 — feedが参照する時点で必ずピン済み)。
        // poll由来はmeta-onlyのため未pinのまま — 実体は初回アクセス時の
        // CacheFill経路(singleflight + rate limiter)で遅延取得されpinへ昇格する。
        // 同一id再送は insert がArcを置換する。feed_ids(アローリスト)は
        // 遅延取得の許可条件になるため実体の有無に関わらず無条件で追加する。
        if let Some(entry) = entry {
            state.pinned.insert(event.meta.id.clone(), entry);
        }
        state.feed_ids.insert(event.meta.id.clone());

        tracing::info!(id = %event.meta.id, title = %event.meta.title, "feed entry added");
        // updated降順の一覧を保つ挿入位置探索。catch-up pollの遅延publish
        // (WSが先に新しい電文を配信済み)でも古いentryが先頭へ飛ばない。
        // 通常のWSフロー(最新が最後に届く)では挿入位置0 = 従来のpush_front。
        // updated は+09:00正規化済みRFC3339なので辞書順比較=時系列比較
        let pos = metas
            .iter()
            .position(|m| m.updated <= event.meta.updated)
            .unwrap_or(metas.len());
        metas.insert(pos, event.meta);
        while metas.len() > capacity {
            if let Some(evicted) = metas.pop_back() {
                if let Some((id, entry)) = state.pinned.remove(&evicted.id) {
                    // feedから外れたdmdata由来entryはmokaへ降格(TTL分の猶予付きで
                    // 配信継続)。未pin(=ウォームアップ由来で実体未取得)は何もしない。
                    // 降格を先に行う — 並行リクエストが「entitiesにもなくアローリスト
                    // からも消えた」瞬間を見ないように。
                    state.entities.insert(id, entry).await;
                }
                // de-allowlistは降格の後(pinned有無問わず無条件で除去)
                state.feed_ids.remove(&evicted.id);
            }
        }

        publish(&state, &mut metas, &base_url);
    }

    state
        .readiness
        .aggregator_running
        .store(false, Ordering::Relaxed);
    tracing::warn!("aggregator stopped (event channel closed)");
}

/// 現在の一覧からAtomを再生成し、スナップショットを差し替える。
fn publish(state: &SharedState, metas: &mut VecDeque<ItemMeta>, base_url: &str) {
    let slice = metas.make_contiguous();
    let body = feed_render::render(slice, base_url);
    let last_updated = slice
        .first()
        .map(|m| m.updated.clone())
        .unwrap_or_else(feed_render::now_jst_rfc3339);

    let previous = state.feed.load();
    if previous.body == body {
        // 本文が同一なら Last-Modified の状態ごと現スナップショットを維持する
        return;
    }

    // Last-Modified 用時刻の算出は next_last_modified を参照。
    // feed本文の <updated> は従来どおり先頭entryの値のまま。
    let rfc3339 = time::format_description::well_known::Rfc3339;
    let max_updated = slice
        .iter()
        .filter_map(|m| time::OffsetDateTime::parse(&m.updated, &rfc3339).ok())
        .max();
    let now = time::OffsetDateTime::now_utc();
    let next = next_last_modified(max_updated, previous.last_modified, now);

    state.feed.store(Arc::new(FeedSnapshot::with_last_modified(
        body,
        last_updated,
        next.map(|(value, _)| value),
        next.is_some_and(|(_, shared)| shared),
    )));
}

/// 本文が変わったときの `Last-Modified` 用時刻と、それを前の本文と共有するかを決める。
/// 値は公開時に確定し、以後変えない。
///
/// - 基本は「パース可能な updated の最大値」を、前回値より小さくならないよう
///   clamp した値(訂正報の ReportDateTime 逆順による後退誤検知を防ぐ)。
/// - Date より未来の値は生成してはならない(RFC 9110 §8.8.2.1)ため `now` で頭打ちにする
///   (dmdataとの時計のずれで updated が未来になる場合も含む)。
/// - 本文が変わったのに前回値から秒粒度で進まない場合(同一発表時刻の電文が続く・
///   古い updated の電文が遅れて届く)、そのままでは `If-Modified-Since` のみで再検証する
///   クライアントに古い本文で304を返してしまう。そこで現在時刻へ進める。
/// - 現在時刻も前回値と同じ秒なら、Dateを超えずに進める手段がない。前回値を共有し、
///   次に時刻が進む本文更新まで日時ベースの304を抑止する([`FeedSnapshot::not_modified_since`])。
///
/// 戻り値は秒単位に切り捨て済み。
fn next_last_modified(
    max_updated: Option<time::OffsetDateTime>,
    previous: Option<time::OffsetDateTime>,
    now: time::OffsetDateTime,
) -> Option<(time::OffsetDateTime, bool)> {
    let now = truncate_to_second(now);
    let capped = max_updated.map(|a| truncate_to_second(a).min(now));
    let Some(previous) = previous.map(truncate_to_second) else {
        return capped.map(|value| (value, false));
    };
    let candidate = capped.map_or(previous, |a| a.max(previous));
    if candidate > previous {
        return Some((candidate, false));
    }
    if now > previous {
        return Some((now, false));
    }
    // 同一秒内の再変更(または時計の後退)。単調性を優先して前回値を共有する
    Some((previous, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    const NOW: time::OffsetDateTime = datetime!(2026-07-05 04:20:00.5 +09:00);

    #[test]
    fn first_publish_uses_max_updated() {
        let a = datetime!(2026-07-05 04:10:00 +09:00);
        assert_eq!(next_last_modified(Some(a), None, NOW), Some((a, false)));
        assert_eq!(next_last_modified(None, None, NOW), None);
    }

    #[test]
    fn newer_updated_advances() {
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        let a = datetime!(2026-07-05 04:15:00 +09:00);
        assert_eq!(
            next_last_modified(Some(a), Some(prev), NOW),
            Some((a, false))
        );
    }

    #[test]
    fn future_updated_is_capped_at_now() {
        let future = datetime!(2026-07-05 04:20:05 +09:00);
        let now = datetime!(2026-07-05 04:20:00 +09:00);
        assert_eq!(
            next_last_modified(Some(future), None, NOW),
            Some((now, false))
        );
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        assert_eq!(
            next_last_modified(Some(future), Some(prev), NOW),
            Some((now, false))
        );
    }

    #[test]
    fn same_updated_advances_to_now() {
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        assert_eq!(
            next_last_modified(Some(prev), Some(prev), NOW),
            Some((datetime!(2026-07-05 04:20:00 +09:00), false))
        );
    }

    #[test]
    fn older_updated_advances_to_now() {
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        let older = datetime!(2026-07-05 04:05:00 +09:00);
        assert_eq!(
            next_last_modified(Some(older), Some(prev), NOW),
            Some((datetime!(2026-07-05 04:20:00 +09:00), false))
        );
    }

    #[test]
    fn change_within_same_second_as_previous_is_shared() {
        // 前回値が現在と同じ秒: Dateを超えて進められないので前回値を共有する
        let prev = datetime!(2026-07-05 04:20:00 +09:00);
        assert_eq!(
            next_last_modified(Some(prev), Some(prev), NOW),
            Some((prev, true))
        );
    }

    #[test]
    fn clock_going_backwards_shares_previous() {
        let prev = datetime!(2026-07-05 04:20:03 +09:00);
        assert_eq!(
            next_last_modified(Some(prev), Some(prev), NOW),
            Some((prev, true))
        );
    }

    #[test]
    fn burst_across_second_boundary_never_confuses_versions() {
        // 02:31:56 内に A→B→C が公開され、02:31:57 に D が公開される。
        // 応答をどの時点で組み立てても Last-Modified は公開時の値のまま。
        let s56 = datetime!(2026-07-05 02:31:56 +09:00);
        let s57 = datetime!(2026-07-05 02:31:57 +09:00);
        let old = Some(datetime!(2026-07-05 02:00:00 +09:00));
        let snap = |(lm, shared): (time::OffsetDateTime, bool)| {
            FeedSnapshot::with_last_modified(bytes::Bytes::new(), String::new(), Some(lm), shared)
        };
        let ims56 = "Sat, 04 Jul 2026 17:31:56 GMT";
        let ims57 = "Sat, 04 Jul 2026 17:31:57 GMT";

        let a = next_last_modified(old, Some(datetime!(2026-07-05 02:30:00 +09:00)), s56).unwrap();
        assert_eq!(a, (s56, false));
        let b = next_last_modified(old, Some(a.0), s56).unwrap();
        assert_eq!(b, (s56, true));
        // B の応答は(組み立てが 02:31:57 にずれても)02:31:56 を送る。
        // C も 02:31:56 を共有するので IMS=02:31:56 では304にならない
        let c = next_last_modified(old, Some(b.0), s56).unwrap();
        assert_eq!(c, (s56, true));
        assert!(!snap(c).not_modified_since(ims56));
        // 次の更新で時刻が進めば共有が解け、日時ベースの304が戻る
        let d = next_last_modified(old, Some(c.0), s57).unwrap();
        assert_eq!(d, (s57, false));
        assert!(!snap(d).not_modified_since(ims56));
        assert!(snap(d).not_modified_since(ims57));
    }

    #[test]
    fn result_never_exceeds_now() {
        let now_s = datetime!(2026-07-05 04:20:00 +09:00);
        let cases = [
            (None, None),
            (Some(datetime!(2026-07-05 05:00:00 +09:00)), None),
            (Some(now_s), Some(now_s)),
            (Some(datetime!(2026-07-05 04:00:00 +09:00)), Some(now_s)),
            (Some(datetime!(2026-07-05 05:00:00 +09:00)), Some(now_s)),
        ];
        for (max_updated, previous) in cases {
            if let Some((value, _)) = next_last_modified(max_updated, previous, NOW) {
                assert!(value <= now_s, "{max_updated:?} {previous:?} -> {value}");
            }
        }
    }
}
