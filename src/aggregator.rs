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

    // Last-Modified 用時刻の算出は next_last_modified を参照。
    // feed本文の <updated> は従来どおり先頭entryの値のまま。
    let rfc3339 = time::format_description::well_known::Rfc3339;
    let max_updated = slice
        .iter()
        .filter_map(|m| time::OffsetDateTime::parse(&m.updated, &rfc3339).ok())
        .max();
    let previous = state.feed.load();
    let body_changed = previous.body != body;
    let last_modified = next_last_modified(
        max_updated,
        previous.last_modified,
        body_changed,
        time::OffsetDateTime::now_utc(),
    );

    state.feed.store(Arc::new(FeedSnapshot::new(
        body,
        last_updated,
        last_modified,
    )));
}

/// `Last-Modified` 用時刻を決める。
///
/// 基本は「パース可能な updated の最大値」を前回値より小さくならないよう clamp した値
/// (訂正報の ReportDateTime 逆順による後退誤検知を防ぐ)。
///
/// ただし本文が変わったのに、その値が前回値から(IMF-fixdateの秒粒度で)進まない場合がある。
/// 同一発表時刻の電文が続く場合や、古い updated の電文が遅れて届いた場合である。このとき
/// `If-Modified-Since` だけで再検証するクライアントに古い本文のまま 304 を返してしまう。
/// そこで、本文が変わったときは前回値より必ず1秒以上進める。その時刻は現在時刻と
/// 前回値+1秒の遅い方とする。同一秒内に変更が連続すると現在時刻より先行し得るが、
/// 304判定は自サーバが出した値同士の比較なので整合は保たれる。
fn next_last_modified(
    max_updated: Option<time::OffsetDateTime>,
    previous: Option<time::OffsetDateTime>,
    body_changed: bool,
    now: time::OffsetDateTime,
) -> Option<time::OffsetDateTime> {
    let Some(previous) = previous else {
        return max_updated;
    };
    let candidate = max_updated.map_or(previous, |a| a.max(previous));
    if !body_changed || truncate_to_second(candidate) > truncate_to_second(previous) {
        return Some(candidate);
    }
    let bumped = truncate_to_second(previous) + time::Duration::SECOND;
    Some(bumped.max(truncate_to_second(now).to_offset(bumped.offset())))
}

/// IMF-fixdateは秒粒度のため、比較・繰り上げは秒単位で行う。
fn truncate_to_second(dt: time::OffsetDateTime) -> time::OffsetDateTime {
    dt.replace_nanosecond(0)
        .expect("0 is always a valid nanosecond")
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    const NOW: time::OffsetDateTime = datetime!(2026-07-05 04:20:00.5 +09:00);

    #[test]
    fn first_publish_uses_max_updated() {
        let a = datetime!(2026-07-05 04:10:00 +09:00);
        assert_eq!(next_last_modified(Some(a), None, true, NOW), Some(a));
        assert_eq!(next_last_modified(None, None, true, NOW), None);
    }

    #[test]
    fn newer_updated_advances() {
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        let a = datetime!(2026-07-05 04:15:00 +09:00);
        assert_eq!(next_last_modified(Some(a), Some(prev), true, NOW), Some(a));
    }

    #[test]
    fn unchanged_body_keeps_previous() {
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        assert_eq!(
            next_last_modified(Some(prev), Some(prev), false, NOW),
            Some(prev)
        );
    }

    #[test]
    fn changed_body_with_same_updated_bumps_to_now() {
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        assert_eq!(
            next_last_modified(Some(prev), Some(prev), true, NOW),
            Some(datetime!(2026-07-05 04:20:00 +09:00))
        );
    }

    #[test]
    fn changed_body_with_older_updated_bumps() {
        let prev = datetime!(2026-07-05 04:10:00 +09:00);
        let older = datetime!(2026-07-05 04:05:00 +09:00);
        assert_eq!(
            next_last_modified(Some(older), Some(prev), true, NOW),
            Some(datetime!(2026-07-05 04:20:00 +09:00))
        );
    }

    #[test]
    fn bump_is_at_least_one_second_when_previous_is_ahead_of_now() {
        // 同一秒内の連続変更で前回値が現在時刻より先行している場合
        let prev = datetime!(2026-07-05 04:20:03 +09:00);
        assert_eq!(
            next_last_modified(Some(prev), Some(prev), true, NOW),
            Some(datetime!(2026-07-05 04:20:04 +09:00))
        );
    }

    #[test]
    fn subsecond_advance_within_same_second_still_bumps() {
        // IMF-fixdate上で同じ秒になる前進は「進んだ」とみなさない
        let prev = datetime!(2026-07-05 04:20:10.1 +09:00);
        let a = datetime!(2026-07-05 04:20:10.9 +09:00);
        assert_eq!(
            next_last_modified(Some(a), Some(prev), true, NOW),
            Some(datetime!(2026-07-05 04:20:11 +09:00))
        );
    }
}
