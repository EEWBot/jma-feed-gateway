//! 唯一の書き込み点となる単一タスク。
//! dedup → entities(moka)更新 → 一覧(VecDeque)更新 → Atom再生成 → ArcSwap store。
//! single-writer のためロック不要。

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use crate::jma::feed_render;
use crate::state::SharedState;
use crate::types::{DedupKey, EntityEntry, Event, EventSource, FeedSnapshot, ItemMeta};

/// Aggregatorタスク本体。`initial_metas` は起動時の初期一覧(新しい順)。
pub async fn run(initial_metas: Vec<ItemMeta>, mut rx: mpsc::Receiver<Event>, state: SharedState) {
    let capacity = state.config.cache.feed_entries;
    let base_url = state.config.http.public_base_url.clone();

    let mut metas: VecDeque<ItemMeta> = initial_metas.into_iter().take(capacity).collect();
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

        // 実体キャッシュ更新(ETagは事前生成)
        let entry = Arc::new(EntityEntry::new(event.xml_body.clone(), event.meta.clone()));

        // キャッシュミス補充(CacheFill)由来はentities挿入のみで一覧は再生成しない
        if event.source == EventSource::CacheFill {
            state.entities.insert(event.meta.id.clone(), entry).await;
            tracing::debug!(id = %event.meta.id, "entity cached (feed unchanged)");
            continue;
        }

        // 同一entry idの再着: TelegramIdキーなら同一電文の重複で確定
        // (dmdata電文IDは電文ごとに一意・訂正報は新ID)なのでdrop。
        // seen TTL失効後もリストページに残る古い電文の再publishによる
        // 順序破壊を防ぐ。Compositeキー(WS空IDフォールバックの合成ID経路)
        // のみ正当な「同一entry id・別本文」更新として置換する。
        if let Some(pos) = metas.iter().position(|m| m.id == event.meta.id) {
            if matches!(event.dedup_key, DedupKey::TelegramId(_)) {
                tracing::debug!(id = %event.meta.id, "stale duplicate entry id dropped");
                continue;
            }
            metas.remove(pos);
        }

        // dmdata/poll由来はpinnedへ(publishより前 — feedが参照する時点で必ずピン済み)。
        // 同一id再送は insert がArcを置換する。
        state.pinned.insert(event.meta.id.clone(), entry);

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
            if let Some(evicted) = metas.pop_back()
                && let Some((id, entry)) = state.pinned.remove(&evicted.id)
            {
                // feedから外れたdmdata由来entryはmokaへ降格(TTL分の猶予付きで配信継続)。
                // None(=ウォームアップ由来で実体未取得)はミス時のCacheFill補充
                // (dmdata telegram.data)でカバーされるため何もしない。
                state.entities.insert(id, entry).await;
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

    // Last-Modified 用時刻: パース可能な updated の最大値を取り、前スナップショットより
    // 小さくならないよう clamp(訂正報の ReportDateTime 逆順による後退誤検知を防ぐ)。
    // feed本文の <updated> は従来どおり先頭entryの値のまま。
    let rfc3339 = time::format_description::well_known::Rfc3339;
    let max_updated = slice
        .iter()
        .filter_map(|m| time::OffsetDateTime::parse(&m.updated, &rfc3339).ok())
        .max();
    let previous = state.feed.load().last_modified;
    let last_modified = match (max_updated, previous) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };

    state.feed.store(Arc::new(FeedSnapshot::new(
        body,
        last_updated,
        last_modified,
    )));
}
