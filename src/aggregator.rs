//! 唯一の書き込み点となる単一タスク。
//! dedup → entities(moka)更新 → 一覧(VecDeque)更新 → Atom再生成 → ArcSwap store。
//! single-writer のためロック不要。

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::jma::feed_render;
use crate::state::SharedState;
use crate::types::{DedupKey, EntityEntry, Event, EventSource, FeedSnapshot, ItemMeta};

/// 重複排除。TTL付きのseenキャッシュ。
/// `seen` はソース固有ID(dmdata電文ID等)、`body_seen` は本文blake3ハッシュによる
/// クロスソースdedupe(同一電文がdmdataとJMA pollingで別IDになるため)。
pub struct Deduper {
    seen: moka::sync::Cache<DedupKey, ()>,
    body_seen: moka::sync::Cache<[u8; 32], ()>,
}

impl Deduper {
    pub fn new(ttl: Duration) -> Self {
        Self {
            seen: moka::sync::Cache::builder()
                .max_capacity(65_536)
                .time_to_live(ttl)
                .build(),
            body_seen: moka::sync::Cache::builder()
                .max_capacity(65_536)
                .time_to_live(ttl)
                .build(),
        }
    }

    /// 未見なら登録して true、既見なら false。
    pub fn check_and_insert(&self, key: &DedupKey) -> bool {
        if self.seen.contains_key(key) {
            return false;
        }
        self.seen.insert(key.clone(), ());
        true
    }

    /// 本文ハッシュが未見なら登録して true、既見なら false。
    /// publish経路(dmdata / poll)のみが呼ぶ。JmaFeed(キャッシュミス補充)由来では
    /// 呼ばないこと — 補充がpublish-dedupeを汚染し、後続の正規配信をdropしてしまう。
    pub fn check_and_insert_body(&self, hash: &[u8; 32]) -> bool {
        if self.body_seen.contains_key(hash) {
            return false;
        }
        self.body_seen.insert(*hash, ());
        true
    }
}

/// Aggregatorタスク本体。`initial_metas` は起動時の初期一覧(新しい順)。
pub async fn run(initial_metas: Vec<ItemMeta>, mut rx: mpsc::Receiver<Event>, state: SharedState) {
    let capacity = state.config.cache.feed_entries;
    let base_url = state.config.http.public_base_url.clone();

    let mut metas: VecDeque<ItemMeta> = initial_metas.into_iter().take(capacity).collect();
    publish(&state, &mut metas, &base_url);

    let deduper = Deduper::new(Duration::from_secs(state.config.cache.seen_ttl_secs));
    state
        .readiness
        .aggregator_running
        .store(true, Ordering::Relaxed);
    tracing::info!(entries = metas.len(), "aggregator started");

    while let Some(event) = rx.recv().await {
        if !deduper.check_and_insert(&event.dedup_key) {
            tracing::debug!(id = %event.meta.id, "duplicate event dropped");
            continue;
        }

        // 実体キャッシュ更新(ETagは事前生成)
        let entry = Arc::new(EntityEntry::new(event.xml_body.clone(), event.meta.clone()));

        // キャッシュミス補充(JmaFeed)由来はentities挿入のみで一覧は再生成しない
        if event.source == EventSource::JmaFeed {
            state.entities.insert(event.meta.id.clone(), entry).await;
            tracing::debug!(id = %event.meta.id, "entity cached (feed unchanged)");
            continue;
        }

        // クロスソースdedupe: 同一電文はdmdataとJMA pollingでentry IDが異なるため
        // 本文blake3ハッシュで突合する。JmaFeed early-return の後に置くことで
        // キャッシュミス補充がpublish-dedupeを汚染しない。
        let body_hash = *blake3::hash(&event.xml_body).as_bytes();
        if !deduper.check_and_insert_body(&body_hash) {
            tracing::debug!(id = %event.meta.id, "duplicate body dropped (cross-source)");
            continue;
        }

        // dmdata/poll由来はpinnedへ(publishより前 — feedが参照する時点で必ずピン済み)。
        // 同一id再送は insert がArcを置換する。
        state.pinned.insert(event.meta.id.clone(), entry);

        // 同一idのentryは置換して先頭へ
        if let Some(pos) = metas.iter().position(|m| m.id == event.meta.id) {
            metas.remove(pos);
        }
        tracing::info!(id = %event.meta.id, title = %event.meta.title, "feed entry added");
        metas.push_front(event.meta);
        while metas.len() > capacity {
            if let Some(evicted) = metas.pop_back()
                && let Some((id, entry)) = state.pinned.remove(&evicted.id)
            {
                // feedから外れたdmdata由来entryはmokaへ降格(TTL分の猶予付きで配信継続)。
                // None(=JMAウォームアップ由来)は上流307でカバーされるため何もしない。
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deduper_rejects_second_insert() {
        let deduper = Deduper::new(Duration::from_secs(60));
        let key = DedupKey::TelegramId("t1".into());
        assert!(deduper.check_and_insert(&key));
        assert!(!deduper.check_and_insert(&key));
        assert!(deduper.check_and_insert(&DedupKey::TelegramId("t2".into())));
    }

    #[test]
    fn deduper_body_hash_rejects_second_insert() {
        let deduper = Deduper::new(Duration::from_secs(60));
        let a = *blake3::hash(b"body-a").as_bytes();
        let b = *blake3::hash(b"body-b").as_bytes();
        assert!(deduper.check_and_insert_body(&a));
        assert!(!deduper.check_and_insert_body(&a));
        // 別ハッシュは独立
        assert!(deduper.check_and_insert_body(&b));
        assert!(!deduper.check_and_insert_body(&b));
    }

    #[test]
    fn deduper_composite_keys_differ_by_hash() {
        let deduper = Deduper::new(Duration::from_secs(60));
        let a = DedupKey::composite("e1", "2026-07-05T04:10:00+09:00", b"body-a");
        let b = DedupKey::composite("e1", "2026-07-05T04:10:00+09:00", b"body-b");
        assert!(deduper.check_and_insert(&a));
        assert!(deduper.check_and_insert(&b));
        assert!(!deduper.check_and_insert(&a));
    }
}
