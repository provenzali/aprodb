use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use aprodb_types::RecordIdentity;
use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

const CACHE_SHARDS: usize = 16;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CacheMetrics {
    pub budget_bytes: usize,
    pub resident_bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub admissions: u64,
    pub rejections: u64,
    pub evictions: u64,
}

struct Entry<V> {
    value: V,
    bytes: usize,
    frequency: u64,
    radial_score_millis: u16,
    protected: bool,
    pinned_until_unix_ms: Option<u64>,
    expires_at_unix_ms: Option<u64>,
}

struct CacheShard<V> {
    values: HashMap<RecordIdentity, Entry<V>>,
    resident_bytes: usize,
}

impl<V> Default for CacheShard<V> {
    fn default() -> Self {
        Self {
            values: HashMap::new(),
            resident_bytes: 0,
        }
    }
}

pub(crate) struct BudgetCache<V> {
    shards: Vec<Mutex<CacheShard<V>>>,
    budget_bytes: usize,
    shard_budget_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    admissions: AtomicU64,
    rejections: AtomicU64,
    evictions: AtomicU64,
    resident_bytes: AtomicUsize,
}

pub(crate) struct CacheAdmission {
    pub(crate) bytes: usize,
    pub(crate) radial_score_millis: u16,
    pub(crate) pinned_until_unix_ms: Option<u64>,
    pub(crate) expires_at_unix_ms: Option<u64>,
    pub(crate) now_unix_ms: u64,
}

impl<V: Clone> BudgetCache<V> {
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self {
            shards: (0..CACHE_SHARDS)
                .map(|_| Mutex::new(CacheShard::default()))
                .collect(),
            budget_bytes,
            shard_budget_bytes: budget_bytes / CACHE_SHARDS,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            resident_bytes: AtomicUsize::new(0),
        }
    }

    pub(crate) fn get(&self, key: &RecordIdentity, now_unix_ms: u64) -> Option<V> {
        let mut shard = self.shards[self.shard_index(key)].lock();
        let expired = shard
            .values
            .get(key)
            .and_then(|entry| entry.expires_at_unix_ms)
            .is_some_and(|expires| expires <= now_unix_ms);
        if expired {
            self.remove_locked(&mut shard, key);
        }
        let Some(entry) = shard.values.get_mut(key) else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        entry.frequency = entry.frequency.saturating_add(1);
        entry.protected |= entry.frequency >= 2;
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry.value.clone())
    }

    pub(crate) fn insert(&self, key: RecordIdentity, value: V, admission: CacheAdmission) -> bool {
        let CacheAdmission {
            bytes,
            radial_score_millis,
            pinned_until_unix_ms,
            expires_at_unix_ms,
            now_unix_ms,
        } = admission;
        if self.shard_budget_bytes == 0 || bytes > self.shard_budget_bytes {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let mut shard = self.shards[self.shard_index(&key)].lock();
        self.remove_locked(&mut shard, &key);
        while shard.resident_bytes.saturating_add(bytes) > self.shard_budget_bytes {
            let victim = shard
                .values
                .iter()
                .filter(|(_, entry)| {
                    !entry
                        .pinned_until_unix_ms
                        .is_some_and(|until| until > now_unix_ms)
                })
                .min_by(|(_, left), (_, right)| compare_priority(left, right))
                .map(|(identity, entry)| (identity.clone(), entry_priority(entry)));
            let Some((victim, victim_priority)) = victim else {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                return false;
            };
            let candidate_priority = (u128::from(1000 + radial_score_millis) * 1024)
                / u128::try_from(bytes).unwrap_or(u128::MAX);
            if candidate_priority <= victim_priority {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            self.remove_locked(&mut shard, &victim);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        shard.resident_bytes = shard.resident_bytes.saturating_add(bytes);
        self.resident_bytes.fetch_add(bytes, Ordering::AcqRel);
        shard.values.insert(
            key,
            Entry {
                value,
                bytes,
                frequency: 1,
                radial_score_millis,
                protected: false,
                pinned_until_unix_ms,
                expires_at_unix_ms,
            },
        );
        self.admissions.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(crate) fn invalidate(&self, key: &RecordIdentity) {
        let mut shard = self.shards[self.shard_index(key)].lock();
        self.remove_locked(&mut shard, key);
    }

    pub(crate) fn is_resident(&self, key: &RecordIdentity, now_unix_ms: u64) -> bool {
        self.shards[self.shard_index(key)]
            .lock()
            .values
            .get(key)
            .is_some_and(|entry| {
                !entry
                    .expires_at_unix_ms
                    .is_some_and(|expires| expires <= now_unix_ms)
            })
    }

    pub(crate) fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            budget_bytes: self.budget_bytes,
            resident_bytes: self.resident_bytes.load(Ordering::Acquire),
            entries: self
                .shards
                .iter()
                .map(|shard| shard.lock().values.len())
                .sum(),
            hits: self.hits.load(Ordering::Acquire),
            misses: self.misses.load(Ordering::Acquire),
            admissions: self.admissions.load(Ordering::Acquire),
            rejections: self.rejections.load(Ordering::Acquire),
            evictions: self.evictions.load(Ordering::Acquire),
        }
    }

    fn shard_index(&self, key: &RecordIdentity) -> usize {
        xxh3_64(&key.storage_key()) as usize & (CACHE_SHARDS - 1)
    }

    fn remove_locked(&self, shard: &mut CacheShard<V>, key: &RecordIdentity) {
        if let Some(removed) = shard.values.remove(key) {
            shard.resident_bytes = shard.resident_bytes.saturating_sub(removed.bytes);
            self.resident_bytes
                .fetch_sub(removed.bytes, Ordering::AcqRel);
        }
    }
}

fn entry_priority<V>(entry: &Entry<V>) -> u128 {
    let protected_bonus = if entry.protected { 1000_u128 } else { 0 };
    let numerator = u128::from(entry.frequency.min(1_000_000)) * 1000
        + u128::from(entry.radial_score_millis)
        + protected_bonus;
    (numerator * 1024) / u128::try_from(entry.bytes.max(1)).unwrap_or(u128::MAX)
}

fn compare_priority<V>(left: &Entry<V>, right: &Entry<V>) -> std::cmp::Ordering {
    entry_priority(left).cmp(&entry_priority(right))
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
