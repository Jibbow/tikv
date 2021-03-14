// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::storage_api::*;
use crate::storage::errors::{extract_kv_pairs, extract_region_error};
use crate::storage::lock_manager::LockManager;
use crate::storage::{Engine, Storage};
use async_trait::async_trait;
use kvproto::kvrpcpb::Context;
use std::ops::Range;
use tikv_util::future::paired_future_callback;

pub struct RawStorageImpl<E: Engine, L: LockManager> {
    context: Context,
    storage: Storage<E, L>,
}

impl<E: Engine, L: LockManager> RawStorageImpl<E, L> {
    pub fn new(context: Context, storage: Storage<E, L>) -> Self {
        RawStorageImpl { context, storage }
    }
}

#[async_trait(?Send)]
impl<E: Engine, L: LockManager> RawStorage for RawStorageImpl<E, L> {
    async fn get(&self, key: Key) -> Result<Option<Value>> {
        let ctx = self.context.clone();
        let cf = "".to_string();

        let res = self.storage.clone().raw_get(ctx, cf, key);

        let v = res.await;
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(v.expect("v has to be Ok here"))
        }
    }

    async fn batch_get(&self, keys: Vec<Key>) -> Result<Vec<KvPair>> {
        let ctx = self.context.clone();
        let cf = "".to_string();

        let res = self.storage.clone().raw_batch_get(ctx, cf, keys);

        let v = res.await;
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(extract_kv_pairs(v)
                .into_iter()
                .map(|kv| (kv.key, kv.value))
                .collect())
        }
    }

    async fn scan(&self, key_range: Range<Key>) -> Result<Vec<Value>> {
        let ctx = self.context.clone();
        let cf = "".to_string();
        let key_only = false;
        let reverse = false;

        let res = self.storage.clone().raw_scan(
            ctx,
            cf,
            key_range.start,
            Some(key_range.end),
            usize::MAX,
            key_only,
            reverse,
        );

        let v = res.await;
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(extract_kv_pairs(v).into_iter().map(|kv| kv.value).collect())
        }
    }

    async fn put(&self, key: Key, value: Value) -> Result<()> {
        let ctx = self.context.clone();
        let cf = "".to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.clone().raw_put(ctx, cf, key, value, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(())
        }
    }

    async fn batch_put(&self, kv_pairs: Vec<KvPair>) -> Result<()> {
        let ctx = self.context.clone();
        let cf = "".to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.clone().raw_batch_put(ctx, cf, kv_pairs, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(())
        }
    }

    async fn delete(&self, key: Key) -> Result<()> {
        let ctx = self.context.clone();
        let cf = "".to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.clone().raw_delete(ctx, cf, key, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(())
        }
    }

    async fn batch_delete(&self, keys: Vec<Key>) -> Result<()> {
        let ctx = self.context.clone();
        let cf = "".to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.clone().raw_batch_delete(ctx, cf, keys, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(())
        }
    }

    async fn delete_range(&self, key_range: Range<Key>) -> Result<()> {
        let ctx = self.context.clone();
        let storage = self.storage.clone();
        let cf = "".to_string();

        let (cb, f) = paired_future_callback();

        let res = storage.raw_delete_range(ctx, cf, key_range.start, key_range.end, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        if let Some(err) = extract_region_error(&v) {
            Err(Error::RegionError(err))
        } else if let Err(e) = v {
            Err(Error::OtherError(format!("{}", e)))
        } else {
            Ok(())
        }
    }
}
