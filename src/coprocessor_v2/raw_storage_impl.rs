// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use coprocessor_plugin_api::*;
use futures::channel::oneshot::Canceled;
use kvproto::kvrpcpb::Context;
use std::ops::Range;
use tikv_util::future::paired_future_callback;

use crate::storage::errors::extract_kv_pairs;
use crate::storage::kv::{Error as EngineError, ErrorInner as EngineErrorInner};
use crate::storage::{self, lock_manager::LockManager, Engine, Storage};

/// Implementation of the [`RawStorage`] trait.
///
/// It wraps TiKV's [`Storage`] into an API that is exposed to coprocessor plugins.
/// The `RawStorageImpl` should be constructed for every invocation of a [`CoprocessorPlugin`] as
/// it wraps a [`Context`] that is unique for every request.
pub struct RawStorageImpl<'a, E: Engine, L: LockManager> {
    context: &'a Context,
    storage: &'a Storage<E, L>,
}

impl<'a, E: Engine, L: LockManager> RawStorageImpl<'a, E, L> {
    /// Constructs a new `RawStorageImpl` that wraps a given [`Context`] and [`Storage`].
    pub fn new(context: &'a Context, storage: &'a Storage<E, L>) -> Self {
        RawStorageImpl { context, storage }
    }
}

#[async_trait(?Send)]
impl<E: Engine, L: LockManager> RawStorage for RawStorageImpl<'_, E, L> {
    async fn get(&self, key: Key) -> StorageResult<Option<Value>> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();

        let res = self.storage.raw_get(ctx, cf, key);

        let value = res.await.map_err(StorageErrorShim::from)?;
        Ok(value)
    }

    async fn batch_get(&self, keys: Vec<Key>) -> StorageResult<Vec<KvPair>> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();

        let res = self.storage.raw_batch_get(ctx, cf, keys);

        let v = res.await.map_err(StorageErrorShim::from)?;
        let kv_pairs = extract_kv_pairs(Ok(v))
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        Ok(kv_pairs)
    }

    async fn scan(&self, key_range: Range<Key>) -> StorageResult<Vec<Value>> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let key_only = false;
        let reverse = false;

        let res = self.storage.raw_scan(
            ctx,
            cf,
            key_range.start,
            Some(key_range.end),
            usize::MAX,
            key_only,
            reverse,
        );

        let v = res.await.map_err(StorageErrorShim::from)?;
        let values = extract_kv_pairs(Ok(v))
            .into_iter()
            .map(|kv| kv.value)
            .collect();
        Ok(values)
    }

    async fn put(&self, key: Key, value: Value) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let ttl = u64::MAX;
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_put(ctx, cf, key, value, ttl, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(StorageErrorShim::from)?,
        }
        .map_err(StorageErrorShim::from)?;
        Ok(())
    }

    async fn batch_put(&self, kv_pairs: Vec<KvPair>) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let ttl = u64::MAX;
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_batch_put(ctx, cf, kv_pairs, ttl, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(StorageErrorShim::from)?,
        }
        .map_err(StorageErrorShim::from)?;
        Ok(())
    }

    async fn delete(&self, key: Key) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_delete(ctx, cf, key, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(StorageErrorShim::from)?,
        }
        .map_err(StorageErrorShim::from)?;
        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<Key>) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_batch_delete(ctx, cf, keys, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(StorageErrorShim::from)?,
        }
        .map_err(StorageErrorShim::from)?;
        Ok(())
    }

    async fn delete_range(&self, key_range: Range<Key>) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();

        let (cb, f) = paired_future_callback();

        let res = self
            .storage
            .raw_delete_range(ctx, cf, key_range.start, key_range.end, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(StorageErrorShim::from)?,
        }
        .map_err(StorageErrorShim::from)?;
        Ok(())
    }
}

/// Helper struct for converting between [`storage::errors::Error`] and
/// [`coprocessor_plugin_api::StorageError`].
struct StorageErrorShim(StorageError);

impl From<StorageErrorShim> for StorageError {
    fn from(err_shim: StorageErrorShim) -> Self {
        err_shim.0
    }
}

impl From<storage::errors::Error> for StorageErrorShim {
    fn from(error: storage::errors::Error) -> Self {
        let inner = match *error.0 {
            // Key not in region
            storage::errors::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(
                ref req_err,
            ))) if req_err.has_key_not_in_region() => {
                let key_err = req_err.get_key_not_in_region();
                StorageError::KeyNotInRegion {
                    key: key_err.get_key().to_owned(),
                    region: todo!(), // TODO: how to construct region here? We only have region_id
                    start_key: key_err.get_start_key().to_owned(),
                    end_key: key_err.get_end_key().to_owned(),
                }
            }
            // Timeout
            storage::errors::ErrorInner::Engine(EngineError(box EngineErrorInner::Timeout(
                duration,
            ))) => StorageError::Timeout(duration),
            // Other errors are passed as-is inside their `Result` so we get a `&Result` when using `Any::downcast_ref`.
            _ => StorageError::Other(Box::new(storage::Result::<()>::Err(error))),
        };
        StorageErrorShim(inner)
    }
}

impl From<Canceled> for StorageErrorShim {
    fn from(_c: Canceled) -> Self {
        StorageErrorShim(StorageError::Canceled)
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
