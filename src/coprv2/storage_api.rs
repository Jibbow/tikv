// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage access for coprocessor plugins.

use async_trait::async_trait;
use std::ops::Range;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type KvPair = (Key, Value);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Default)]
pub struct Region {
    pub id: u64,
    pub start_key: Key,
    pub end_key: Key,
    pub region_epoch: RegionEpoch,
}

#[derive(Debug, Clone, Default)]
pub struct RegionEpoch {
    pub conf_ver: u64,
    pub version: u64,
}

#[derive(Debug)]
pub enum Error {
    //KeyNotInRegion { key: Key, region: Region },
    // More
    RegionError(kvproto::errorpb::Error),
    OtherError(String),
}

/// Storage API for coprocessor plugins.
///
/// [`RawStorage`] allows coprocessor plugins to interact with TiKV storage on a low level.
/// TODO: in the RFC, some methods took `&mut self`. Why?
#[async_trait(?Send)]
pub trait RawStorage: Send {
    async fn get(&self, key: Key) -> Result<Option<Value>>;
    async fn batch_get(&self, keys: Vec<Key>) -> Result<Vec<KvPair>>;
    async fn scan(&self, key_range: Range<Key>) -> Result<Vec<Value>>;
    async fn put(&self, key: Key, value: Value) -> Result<()>;
    async fn batch_put(&self, kv_pairs: Vec<KvPair>) -> Result<()>;
    async fn delete(&self, key: Key) -> Result<()>;
    async fn batch_delete(&self, keys: Vec<Key>) -> Result<()>;
    async fn delete_range(&self, key_range: Range<Key>) -> Result<()>;
}
