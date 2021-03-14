// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::sync::Arc;

use super::plugin_api::CoprocessorPlugin;
use super::plugin_manager::PluginManager;
use super::storage_api::Region;
use super::storage_impl::RawStorageImpl;
use crate::storage::lock_manager::LockManager;
use crate::storage::{Engine, Storage};
use kvproto::coprocessor_v2 as coprv2pb;

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct CoprV2Endpoint /*<E: Engine + std::marker::Sync>*/ {
    //storage: crate::storage::Storage<E, crate::storage::lock_manager::DummyLockManager>,
    plugin_manager: Arc<PluginManager>,
}

impl tikv_util::AssertSend for CoprV2Endpoint {}

impl CoprV2Endpoint {
    pub fn new() -> Self {
        let plugin_manager = Arc::new(PluginManager::new());
        Self { plugin_manager }
    }

    /// Handles a request to the coprocessor framework.
    ///
    /// Each request is dispatched to the corresponding coprocessor plugin based on it's `copr_name`
    /// field. A plugin with a matching name must be loaded by TiKV, otherwise an error is returned.
    #[inline]
    pub fn handle_request<E: Engine, L: LockManager>(
        &self,
        storage: &Storage<E, L>,
        req: coprv2pb::RawCoprocessorRequest,
    ) -> impl Future<Output = coprv2pb::RawCoprocessorResponse> {
        // TODO: how to get Region?

        let plugin = self.plugin_manager.get_plugin(&req.copr_name).unwrap();
        let raw_storage = RawStorageImpl::new(req.get_context().clone(), storage.clone());
        let result = plugin
            .on_raw_coprocessor_request(&Region::default(), &req.data, &raw_storage)
            .unwrap();

        let mut response = coprv2pb::RawCoprocessorResponse::new();
        response.data = result;
        std::future::ready(response)
    }
}

#[cfg(test)]
mod tests {}
