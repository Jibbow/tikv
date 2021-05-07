// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use coprocessor_plugin_api::*;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
enum PluginRequest {
    /// Reads a value from the storage for the given `key`.
    Read { key: Key },
    /// Writes a key-value pair to the storage.
    Write { key: Key, value: Value },
    /// Adds two numbers (without touching the storage).
    Add { x: u32, y: u32 },
    /// Returns an error that originated in the plugin.
    Error,
    /// Lets the plugin panic.
    Panic,
}

#[derive(Serialize)]
enum PluginResponse {
    Read(Option<Value>),
    Write(),
    Add(u32),
}

#[derive(Default)]
struct ExamplePlugin;

impl CoprocessorPlugin for ExamplePlugin {
    fn on_raw_coprocessor_request(
        &self,
        _region: &Region,
        request: &RawRequest,
        storage: &dyn RawStorage,
    ) -> Result<RawResponse, PluginError> {
        // We use JSON format for interaction with our plugin.
        // You can also use any other format, e.g. Protobuf.
        let request = serde_json::from_slice(request)
            .map_err(|e| format!("Failed to decode coprocessor request: {}", e));

        let response: Result<PluginResponse, String> = match request {
            Ok(PluginRequest::Read { key }) => {
                let value = block_on(storage.get(key))?;
                Ok(PluginResponse::Read(value))
            }
            Ok(PluginRequest::Write { key, value }) => {
                block_on(storage.put(key, value))?;
                Ok(PluginResponse::Write())
            }
            Ok(PluginRequest::Add { x, y }) => Ok(PluginResponse::Add(x + y)),
            Ok(PluginRequest::Error) => {
                // Plugins will need to do their own error handling and encode
                // the error accordingly in their response. `PluginError` is
                // only used for returning errors originated in TiKV.
                Err("User-defined error message".to_string())
            }
            Ok(PluginRequest::Panic) => {
                panic!("Coprocessor plugin received a `PanicRequest`. This panic is intended.")
            }
            Err(err) => Err(err),
        };

        // In the end, we need to encode our response as JSON again.
        Ok(serde_json::to_vec(&response).unwrap())
    }
}

declare_plugin!(ExamplePlugin::default());
