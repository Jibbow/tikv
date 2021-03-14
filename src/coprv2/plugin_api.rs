// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::storage_api::*;
use std::any::Any;

/// A plugin that allows users to execute arbitrary code on TiKV nodes.
pub trait CoprocessorPlugin: Any + Send + Sync {
    /// Returns the name of the plugin.
    /// Requests that are sent to TiKV coprocessor must have a matching `copr_name` field.
    fn name(&self) -> &'static str;

    /// A callback fired immediately after the plugin is loaded. Usually used
    /// for initialization.
    fn on_plugin_load(&self) {}

    /// A callback fired immediately before the plugin is unloaded. Use this if
    /// you need to do any cleanup.
    fn on_plugin_unload(&self) {}

    /// Handles a request to the coprocessor.
    ///
    /// The data in the `request` parameter is exactly the same data that was passed with the
    /// `RawCoprocessorRequest` in the `data` field. Each plugin is responsible to properly decode
    /// the raw bytes by itself.
    /// The same is true for the return parameter of this function. Upon successful completion, the
    /// function should return a properly encoded result as raw bytes which is then sent back to
    /// the client.
    /// Most of the time, it's a good idea to use Protobuf for encoding/decoding, but in general you
    /// can also send raw bytes.
    ///
    /// Plugins can read and write data from the underlying [`RawStorage`] via the `storage`
    /// parameter.
    fn on_raw_coprocessor_request(
        &self,
        region: &Region,
        request: &[u8],
        storage: &dyn RawStorage,
    ) -> Result<Vec<u8>>;
}

/// Declare a plugin type and its constructor.
///
/// # Notes
///
/// This works by automatically generating an `extern "C"` function with a
/// pre-defined signature and symbol name. Therefore you will only be able to
/// declare one plugin per library.
#[macro_export]
macro_rules! declare_plugin {
    ($plugin_type:ty, $constructor:path) => {
        #[no_mangle]
        pub extern "C" fn _plugin_create() -> *mut $crate::CoprocessorPlugin {
            // make sure the constructor is the correct type.
            let constructor: fn() -> $plugin_type = $constructor;

            let object = constructor();
            let boxed: Box<$crate::CoprocessorPlugin> = Box::new(object);
            Box::into_raw(boxed)
        }
    };
}
