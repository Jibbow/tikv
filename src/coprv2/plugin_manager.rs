// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::plugin_api::CoprocessorPlugin;
use super::storage_api::*;
use libloading::{Library, Symbol};
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsStr;
//use std::marker::PhantomPinned;
use std::ops::Deref;
use std::pin::Pin;

#[derive(Default)]
pub struct PluginManager {
    /// Plugins that are currently loaded.
    /// Provides a mapping from the plugin's name to the actual instance.
    loaded_plugins: BTreeMap<String, LoadedPlugin>,
}

impl PluginManager {
    /// Creates a new `PluginManager`.
    pub fn new() -> Self {
        PluginManager::default()
    }

    /// Finds a plugin by its name. The plugin must have been loaded before with [`load_plugin()`].
    ///
    /// Plugins are indexed by the name that is returned by [`CoprocessorPlugin::name()`].
    pub fn get_plugin(&self, plugin_name: &str) -> Option<&impl CoprocessorPlugin> {
        self.loaded_plugins.get(plugin_name)
    }

    /// Loads a [`CoprocessorPlugin`] from a `cdylib`.
    ///
    /// After this function has successfully finished, the plugin is registered with the
    /// [`PluginManager`] and can later be obtained by calling [`get_plugin()`] with the proper
    /// name.
    /// TODO: either return reference to plugin or the name of the plugin
    pub fn load_plugin<P: AsRef<OsStr>>(&mut self, filename: P) -> Result<()> {
        let lib = unsafe { Library::new(filename).expect("failed to load library") };
        let plugin = unsafe { LoadedPlugin::new(lib)? };
        let plugin_name = plugin.name().to_string();

        self.loaded_plugins.insert(plugin_name, plugin);
        Ok(())
    }
}

/// A wrapper around a loaded raw coprocessor plugin library.
///
/// Can be dereferenced to [`CoprocessorPlugin`].
///
/// Takes care of calling [`on_plugin_load()`] and [`on_plugin_unload()`];
/// [`on_plugin_unload()`] is called when `LoadedPlugin` is dropped.
struct LoadedPlugin {
    /// Pointer to a [`CoprocessorPlugin`] in the loaded `lib`.
    plugin: Box<dyn CoprocessorPlugin>,
    /// Underlying library file on a fixed position on the heap.
    lib: Pin<Box<Library>>,
    // Make sure the struct does not implement [`Unpin`]
    //_pin: PhantomPinned,
}

impl LoadedPlugin {
    /// Creates a new `LoadedPlugin` by loading a `cdylib` from a file into memory.
    ///
    /// The function instantiates the plugin by calling `_plugin_create()` to obtain a
    /// [`CoprocessorPlugin`]. It also calls [`on_plugin_load()`] on before the function returns.
    pub unsafe fn new(lib: Library) -> Result<Self> {
        type PluginCreate = unsafe fn() -> *mut dyn CoprocessorPlugin;

        let lib = Box::pin(lib);
        let constructor: Symbol<PluginCreate> = lib
            .get(b"_plugin_create")
            .expect("The `_plugin_create` symbol wasn't found.");

        let boxed_raw_plugin = constructor();
        let plugin = Box::from_raw(boxed_raw_plugin);

        plugin.on_plugin_load();

        Ok(LoadedPlugin { plugin, lib })
    }
}

impl Drop for LoadedPlugin {
    fn drop(&mut self) {
        self.plugin.on_plugin_unload();
    }
}

impl Deref for LoadedPlugin {
    type Target = Box<dyn CoprocessorPlugin>;
    fn deref(&self) -> &Self::Target {
        &self.plugin
    }
}

impl CoprocessorPlugin for LoadedPlugin {
    fn name(&self) -> &'static str {
        self.plugin.name()
    }

    /// A callback fired immediately after the plugin is loaded. Usually used
    /// for initialization.
    fn on_plugin_load(&self) {
        self.plugin.on_plugin_load()
    }

    /// A callback fired immediately before the plugin is unloaded. Use this if
    /// you need to do any cleanup.
    fn on_plugin_unload(&self) {
        self.plugin.on_plugin_unload()
    }

    fn on_raw_coprocessor_request(
        &self,
        region: &Region,
        request: &[u8],
        storage: &dyn RawStorage,
    ) -> Result<Vec<u8>> {
        self.plugin
            .on_raw_coprocessor_request(region, request, storage)
    }
}
