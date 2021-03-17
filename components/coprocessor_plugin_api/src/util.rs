// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::plugin_api::CoprocessorPlugin;

/// Name of the exported constructor function for the plugin in the `dylib`.
pub const PLUGIN_CONSTRUCTOR_NAME: &'static [u8] = b"_plugin_create";
/// Type signature of the exported constructor function for the plugin in the `dylib`.
pub type PluginConstructorSignature = unsafe fn() -> *mut dyn CoprocessorPlugin;

/// Declare a plugin for the library so that it can be loaded by TiKV.
///
/// # Notes
/// This works by automatically generating an `extern "C"` function with a
/// pre-defined signature and symbol name. Therefore you will only be able to
/// declare one plugin per library.
#[macro_export]
macro_rules! declare_plugin {
    ($plugin_type:ty) => {
        #[no_mangle]
        pub extern "C" fn _plugin_create() -> *mut $crate::CoprocessorPlugin {
            let object = <$plugin_type>::create();
            let boxed: Box<dyn $crate::CoprocessorPlugin> = Box::new(object);
            Box::into_raw(boxed)
        }
    };
}

/// Transforms the name of a package into the name of the compiled library.
///
/// The result of the function can be used to correctly locate build artifacts of `cdylib` on
/// different platforms.
///
/// The name of the `cdylib` is
/// * `lib<pkgname>.so` on Linux
/// * `lib<pkgname>.dylib` on MaxOS
/// * `lib<pkgname>.dll` on Windows
///
/// See also <https://doc.rust-lang.org/reference/linkage.html>
///
/// *Note: Depending on artifacts of other crates will be easier with
/// [this RFC](https://github.com/rust-lang/cargo/issues/9096).*
pub fn pkgname_to_libname(pkgname: &str) -> String {
    let pkgname = pkgname.to_string().replace("-", "_");
    if cfg!(target_os = "windows") {
        format!("{}.dll", pkgname)
    } else if cfg!(target_os = "macos") {
        format!("lib{}.dylib", pkgname)
    } else {
        format!("lib{}.so", pkgname)
    }
}
