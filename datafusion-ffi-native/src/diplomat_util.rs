//! Shared helpers for converting Diplomat FFI string types into Rust `&str` / `String` /
//! `Vec<String>` / `HashMap<String, String>`. Used across every bridge module — must not live
//! inside a `#[diplomat::bridge]` mod.
//!
//! Errors convert to `Box<DfError>` via the `From<Utf8Error>` / `From<String>` impls in
//! `bridge.rs`, so the tuple constructor for `DfError` (which is `pub(super)` to the bridge
//! module) is never needed here.

use std::collections::HashMap;

use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

use crate::bridge::ffi::DfError;

/// Borrow a `DiplomatStr` as a UTF-8 `&str`.
pub(crate) fn diplomat_str(s: &DiplomatStr) -> Result<&str, Box<DfError>> {
    Ok(std::str::from_utf8(s)?)
}

/// Borrow a `DiplomatStrSlice` as a UTF-8 `&str`. Parallels `diplomat_str` but for the
/// by-value slice type Diplomat uses inside `&[DiplomatStrSlice]` parameters.
pub(crate) fn diplomat_slice_to_str<'a>(
    s: &'a DiplomatStrSlice<'_>,
) -> Result<&'a str, Box<DfError>> {
    Ok(std::str::from_utf8(s)?)
}

/// Convert a slice of `DiplomatStrSlice` into an owned `Vec<String>`.
pub(crate) fn diplomat_str_slice_to_vec(
    slices: &[DiplomatStrSlice<'_>],
) -> Result<Vec<String>, Box<DfError>> {
    slices
        .iter()
        .map(|s| diplomat_slice_to_str(s).map(str::to_owned))
        .collect()
}

/// Build a `HashMap<String, String>` from parallel key / value slice arrays. Returns an error
/// if the two arrays are not the same length.
pub(crate) fn diplomat_str_pairs_to_map(
    keys: &[DiplomatStrSlice<'_>],
    values: &[DiplomatStrSlice<'_>],
) -> Result<HashMap<String, String>, Box<DfError>> {
    if keys.len() != values.len() {
        return Err(format!(
            "keys and values must have the same length (got {} vs {})",
            keys.len(),
            values.len()
        )
        .into());
    }
    let mut map = HashMap::with_capacity(keys.len());
    for (k, v) in keys.iter().zip(values.iter()) {
        map.insert(
            diplomat_slice_to_str(k)?.to_owned(),
            diplomat_slice_to_str(v)?.to_owned(),
        );
    }
    Ok(map)
}
