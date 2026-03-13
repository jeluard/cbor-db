//! Proc-macro crate implementing `get!`, `update!`, and `insert!` macros
//!
//! These macros provide compile-time validated path navigation through
//! CBOR data structures defined by CDDL schemas.
//!
//! # Features
//!
//! - **Compile-time path validation**: Paths are checked against the CDDL schema
//! - **Static offset computation**: When possible, byte offsets are computed at compile time
//! - **Hybrid navigation**: For paths through dynamic types, uses efficient runtime navigation
//! - **Rich error messages**: Clear explanations when static navigation isn't possible
//!
//! # Syntax
//!
//! ```ignore
//! // Fully static path - offset computed at compile time
//! let data = get!(store, b"block", b"key", header / header_body / slot);
//!
//! // Dynamic path - requires runtime navigation
//! let data = get!(store, b"block", b"key", header / prev_hash / foo, dynamic = true);
//!
//! // Insert data
//! insert!(store, b"block", b"key", data_bytes);
//!
//! // Update with closure
//! update!(store, b"block", b"key", header / slot, |slice| { slice[0] = 0xFF; });
//! ```

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Expr, Token};

mod cddl_convert;
mod expansion;
mod parse;
mod path_analysis;
mod schema_ir;

use cddl_convert::cddl_to_ir;
use schema_ir::SchemaIR;

pub(crate) fn load_schema() -> Result<SchemaIR, String> {
    let schema_path =
        std::env::var("CBOR_DB_SCHEMA").unwrap_or_else(|_| "schemas/conway.cddl".to_string());
    let source = std::fs::read_to_string(&schema_path)
        .map_err(|err| format!("Failed to read CDDL from {}: {}", schema_path, err))?;
    let ast = cddl::parser::cddl_from_str(&source, false)
        .map_err(|err| format!("Failed to parse CDDL from {}: {}", schema_path, err))?;
    cddl_to_ir(&ast)
}

fn compile_error_ts(span: Span, msg: &str) -> TokenStream {
    syn::Error::new(span, msg).to_compile_error().into()
}

// ============================================================================
// get! Macro
// ============================================================================

/// Read CBOR data with compile-time path validation.
///
/// # Syntax
///
/// ```ignore
/// get!(store, b"type_name", b"key")
/// get!(store, b"type_name", b"key", path / to / field)
/// get!(store, b"type_name", b"key", path / to / field, dynamic = true)
/// ```
///
/// # Static vs Dynamic Navigation
///
/// By default, the macro requires all paths to be statically navigable (i.e., all
/// fields between root and target must have compile-time known sizes). If a path
/// traverses a choice type, optional field, or unbounded array, you must add
/// `dynamic = true` to opt into runtime navigation.
///
/// Static navigation is faster (just byte slice indexing), while dynamic navigation
/// uses the CBOR navigator to skip through variable-size elements.
#[proc_macro]
pub fn get(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as parse::GetInput);
    match expansion::expand_get(parsed) {
        Ok(tokens) => tokens.into(),
        Err(err) => compile_error_ts(Span::call_site(), &err),
    }
}

// ============================================================================
// update! Macro
// ============================================================================

/// Modify CBOR data with compile-time path validation.
///
/// # Syntax
///
/// ```ignore
/// update!(store, b"type_name", b"key", |buf| { ... })
/// update!(store, b"type_name", b"key", path / to / field, |buf| { ... })
/// update!(store, b"type_name", b"key", path / to / field, |buf| { ... }, dynamic = true)
/// ```
#[proc_macro]
pub fn update(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as parse::UpdateInput);
    match expansion::expand_update(parsed) {
        Ok(tokens) => tokens.into(),
        Err(err) => compile_error_ts(Span::call_site(), &err),
    }
}

// ============================================================================
// insert! Macro
// ============================================================================

/// Insert CBOR data into the store.
///
/// # Syntax
///
/// ```ignore
/// insert!(store, b"type_name", b"key", value)
/// ```
#[proc_macro]
pub fn insert(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input with syn::punctuated::Punctuated::<Expr, Token![,]>::parse_terminated);

    if args.len() != 4 {
        return compile_error_ts(
            Span::call_site(),
            "insert! expects 4 arguments: store, type_name, key, value",
        );
    }

    let store = &args[0];
    let ns = &args[1];
    let key = &args[2];
    let value = &args[3];

    let expanded = quote! {
        {
            let __bytes: Vec<u8> = (#value).to_vec();
            #store.insert(#ns, #key, __bytes)
        }
    };

    expanded.into()
}
