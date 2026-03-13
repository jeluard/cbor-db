use crate::parse::{GetInput, PathSegments, UpdateInput};
use crate::path_analysis::{analyze_path, PathPlan, PathPlanKind};
use crate::schema_ir::{SchemaIR, TypeDef};
use quote::quote;
use syn::{Expr, LitByteStr};

pub(crate) fn expand_get(input: GetInput) -> Result<proc_macro2::TokenStream, String> {
    let store = input.prefix.store;
    let ns = input.prefix.type_name;
    let key = input.prefix.key;

    let schema = crate::load_schema()?;
    let plan = resolve_validated_plan(&schema, &ns, input.path.as_ref(), input.dynamic)?;

    Ok(match plan.kind {
        PathPlanKind::WholeValue => quote! { #store.get(#ns, #key) },
        PathPlanKind::StaticSlice { offset, size } => quote! {
            #store.get_range(#ns, #key, (#offset, #size))
        },
        PathPlanKind::RuntimeSlice { offset, indices } => {
            expand_runtime_get(&store, &ns, &key, offset, &indices)
        }
    })
}

pub(crate) fn expand_update(input: UpdateInput) -> Result<proc_macro2::TokenStream, String> {
    let store = input.prefix.store;
    let ns = input.prefix.type_name;
    let key = input.prefix.key;
    let closure = input.closure.ok_or("update! requires a closure")?;

    let schema = crate::load_schema()?;
    let plan = resolve_validated_plan(&schema, &ns, input.path.as_ref(), input.dynamic)?;

    Ok(match plan.kind {
        PathPlanKind::WholeValue => quote! { #store.update(#ns, #key, &mut #closure) },
        PathPlanKind::StaticSlice { offset, size } => quote! {
            #store.update(#ns, #key, &mut |__buf: &mut Vec<u8>| {
                let __slice = &mut __buf[#offset..#offset + #size];
                (#closure)(__slice);
            })
        },
        PathPlanKind::RuntimeSlice { offset, indices } => {
            expand_runtime_update(&store, &ns, &key, &closure, offset, &indices)
        }
    })
}

fn resolve_validated_plan(
    schema: &SchemaIR,
    ns: &LitByteStr,
    path: Option<&PathSegments>,
    dynamic: bool,
) -> Result<PathPlan, String> {
    let type_name = parse_type_name(ns)?;
    let root_type = validate_type_name(schema, &type_name)?;
    let (path_strings, plan) = analyze_requested_path(schema, root_type, path)?;
    require_dynamic_opt_in(&plan, &path_strings, dynamic)?;
    Ok(plan)
}

fn parse_type_name(ns: &LitByteStr) -> Result<String, String> {
    let type_name_bytes = ns.value();
    let type_name =
        std::str::from_utf8(&type_name_bytes).map_err(|_| "Type name must be valid UTF-8")?;
    Ok(type_name.to_string())
}

fn validate_type_name<'a>(schema: &'a SchemaIR, type_name: &str) -> Result<&'a TypeDef, String> {
    schema.get(type_name).ok_or_else(|| {
        let mut available: Vec<&str> = schema.types.keys().map(|s| s.as_str()).collect();
        available.sort_unstable();
        format!(
            "Type '{}' not found in schema. Available types: {:?}",
            type_name, available
        )
    })
}

fn analyze_requested_path(
    schema: &SchemaIR,
    root_type: &TypeDef,
    path: Option<&PathSegments>,
) -> Result<(Vec<String>, PathPlan), String> {
    let Some(path) = path else {
        return Ok((Vec::new(), PathPlan::whole_value()));
    };

    if path.segments.is_empty() {
        return Ok((Vec::new(), PathPlan::whole_value()));
    }

    let path_strings: Vec<String> = path
        .segments
        .iter()
        .map(|segment| segment.to_string())
        .collect();
    let path_refs: Vec<&str> = path_strings.iter().map(String::as_str).collect();
    let plan = analyze_path(&path_refs, root_type, schema)?;
    Ok((path_strings, plan))
}

fn require_dynamic_opt_in(
    plan: &PathPlan,
    path_strings: &[String],
    dynamic: bool,
) -> Result<(), String> {
    if plan.requires_dynamic() && !dynamic {
        Err(plan.format_dynamic_error(path_strings))
    } else {
        Ok(())
    }
}

fn expand_runtime_get(
    store: &Expr,
    namespace: &LitByteStr,
    key: &Expr,
    offset: usize,
    indices: &[usize],
) -> proc_macro2::TokenStream {
    if indices.is_empty() {
        quote! {
            (|| -> Result<store::Bytes, store::StoreError> {
                let __data = #store.get(#namespace, #key)?;
                let __slice = store::navigator::take_cbor_value(&__data.as_ref()[#offset..])
                    .map_err(|err| store::StoreError::BackendError(err.to_string()))?;
                Ok::<store::Bytes, store::StoreError>(store::Bytes::new(__slice.to_vec()))
            })()
        }
    } else {
        quote! {
            (|| -> Result<store::Bytes, store::StoreError> {
                let __data = #store.get(#namespace, #key)?;
                let __base = &__data.as_ref()[#offset..];
                let __slice = store::navigator::navigate(__base, &[#(#indices),*])
                    .map_err(|err| store::StoreError::BackendError(err.to_string()))?;
                Ok::<store::Bytes, store::StoreError>(store::Bytes::new(__slice.to_vec()))
            })()
        }
    }
}

fn expand_runtime_update(
    store: &Expr,
    namespace: &LitByteStr,
    key: &Expr,
    closure: &syn::ExprClosure,
    offset: usize,
    indices: &[usize],
) -> proc_macro2::TokenStream {
    if indices.is_empty() {
        quote! {
            {
                let mut __navigation_error: Option<String> = None;
                #store.update(#namespace, #key, &mut |__buf: &mut Vec<u8>| {
                    match store::navigator::take_cbor_value_to_offset(&__buf[#offset..]) {
                        Ok((__start, __len)) => {
                            let __actual_start = #offset + __start;
                            let __slice = &mut __buf[__actual_start..__actual_start + __len];
                            (#closure)(__slice);
                        }
                        Err(err) => {
                            __navigation_error = Some(err.to_string());
                        }
                    }
                })?;
                if let Some(__err) = __navigation_error {
                    Err(__err)
                } else {
                    Ok(())
                }
            }
        }
    } else {
        quote! {
            {
                let mut __navigation_error: Option<String> = None;
                #store.update(#namespace, #key, &mut |__buf: &mut Vec<u8>| {
                    match store::navigator::navigate_to_offset(&__buf[#offset..], &[#(#indices),*]) {
                        Ok((__start, __len)) => {
                            let __actual_start = #offset + __start;
                            let __slice = &mut __buf[__actual_start..__actual_start + __len];
                            (#closure)(__slice);
                        }
                        Err(err) => {
                            __navigation_error = Some(err.to_string());
                        }
                    }
                })?;
                if let Some(__err) = __navigation_error {
                    Err(__err)
                } else {
                    Ok(())
                }
            }
        }
    }
}
