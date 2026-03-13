use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, Ident, LitBool, LitByteStr, Token};

pub(crate) struct PathSegments {
    pub(crate) segments: Vec<Ident>,
}

impl Parse for PathSegments {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut segments = Vec::new();
        loop {
            if input.is_empty() || input.peek(Token![,]) {
                break;
            }
            let id: Ident = input.parse()?;
            segments.push(id);
            if input.peek(Token![/]) {
                let _: Token![/] = input.parse()?;
                continue;
            }
            break;
        }
        Ok(PathSegments { segments })
    }
}

pub(crate) struct MacroPrefix {
    pub(crate) store: Expr,
    pub(crate) type_name: LitByteStr,
    pub(crate) key: Expr,
}

pub(crate) struct GetInput {
    pub(crate) prefix: MacroPrefix,
    pub(crate) path: Option<PathSegments>,
    pub(crate) dynamic: bool,
}

impl Parse for GetInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let prefix = parse_macro_prefix(input)?;
        let (path, dynamic) = parse_path_and_dynamic(input)?;
        Ok(GetInput {
            prefix,
            path,
            dynamic,
        })
    }
}

pub(crate) struct UpdateInput {
    pub(crate) prefix: MacroPrefix,
    pub(crate) path: Option<PathSegments>,
    pub(crate) closure: Option<syn::ExprClosure>,
    pub(crate) dynamic: bool,
}

impl Parse for UpdateInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let prefix = parse_macro_prefix(input)?;
        let mut path = None;
        let mut closure = None;
        let mut dynamic = false;

        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            if input.peek(Token![|]) || input.peek(Token![move]) {
                let expr: Expr = input.parse()?;
                if let Expr::Closure(cl) = expr {
                    closure = Some(cl);
                } else {
                    return Err(syn::Error::new(
                        Span::call_site(),
                        "update! requires a closure",
                    ));
                }
            } else {
                parse_path_or_dynamic(input, &mut path, &mut dynamic)?;
            }
        }

        Ok(UpdateInput {
            prefix,
            path,
            closure,
            dynamic,
        })
    }
}

fn parse_macro_prefix(input: ParseStream) -> syn::Result<MacroPrefix> {
    let store: Expr = input.parse()?;
    input.parse::<Token![,]>()?;
    let type_name: LitByteStr = input.parse()?;
    input.parse::<Token![,]>()?;
    let key: Expr = input.parse()?;
    Ok(MacroPrefix {
        store,
        type_name,
        key,
    })
}

fn parse_dynamic_assignment(input: ParseStream) -> syn::Result<bool> {
    let identifier: Ident = input.parse()?;
    if identifier != "dynamic" {
        return Err(syn::Error::new(
            identifier.span(),
            "expected 'dynamic = true'",
        ));
    }
    input.parse::<Token![=]>()?;
    let value: LitBool = input.parse()?;
    Ok(value.value())
}

fn parse_path_or_dynamic(
    input: ParseStream,
    path: &mut Option<PathSegments>,
    dynamic: &mut bool,
) -> syn::Result<()> {
    if !input.peek(Ident) {
        return Err(syn::Error::new(
            Span::call_site(),
            "expected a path or 'dynamic = true'",
        ));
    }

    let lookahead = input.fork();
    let first_ident: Ident = lookahead.parse()?;
    if first_ident == "dynamic" && lookahead.peek(Token![=]) {
        *dynamic = parse_dynamic_assignment(input)?;
        return Ok(());
    }

    if path.is_some() {
        return Err(syn::Error::new(first_ident.span(), "path already provided"));
    }

    *path = Some(input.parse()?);
    Ok(())
}

fn parse_path_and_dynamic(input: ParseStream) -> syn::Result<(Option<PathSegments>, bool)> {
    let mut path = None;
    let mut dynamic = false;
    while input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
        if input.is_empty() {
            break;
        }
        parse_path_or_dynamic(input, &mut path, &mut dynamic)?;
    }
    Ok((path, dynamic))
}
