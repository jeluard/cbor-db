//! Convert the subset of CDDL used by this crate into a path-oriented schema model.
//!
//! Supported CDDL subset:
//! - named type rules and named group rules
//! - arrays with named fields
//! - choices expressed with `/` or inline group choices
//! - references to named types
//! - `uint`, `int`, `bytes`, `text`, `bool`, `true`, `false`, `nil`, `null`, `undefined`
//! - `.size` constraints on `uint`, `int`, `bytes`, and `text`
//! - integer literal ranges with `..`
//!
//! Explicitly not supported here:
//! - maps
//! - tagged values
//! - generic instantiations
//! - wildcard `any`
//! - other CDDL constructs that are not needed for byte-offset and path analysis

use crate::schema_ir::{PrimitiveKind, SchemaIR, TypeDef, TypeKind, VariantDef};
use cddl::ast::{self, GroupChoice, Rule, Type, Type1, Type2};
use cddl::token::ControlOperator;

pub fn cddl_to_ir(cddl: &ast::CDDL) -> Result<SchemaIR, String> {
    let mut schema = SchemaIR::new();

    for rule in &cddl.rules {
        let (name, type_def) = convert_rule(rule)?;
        schema.types.insert(name, type_def);
    }

    Ok(schema)
}

fn convert_rule(rule: &Rule) -> Result<(String, TypeDef), String> {
    let name = rule.name().to_string();
    let type_def = match rule {
        Rule::Type { rule, .. } => convert_type(&rule.value)?,
        Rule::Group { rule, .. } => convert_group_entry(&rule.entry)?,
    };
    Ok((name, type_def))
}

fn convert_type(ty: &Type) -> Result<TypeDef, String> {
    if ty.type_choices.len() == 1 {
        convert_type1(&ty.type_choices[0].type1)
    } else {
        let mut variants = Vec::new();
        for tc in &ty.type_choices {
            let type_def = convert_type1(&tc.type1)?;
            let name = extract_type_name(&tc.type1.type2);
            variants.push(VariantDef { name, type_def });
        }
        Ok(TypeDef::new(TypeKind::Choice { variants }))
    }
}

fn convert_type1(t1: &Type1) -> Result<TypeDef, String> {
    if let Some(ref op) = t1.operator {
        if let Some(size) = extract_size_constraint(op) {
            return convert_type2_with_size(&t1.type2, size);
        }
        if let Some((min, max)) = extract_range_constraint(t1) {
            return Ok(TypeDef::new(TypeKind::Range { min, max }));
        }
    }
    convert_type2(&t1.type2)
}

fn extract_size_constraint(op: &ast::Operator) -> Option<usize> {
    match &op.operator {
        ast::RangeCtlOp::CtlOp { ctrl, .. } => {
            if *ctrl == ControlOperator::SIZE {
                if let Type2::UintValue { value, .. } = &op.type2 {
                    return Some(*value);
                }
            }
            None
        }
        _ => None,
    }
}

fn extract_range_constraint(t1: &Type1) -> Option<(i128, i128)> {
    if let Some(ref op) = t1.operator {
        if let ast::RangeCtlOp::RangeOp { .. } = &op.operator {
            let min = type2_to_int(&t1.type2)?;
            let max = type2_to_int(&op.type2)?;
            return Some((min, max));
        }
    }
    None
}

fn type2_to_int(t2: &Type2) -> Option<i128> {
    match t2 {
        Type2::UintValue { value, .. } => Some(*value as i128),
        Type2::IntValue { value, .. } => Some(*value as i128),
        _ => None,
    }
}

fn convert_type2_with_size(t2: &Type2, size: usize) -> Result<TypeDef, String> {
    match t2 {
        Type2::Typename { ident, .. } => {
            let name = ident.ident;
            match name {
                "uint" | "int" => Ok(TypeDef::new(TypeKind::FixedUint { bytes: size })),
                "bytes" | "bstr" => Ok(TypeDef::new(TypeKind::FixedBytes { len: size })),
                "text" | "tstr" => Ok(TypeDef::new(TypeKind::FixedText { len: size })),
                _ => Ok(TypeDef::new(TypeKind::Reference {
                    name: name.to_string(),
                })),
            }
        }
        _ => Err(format!("Cannot apply .size constraint to {:?}", t2)),
    }
}

fn convert_type2(t2: &Type2) -> Result<TypeDef, String> {
    match t2 {
        Type2::UintValue { value, .. } => Ok(TypeDef::new(TypeKind::Range {
            min: *value as i128,
            max: *value as i128,
        })),
        Type2::IntValue { value, .. } => Ok(TypeDef::new(TypeKind::Range {
            min: *value as i128,
            max: *value as i128,
        })),
        Type2::Typename {
            ident,
            generic_args,
            ..
        } => {
            let name = ident.ident;
            match name {
                "uint" => Ok(TypeDef::new(TypeKind::Uint)),
                "int" => Ok(TypeDef::new(TypeKind::Int)),
                "bytes" | "bstr" => Ok(TypeDef::new(TypeKind::Bytes)),
                "text" | "tstr" => Ok(TypeDef::new(TypeKind::Text)),
                "bool" => Ok(TypeDef::new(TypeKind::Primitive(PrimitiveKind::Bool))),
                "true" => Ok(TypeDef::new(TypeKind::Primitive(PrimitiveKind::True))),
                "false" => Ok(TypeDef::new(TypeKind::Primitive(PrimitiveKind::False))),
                "nil" => Ok(TypeDef::new(TypeKind::Primitive(PrimitiveKind::Nil))),
                "null" => Ok(TypeDef::new(TypeKind::Primitive(PrimitiveKind::Null))),
                "undefined" => Ok(TypeDef::new(TypeKind::Primitive(PrimitiveKind::Undefined))),
                _ => {
                    if generic_args.is_some() {
                        return Err(format!("Generic type '{}' is not supported yet", name));
                    }
                    Ok(TypeDef::new(TypeKind::Reference {
                        name: name.to_string(),
                    }))
                }
            }
        }
        Type2::Array { group, .. } => {
            let elements = convert_group(group)?;
            Ok(TypeDef::new(TypeKind::Array { elements }))
        }
        Type2::ParenthesizedType { pt, .. } => convert_type(pt),
        Type2::ChoiceFromInlineGroup { group, .. } => {
            let mut variants = Vec::new();
            for gc in &group.group_choices {
                for (entry, _optional_comma) in &gc.group_entries {
                    if let Some((name, type_def)) = convert_group_entry_to_variant(entry)? {
                        variants.push(VariantDef { name, type_def });
                    }
                }
            }
            Ok(TypeDef::new(TypeKind::Choice { variants }))
        }
        other => Err(format!(
            "Unsupported CDDL construct: {:?}",
            std::mem::discriminant(other)
        )),
    }
}

fn convert_group(group: &ast::Group) -> Result<Vec<crate::schema_ir::FieldDef>, String> {
    let mut fields = Vec::new();
    for group_choice in &group.group_choices {
        fields.extend(convert_group_choice(group_choice)?);
    }
    Ok(fields)
}

fn convert_group_choice(gc: &GroupChoice) -> Result<Vec<crate::schema_ir::FieldDef>, String> {
    let mut fields = Vec::new();
    for (entry, _optional_comma) in &gc.group_entries {
        let (field_name, field_type, optional) = convert_group_entry_to_field(entry)?;
        fields.push(crate::schema_ir::FieldDef {
            name: field_name,
            type_def: field_type,
            optional,
        });
    }
    Ok(fields)
}

fn convert_group_entry_to_field(
    entry: &ast::GroupEntry,
) -> Result<(Option<String>, TypeDef, bool), String> {
    match entry {
        ast::GroupEntry::ValueMemberKey { ge, .. } => {
            let type_def = convert_type(&ge.entry_type)?;
            let optional = ge
                .occur
                .as_ref()
                .map(|o| is_optional_occurrence(&o.occur))
                .unwrap_or(false);
            let name = extract_member_key_name(&ge.member_key).or_else(|| {
                if let TypeKind::Reference { name } = &type_def.kind {
                    Some(name.clone())
                } else {
                    None
                }
            });

            Ok((name, type_def, optional))
        }
        ast::GroupEntry::TypeGroupname { ge, .. } => {
            let name = Some(ge.name.ident.to_string());
            let type_def = TypeDef::new(TypeKind::Reference {
                name: ge.name.ident.to_string(),
            });
            let optional = ge
                .occur
                .as_ref()
                .map(|o| is_optional_occurrence(&o.occur))
                .unwrap_or(false);
            Ok((name, type_def, optional))
        }
        ast::GroupEntry::InlineGroup { group, occur, .. } => {
            let elements = convert_group(group)?;
            let optional = occur
                .as_ref()
                .map(|o| is_optional_occurrence(&o.occur))
                .unwrap_or(false);
            Ok((None, TypeDef::new(TypeKind::Array { elements }), optional))
        }
    }
}

fn convert_group_entry(entry: &ast::GroupEntry) -> Result<TypeDef, String> {
    match entry {
        ast::GroupEntry::ValueMemberKey { ge, .. } => convert_type(&ge.entry_type),
        ast::GroupEntry::TypeGroupname { ge, .. } => Ok(TypeDef::new(TypeKind::Reference {
            name: ge.name.ident.to_string(),
        })),
        ast::GroupEntry::InlineGroup { group, .. } => {
            let elements = convert_group(group)?;
            Ok(TypeDef::new(TypeKind::Array { elements }))
        }
    }
}

fn convert_group_entry_to_variant(
    entry: &ast::GroupEntry,
) -> Result<Option<(Option<String>, TypeDef)>, String> {
    match entry {
        ast::GroupEntry::ValueMemberKey { ge, .. } => {
            let name = extract_member_key_name(&ge.member_key);
            let type_def = convert_type(&ge.entry_type)?;
            Ok(Some((name, type_def)))
        }
        ast::GroupEntry::TypeGroupname { ge, .. } => {
            let name = ge.name.ident.to_string();
            let type_def = TypeDef::new(TypeKind::Reference { name: name.clone() });
            Ok(Some((Some(name), type_def)))
        }
        ast::GroupEntry::InlineGroup { group, .. } => {
            let elements = convert_group(group)?;
            Ok(Some((None, TypeDef::new(TypeKind::Array { elements }))))
        }
    }
}

fn extract_member_key_name(mk: &Option<ast::MemberKey>) -> Option<String> {
    match mk {
        Some(ast::MemberKey::Bareword { ident, .. }) => Some(ident.ident.to_string()),
        _ => None,
    }
}

fn is_optional_occurrence(occur: &ast::Occur) -> bool {
    match occur {
        ast::Occur::Optional { .. } => true,
        ast::Occur::ZeroOrMore { .. } => true,
        ast::Occur::Exact { lower, .. } if lower.is_none() || *lower == Some(0) => true,
        _ => false,
    }
}

fn extract_type_name(t2: &Type2) -> Option<String> {
    match t2 {
        Type2::Typename { ident, .. } => Some(ident.ident.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_schema_conversion() {
        let source = r#"
block = [header]
header = [header_body]
header_body = [slot: uint]
"#;
        let ast = cddl::parser::cddl_from_str(source, false).unwrap();
        let schema = cddl_to_ir(&ast).unwrap();

        // Check block type
        let block = schema.get("block").expect("block type should exist");
        println!("block: {:?}", block);

        if let TypeKind::Array { elements } = &block.kind {
            println!("block elements: {:?}", elements);
            assert!(!elements.is_empty(), "block should have elements");
            assert_eq!(elements.len(), 1, "block should have one element");

            let first = &elements[0];
            println!("first element: {:?}", first);
            assert_eq!(
                first.name.as_deref(),
                Some("header"),
                "first element should be named 'header'"
            );
        } else {
            panic!("block should be an Array type, got {:?}", block.kind);
        }
    }

    #[test]
    fn rejects_generic_references() {
        let source = "foo<T> = T\nbar = foo<uint>\n";
        let ast = cddl::parser::cddl_from_str(source, false).unwrap();
        let err = cddl_to_ir(&ast).unwrap_err();

        assert!(err.contains("Generic type 'foo' is not supported yet"));
    }
}
