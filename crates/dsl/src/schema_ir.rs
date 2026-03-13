//! Path-focused schema model used by the proc macros.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaIR {
    pub types: HashMap<String, TypeDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDef {
    pub kind: TypeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeKind {
    FixedUint { bytes: usize },
    FixedBytes { len: usize },
    FixedText { len: usize },
    Array { elements: Vec<FieldDef> },
    Choice { variants: Vec<VariantDef> },
    Primitive(PrimitiveKind),
    Uint,
    Int,
    Bytes,
    Text,
    Reference { name: String },
    Range { min: i128, max: i128 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: Option<String>,
    pub type_def: TypeDef,
    pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantDef {
    pub name: Option<String>,
    pub type_def: TypeDef,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PrimitiveKind {
    Nil,
    Null,
    Bool,
    True,
    False,
    Undefined,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamicReason {
    pub field: String,
    pub reason: DynamicCause,
    pub segment_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DynamicCause {
    Choice { variants: Vec<String> },
    UnconstrainedInt,
    VariableLength,
    Optional,
    VariableRange { min: i128, max: i128 },
    Unknown,
}

impl std::fmt::Display for DynamicReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            DynamicCause::Choice { variants } => {
                write!(
                    f,
                    "field '{}' is a choice between [{}] with variable sizes",
                    self.field,
                    variants.join(" | ")
                )
            }
            DynamicCause::UnconstrainedInt => {
                write!(
                    f,
                    "field '{}' is an unconstrained integer (size depends on value)",
                    self.field
                )
            }
            DynamicCause::VariableLength => {
                write!(
                    f,
                    "field '{}' has variable length (no .size constraint)",
                    self.field
                )
            }
            DynamicCause::Optional => {
                write!(
                    f,
                    "field '{}' is optional (may or may not be present)",
                    self.field
                )
            }
            DynamicCause::VariableRange { min, max } => {
                write!(
                    f,
                    "field '{}' range {}..{} spans multiple CBOR size classes",
                    self.field, min, max
                )
            }
            DynamicCause::Unknown => {
                write!(f, "field '{}' has unknown/unsupported type", self.field)
            }
        }
    }
}

impl SchemaIR {
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&TypeDef> {
        self.types.get(name)
    }

    pub fn resolve<'a>(&'a self, type_def: &'a TypeDef) -> Result<&'a TypeDef, String> {
        match &type_def.kind {
            TypeKind::Reference { name } => {
                let resolved = self
                    .get(name)
                    .ok_or_else(|| format!("Referenced type '{}' not found", name))?;
                self.resolve(resolved)
            }
            _ => Ok(type_def),
        }
    }
}

impl TypeDef {
    pub fn new(kind: TypeKind) -> Self {
        Self { kind }
    }

    pub fn cbor_size(&self, schema: &SchemaIR, field_name: &str) -> Result<usize, DynamicReason> {
        match &self.kind {
            TypeKind::FixedUint { bytes } => Ok(1 + bytes),
            TypeKind::FixedBytes { len } | TypeKind::FixedText { len } => {
                Ok(cbor_header_size(*len) + len)
            }
            TypeKind::Array { elements } => {
                if let Some(optional_field) = elements.iter().find(|field| field.optional) {
                    return Err(DynamicReason {
                        field: optional_field
                            .name
                            .clone()
                            .unwrap_or_else(|| field_name.to_string()),
                        reason: DynamicCause::Optional,
                        segment_index: None,
                    });
                }

                let mut total = cbor_header_size(elements.len());
                for (index, element) in elements.iter().enumerate() {
                    let element_name = element
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("[{}]", index));
                    total += element.type_def.cbor_size(schema, &element_name)?;
                }
                Ok(total)
            }
            TypeKind::Choice { variants } => {
                let Some(first_variant) = variants.first() else {
                    return Err(DynamicReason {
                        field: field_name.to_string(),
                        reason: DynamicCause::Unknown,
                        segment_index: None,
                    });
                };

                let first_name = first_variant
                    .name
                    .clone()
                    .unwrap_or_else(|| "variant0".to_string());
                let first_size = first_variant.type_def.cbor_size(schema, &first_name)?;

                for (index, variant) in variants.iter().enumerate().skip(1) {
                    let variant_name = variant
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("variant{}", index));
                    match variant.type_def.cbor_size(schema, &variant_name) {
                        Ok(size) if size == first_size => {}
                        _ => {
                            return Err(DynamicReason {
                                field: field_name.to_string(),
                                reason: DynamicCause::Choice {
                                    variants: variants
                                        .iter()
                                        .map(|entry| {
                                            entry.name.clone().unwrap_or_else(|| "?".to_string())
                                        })
                                        .collect(),
                                },
                                segment_index: None,
                            });
                        }
                    }
                }

                Ok(first_size)
            }
            TypeKind::Primitive(primitive) => Ok(primitive_cbor_size(*primitive)),
            TypeKind::Uint | TypeKind::Int => Err(DynamicReason {
                field: field_name.to_string(),
                reason: DynamicCause::UnconstrainedInt,
                segment_index: None,
            }),
            TypeKind::Bytes | TypeKind::Text => Err(DynamicReason {
                field: field_name.to_string(),
                reason: DynamicCause::VariableLength,
                segment_index: None,
            }),
            TypeKind::Reference { name } => {
                let resolved = schema.get(name).ok_or_else(|| DynamicReason {
                    field: name.clone(),
                    reason: DynamicCause::Unknown,
                    segment_index: None,
                })?;
                resolved.cbor_size(schema, name)
            }
            TypeKind::Range { min, max } => {
                let min_size = int_cbor_size(*min);
                let max_size = int_cbor_size(*max);
                if min_size == max_size {
                    Ok(min_size)
                } else {
                    Err(DynamicReason {
                        field: field_name.to_string(),
                        reason: DynamicCause::VariableRange {
                            min: *min,
                            max: *max,
                        },
                        segment_index: None,
                    })
                }
            }
        }
    }
}

fn cbor_header_size(len: usize) -> usize {
    if len <= 23 {
        1
    } else if len <= 255 {
        2
    } else if len <= 65535 {
        3
    } else if len <= 4_294_967_295 {
        5
    } else {
        9
    }
}

fn int_cbor_size(value: i128) -> usize {
    let abs_value = if value >= 0 {
        value as u128
    } else {
        (-(value + 1)) as u128
    };

    if abs_value <= 23 {
        1
    } else if abs_value <= 255 {
        2
    } else if abs_value <= 65535 {
        3
    } else if abs_value <= 4_294_967_295 {
        5
    } else {
        9
    }
}

fn primitive_cbor_size(primitive: PrimitiveKind) -> usize {
    match primitive {
        PrimitiveKind::Nil
        | PrimitiveKind::Null
        | PrimitiveKind::Bool
        | PrimitiveKind::True
        | PrimitiveKind::False
        | PrimitiveKind::Undefined => 1,
    }
}
