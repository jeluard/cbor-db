//! Build a navigation plan from a schema path.

use crate::schema_ir::{DynamicCause, DynamicReason, FieldDef, SchemaIR, TypeDef, TypeKind};

#[derive(Debug, Clone, PartialEq)]
pub enum PathPlanKind {
    WholeValue,
    StaticSlice { offset: usize, size: usize },
    RuntimeSlice { offset: usize, indices: Vec<usize> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamicRequirement {
    pub reason: DynamicReason,
    pub static_segment_count: usize,
    pub total_segments: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathPlan {
    pub kind: PathPlanKind,
    pub requirement: Option<DynamicRequirement>,
}

impl PathPlan {
    pub fn whole_value() -> Self {
        Self {
            kind: PathPlanKind::WholeValue,
            requirement: None,
        }
    }

    pub fn requires_dynamic(&self) -> bool {
        self.requirement.is_some()
    }

    pub fn format_dynamic_error(&self, path_segments: &[String]) -> String {
        let Some(requirement) = &self.requirement else {
            return "unknown reason".to_string();
        };

        let static_part = if requirement.static_segment_count > 0 {
            path_segments[..requirement.static_segment_count].join(" / ")
        } else {
            "root".to_string()
        };

        let dynamic_segment = if requirement.static_segment_count < path_segments.len() {
            &path_segments[requirement.static_segment_count]
        } else {
            "target"
        };

        format!(
            "Static navigation fails at '{}' (segment {}/{}): {}. \
             Static offset to '{}': {} bytes. \
             Add ', dynamic = true' to enable hybrid navigation.",
            dynamic_segment,
            requirement.static_segment_count + 1,
            requirement.total_segments,
            requirement.reason,
            static_part,
            self.offset()
        )
    }

    pub fn offset(&self) -> usize {
        match &self.kind {
            PathPlanKind::WholeValue => 0,
            PathPlanKind::StaticSlice { offset, .. }
            | PathPlanKind::RuntimeSlice { offset, .. } => *offset,
        }
    }
}

pub fn analyze_path(
    path: &[&str],
    root_type: &TypeDef,
    schema: &SchemaIR,
) -> Result<PathPlan, String> {
    if path.is_empty() {
        return Ok(PathPlan::whole_value());
    }

    let mut current_type = root_type;
    let mut static_offset = 0;
    let mut static_segment_count = 0;
    let mut runtime_indices = Vec::new();
    let mut requirement = None;

    for (segment_index, segment) in path.iter().enumerate() {
        let resolved = schema.resolve(current_type)?;
        match &resolved.kind {
            TypeKind::Array { elements } => {
                let (field_index, field) = find_field_by_name(elements, segment)?;

                if requirement.is_some() {
                    runtime_indices.push(field_index);
                } else {
                    match offset_to_field(elements, field_index, schema) {
                        Ok(field_offset) => {
                            static_offset += cbor_array_header_size(elements.len()) + field_offset;
                            static_segment_count = segment_index + 1;
                        }
                        Err(reason) => {
                            requirement = Some(DynamicRequirement {
                                reason: DynamicReason {
                                    field: segment.to_string(),
                                    reason: reason.reason,
                                    segment_index: Some(segment_index),
                                },
                                static_segment_count,
                                total_segments: path.len(),
                            });
                            runtime_indices.push(field_index);
                        }
                    }
                }

                current_type = &field.type_def;
            }
            TypeKind::Choice { variants } => {
                if requirement.is_none() {
                    requirement = Some(DynamicRequirement {
                        reason: DynamicReason {
                            field: segment.to_string(),
                            reason: DynamicCause::Choice {
                                variants: variants
                                    .iter()
                                    .map(|variant| {
                                        variant.name.clone().unwrap_or_else(|| "?".to_string())
                                    })
                                    .collect(),
                            },
                            segment_index: Some(segment_index),
                        },
                        static_segment_count,
                        total_segments: path.len(),
                    });
                }

                let variant = variants
                    .iter()
                    .find(|variant| variant.name.as_deref() == Some(*segment))
                    .ok_or_else(|| {
                        format!(
                            "Segment '{}' not found in choice variants at segment {}",
                            segment, segment_index
                        )
                    })?;
                current_type = &variant.type_def;
            }
            other => {
                return Err(format!(
                    "Cannot navigate into {} at segment '{}' (segment {})",
                    type_kind_name(other),
                    segment,
                    segment_index
                ));
            }
        }
    }

    if let Some(existing_requirement) = requirement {
        return Ok(PathPlan {
            kind: PathPlanKind::RuntimeSlice {
                offset: static_offset,
                indices: runtime_indices,
            },
            requirement: Some(existing_requirement),
        });
    }

    let target_name = path.last().copied().unwrap_or("target");
    match current_type.cbor_size(schema, target_name) {
        Ok(size) => Ok(PathPlan {
            kind: PathPlanKind::StaticSlice {
                offset: static_offset,
                size,
            },
            requirement: None,
        }),
        Err(reason) => Ok(PathPlan {
            kind: PathPlanKind::RuntimeSlice {
                offset: static_offset,
                indices: Vec::new(),
            },
            requirement: Some(DynamicRequirement {
                reason,
                static_segment_count,
                total_segments: path.len(),
            }),
        }),
    }
}

fn find_field_by_name<'a>(
    elements: &'a [FieldDef],
    name: &str,
) -> Result<(usize, &'a FieldDef), String> {
    for (idx, field) in elements.iter().enumerate() {
        if field.name.as_deref() == Some(name) {
            return Ok((idx, field));
        }
    }

    let available: Vec<String> = elements.iter().filter_map(|f| f.name.clone()).collect();
    Err(format!(
        "Field '{}' not found. Available fields: [{}]",
        name,
        available.join(", ")
    ))
}

fn offset_to_field(
    elements: &[FieldDef],
    target_idx: usize,
    schema: &SchemaIR,
) -> Result<usize, DynamicReason> {
    let mut offset = 0;

    for (idx, field) in elements.iter().enumerate() {
        if idx == target_idx {
            return Ok(offset);
        }

        if field.optional {
            return Err(DynamicReason {
                field: field.name.clone().unwrap_or_else(|| format!("[{}]", idx)),
                reason: DynamicCause::Optional,
                segment_index: Some(idx),
            });
        }
        let field_name = field.name.clone().unwrap_or_else(|| format!("[{}]", idx));
        let field_size = field.type_def.cbor_size(schema, &field_name)?;
        offset += field_size;
    }
    Err(DynamicReason {
        field: "target".to_string(),
        reason: DynamicCause::Unknown,
        segment_index: None,
    })
}

fn cbor_array_header_size(count: usize) -> usize {
    if count <= 23 {
        1
    } else if count <= 255 {
        2
    } else if count <= 65535 {
        3
    } else if count <= 4294967295 {
        5
    } else {
        9
    }
}

fn type_kind_name(kind: &TypeKind) -> &'static str {
    match kind {
        TypeKind::FixedUint { .. } => "fixed uint",
        TypeKind::FixedBytes { .. } => "fixed bytes",
        TypeKind::FixedText { .. } => "fixed text",
        TypeKind::Array { .. } => "array",
        TypeKind::Choice { .. } => "choice",
        TypeKind::Primitive(_) => "primitive",
        TypeKind::Uint => "uint",
        TypeKind::Int => "int",
        TypeKind::Bytes => "bytes",
        TypeKind::Text => "text",
        TypeKind::Reference { .. } => "reference",
        TypeKind::Range { .. } => "range",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_ir::{FieldDef, TypeDef, TypeKind, VariantDef};

    fn make_test_schema() -> SchemaIR {
        let mut schema = SchemaIR::new();

        // uint .size 8 -> 9 bytes
        let uint64_type = TypeDef::new(TypeKind::FixedUint { bytes: 8 });

        // bytes .size 32 -> 34 bytes
        let hash32_type = TypeDef::new(TypeKind::FixedBytes { len: 32 });

        // nil -> 1 byte
        let nil_type = TypeDef::new(TypeKind::Primitive(crate::schema_ir::PrimitiveKind::Nil));

        // hash32 / nil (choice)
        let optional_hash = TypeDef::new(TypeKind::Choice {
            variants: vec![
                VariantDef {
                    name: Some("hash32".to_string()),
                    type_def: hash32_type.clone(),
                },
                VariantDef {
                    name: Some("nil".to_string()),
                    type_def: nil_type.clone(),
                },
            ],
        });

        // header_body = [ block_number, slot, prev_hash, ... ]
        let header_body = TypeDef::new(TypeKind::Array {
            elements: vec![
                FieldDef {
                    name: Some("block_number".to_string()),
                    type_def: TypeDef::new(TypeKind::Reference {
                        name: "block_number".to_string(),
                    }),
                    optional: false,
                },
                FieldDef {
                    name: Some("slot".to_string()),
                    type_def: TypeDef::new(TypeKind::Reference {
                        name: "slot".to_string(),
                    }),
                    optional: false,
                },
                FieldDef {
                    name: Some("prev_hash".to_string()),
                    type_def: optional_hash,
                    optional: false,
                },
            ],
        });

        // header = [ header_body ]
        let header = TypeDef::new(TypeKind::Array {
            elements: vec![FieldDef {
                name: Some("header_body".to_string()),
                type_def: header_body,
                optional: false,
            }],
        });

        // block = [ header ]
        let block = TypeDef::new(TypeKind::Array {
            elements: vec![FieldDef {
                name: Some("header".to_string()),
                type_def: header,
                optional: false,
            }],
        });

        schema
            .types
            .insert("block_number".to_string(), uint64_type.clone());
        schema.types.insert("slot".to_string(), uint64_type);
        schema.types.insert("hash32".to_string(), hash32_type);
        schema.types.insert("block".to_string(), block);

        schema
    }

    #[test]
    fn test_static_path() {
        let schema = make_test_schema();
        let root = schema.get("block").unwrap();

        // block / header / header_body / slot should be fully static
        let result = analyze_path(&["header", "header_body", "slot"], root, &schema).unwrap();

        assert_eq!(
            result.kind,
            PathPlanKind::StaticSlice {
                offset: 12,
                size: 9,
            }
        );
        assert!(!result.requires_dynamic());
    }

    #[test]
    fn test_dynamic_path() {
        let schema = make_test_schema();
        let root = schema.get("block").unwrap();

        let result = analyze_path(&["header", "header_body", "prev_hash"], root, &schema).unwrap();

        assert_eq!(
            result.kind,
            PathPlanKind::RuntimeSlice {
                offset: 21,
                indices: Vec::new(),
            }
        );
        assert!(result.requires_dynamic());
    }
}
