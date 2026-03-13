use serde::{Deserialize, Serialize};

/// Full schema representation stored alongside the database.
/// This can include types, bit ranges, fields, and related metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSchema {
    /// Raw schema text (the original CDDL)
    pub cddl_source: String,

    /// Generated internal forms
    pub types: Vec<SchemaType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaType {
    pub name: String,
    pub bit_ranges: Vec<TypeRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeRange {
    pub field: String,
    pub start: usize,
    pub len: usize,
}

/// Reserved key where we store the schema
pub const SCHEMA_KEY: &[u8] = b"_schema";
