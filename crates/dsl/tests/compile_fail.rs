use std::path::PathBuf;

#[test]
fn invalid_types_and_paths_are_rejected_at_compile_time() {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schemas/conway.cddl")
        .canonicalize()
        .expect("schema path should resolve");

    std::env::set_var("CBOR_DB_SCHEMA", &schema_path);

    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/*.rs");
}
