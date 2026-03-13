use dsl::get;
use store::backend::memory_backend::MemoryBackend;
use store::Store;

#[test]
fn get_dynamic_target_returns_only_the_target_value() {
    let store = Store::open(MemoryBackend::new()).unwrap();
    let block_data: &[u8] = &[
        0x81, // block: array(1)
        0x81, // header: array(1)
        0x84, // header_body: array(4)
        0x1B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, // block_number: 1000
        0x1B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xD0, // slot: 2000
        0xF6, // prev_hash: nil
        0x82, 0x0A, 0x01, // protocol_version: [10, 1]
    ];

    store
        .insert(b"block", b"block-001", block_data.into())
        .unwrap();

    let protocol_version = get!(
        store,
        b"block",
        b"block-001",
        header / header_body / protocol_version,
        dynamic = true
    )
    .unwrap();

    assert_eq!(protocol_version.as_ref(), &[0x82, 0x0A, 0x01]);
}
