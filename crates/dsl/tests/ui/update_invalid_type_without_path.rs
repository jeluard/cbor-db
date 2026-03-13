use dsl::update;
use store::backend::memory_backend::MemoryBackend;
use store::Store;

fn main() {
    let store = Store::open(MemoryBackend::new()).unwrap();
    let _ = update!(store, b"missing_type", b"row-001", |value: &mut Vec<u8>| {
        let _ = value;
    });
}