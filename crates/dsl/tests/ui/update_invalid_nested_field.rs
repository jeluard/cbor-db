use dsl::update;
use store::backend::memory_backend::MemoryBackend;
use store::Store;

fn main() {
    let store = Store::open(MemoryBackend::new()).unwrap();
    let _ = update!(
        store,
        b"row_static",
        b"row-001",
        pool / unknown_field,
        |value: &mut [u8]| {
            let _ = value;
        }
    );
}