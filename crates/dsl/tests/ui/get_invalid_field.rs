use dsl::get;
use store::backend::memory_backend::MemoryBackend;
use store::Store;

fn main() {
    let store = Store::open(MemoryBackend::new()).unwrap();
    let _ = get!(store, b"row_static", b"row-001", missing_field);
}