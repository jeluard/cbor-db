pub trait StorageBackend: 'static {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String>;
    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String>;
    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String>;
    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String>;
}

impl<T> StorageBackend for Box<T>
where
    T: StorageBackend + ?Sized,
{
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        (**self).get(ns, key)
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        (**self).insert(ns, key, val)
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        (**self).update(ns, key, f)
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        (**self).delete(ns, key)
    }
}

#[cfg(feature = "fjall-backend")]
pub mod fjall_backend;
pub mod memory_backend;
#[cfg(feature = "rocksdb-backend")]
pub mod rocksdb_backend;
pub mod sled_backend;
#[cfg(feature = "surrealkv-backend")]
pub mod surrealkv_backend;
#[cfg(feature = "tidehunter-backend")]
pub mod tidehunter_backend;
#[cfg(feature = "turso-backend")]
pub mod turso_backend;

pub(crate) fn scoped_key(ns: &[u8], key: &[u8]) -> Vec<u8> {
    let mut composite = Vec::with_capacity(4 + ns.len() + key.len());
    composite.extend_from_slice(&(ns.len() as u32).to_be_bytes());
    composite.extend_from_slice(ns);
    composite.extend_from_slice(key);
    composite
}
