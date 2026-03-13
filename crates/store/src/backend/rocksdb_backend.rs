use super::{scoped_key, StorageBackend};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::Arc;

pub struct RocksDbBackend {
    db: Arc<DB>,
}

impl RocksDbBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path).map_err(|err| err.to_string())?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl StorageBackend for RocksDbBackend {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let scoped = scoped_key(ns, key);
        self.db
            .get(scoped)
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .map_err(|err| err.to_string())
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        self.db.put(scoped, val).map_err(|err| err.to_string())
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let mut value = self
            .db
            .get(&scoped)
            .map_err(|err| err.to_string())?
            .unwrap_or_default();
        let before = value.clone();
        f(&mut value);
        if value != before {
            self.db.put(scoped, value).map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        self.db
            .delete(scoped_key(ns, key))
            .map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn keeps_namespaces_isolated() {
        let dir = tempdir().unwrap();
        let backend = RocksDbBackend::open(dir.path()).unwrap();

        backend
            .insert(b"alpha", b"shared", b"one".to_vec())
            .unwrap();
        backend.insert(b"beta", b"shared", b"two".to_vec()).unwrap();

        let first = backend.get(b"alpha", b"shared").unwrap().unwrap();
        let second = backend.get(b"beta", b"shared").unwrap().unwrap();

        assert_eq!(&*first, b"one");
        assert_eq!(&*second, b"two");
    }
}
