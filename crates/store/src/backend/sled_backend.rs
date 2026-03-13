use super::{scoped_key, StorageBackend};
use sled::{Db, IVec};

pub struct SledBackend {
    db: Db,
}

impl SledBackend {
    pub fn open(path: &str) -> Result<Self, String> {
        let db = sled::open(path).map_err(|e| e.to_string())?;
        Ok(Self { db })
    }
}

impl StorageBackend for SledBackend {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let scoped = scoped_key(ns, key);
        Ok(self
            .db
            .get(scoped)
            .map_err(|e| e.to_string())?
            .map(|ivec| ivec.to_vec()))
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        self.db
            .insert(scoped, IVec::from(val))
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        self.db
            .fetch_and_update(scoped, |old| {
                let mut v = old.map(|b| b.to_vec()).unwrap_or_default();
                let before = v.clone();
                f(&mut v);
                if v == before {
                    old.map(|existing| existing.to_vec())
                } else {
                    Some(v)
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        self.db
            .remove(scoped_key(ns, key))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn keeps_namespaces_isolated() {
        let db_path = std::env::temp_dir().join(format!(
            "cbor-db-sled-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let backend = SledBackend::open(db_path.to_str().unwrap()).unwrap();
        backend
            .insert(b"alpha", b"shared", b"one".to_vec())
            .unwrap();
        backend.insert(b"beta", b"shared", b"two".to_vec()).unwrap();

        let first = backend.get(b"alpha", b"shared").unwrap().unwrap();
        let second = backend.get(b"beta", b"shared").unwrap().unwrap();

        assert_eq!(&*first, b"one");
        assert_eq!(&*second, b"two");

        drop(backend);
        let _ = std::fs::remove_dir_all(db_path);
    }
}
