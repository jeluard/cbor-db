use super::{scoped_key, StorageBackend};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::path::Path;

pub struct FjallBackend {
    keyspace: Keyspace,
}

impl FjallBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let db = Database::builder(path)
            .open()
            .map_err(|err| err.to_string())?;
        let keyspace = db
            .keyspace("default", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;
        Ok(Self { keyspace })
    }
}

impl StorageBackend for FjallBackend {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let scoped = scoped_key(ns, key);
        self.keyspace
            .get(scoped)
            .map(|value| value.map(|bytes| bytes.as_ref().to_vec()))
            .map_err(|err| err.to_string())
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        self.keyspace
            .insert(scoped_key(ns, key), val)
            .map_err(|err| err.to_string())
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let mut value = self
            .keyspace
            .get(&scoped)
            .map_err(|err| err.to_string())?
            .map(|bytes| bytes.as_ref().to_vec())
            .unwrap_or_default();
        let before = value.clone();
        f(&mut value);
        if value != before {
            self.keyspace
                .insert(scoped, value)
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        self.keyspace
            .remove(scoped_key(ns, key))
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
        let backend = FjallBackend::open(dir.path()).unwrap();

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
