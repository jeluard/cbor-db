use super::{scoped_key, StorageBackend};
use std::{collections::HashMap, sync::RwLock};

pub struct MemoryBackend {
    map: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MemoryBackend {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let scoped = scoped_key(ns, key);
        let map = self
            .map
            .read()
            .map_err(|_| "memory backend lock poisoned".to_string())?;
        Ok(map.get(&scoped).cloned())
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let mut map = self
            .map
            .write()
            .map_err(|_| "memory backend lock poisoned".to_string())?;
        map.insert(scoped, val);
        Ok(())
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let mut map = self
            .map
            .write()
            .map_err(|_| "memory backend lock poisoned".to_string())?;
        let entry = map.entry(scoped).or_default();
        let mut v = entry.clone();
        f(&mut v);
        if entry.as_slice() != v.as_slice() {
            *entry = v;
        }
        Ok(())
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let mut map = self
            .map
            .write()
            .map_err(|_| "memory backend lock poisoned".to_string())?;
        map.remove(&scoped);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_namespaces_isolated() {
        let backend = MemoryBackend::new();

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
