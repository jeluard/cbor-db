use super::{scoped_key, StorageBackend};
use std::path::Path;
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyIndexing, KeyShape, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;

const TIDEHUNTER_MUTEXES: usize = 16;
const TIDEHUNTER_HASH_PREFIX_BYTES: usize = 2;

fn db_error(err: impl std::fmt::Debug) -> String {
    format!("{err:?}")
}

pub struct TidehunterBackend {
    db: Arc<Db>,
    key_space: KeySpace,
}

impl TidehunterBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        std::fs::create_dir_all(path.as_ref()).map_err(|err| err.to_string())?;

        let config = Arc::new(Config::small());
        let (key_shape, key_space) = KeyShape::new_single_config_indexing(
            KeyIndexing::hash(),
            TIDEHUNTER_MUTEXES,
            KeyType::prefix_uniform(TIDEHUNTER_HASH_PREFIX_BYTES, 0),
            KeySpaceConfig::default(),
        );
        let db = Db::open(path.as_ref(), key_shape, config, Metrics::new()).map_err(db_error)?;

        Ok(Self { db, key_space })
    }
}

impl StorageBackend for TidehunterBackend {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let scoped = scoped_key(ns, key);
        self.db
            .get(self.key_space, &scoped)
            .map(|value| value.map(|bytes| bytes.as_ref().to_vec()))
            .map_err(db_error)
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        self.db
            .insert(self.key_space, scoped_key(ns, key), val)
            .map_err(db_error)
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let mut value = self
            .db
            .get(self.key_space, &scoped)
            .map_err(db_error)?
            .map(|bytes| bytes.to_vec())
            .unwrap_or_default();
        let before = value.clone();
        f(&mut value);
        if value != before {
            self.db
                .insert(self.key_space, scoped, value)
                .map_err(db_error)?;
        }
        Ok(())
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        self.db
            .remove(self.key_space, scoped_key(ns, key))
            .map_err(db_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn benchmark_like_key(index: u64) -> Vec<u8> {
        let mut seed = index ^ 0xD3AD_B33F;
        let mut key = Vec::with_capacity(24);

        while key.len() < 24 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            key.extend_from_slice(&seed.to_le_bytes());
        }

        let bucket = (index % 4) as u32;
        key[..4].copy_from_slice(&bucket.to_be_bytes());
        key.truncate(24);
        key
    }

    #[test]
    fn keeps_namespaces_isolated() {
        let dir = tempdir().unwrap();
        let backend = TidehunterBackend::open(dir.path()).unwrap();

        backend
            .insert(b"alpha", b"shared", b"one".to_vec())
            .unwrap();
        backend.insert(b"beta", b"shared", b"two".to_vec()).unwrap();

        let first = backend.get(b"alpha", b"shared").unwrap().unwrap();
        let second = backend.get(b"beta", b"shared").unwrap().unwrap();

        assert_eq!(&*first, b"one");
        assert_eq!(&*second, b"two");
    }

    #[test]
    fn round_trips_benchmark_sized_scoped_keys() {
        let dir = tempdir().unwrap();
        let backend = TidehunterBackend::open(dir.path()).unwrap();

        for index in 0..64_u64 {
            let key = benchmark_like_key(index);
            let value = index.to_be_bytes().to_vec();
            backend.insert(b"row_static", &key, value).unwrap();
        }

        for index in 0..64_u64 {
            let key = benchmark_like_key(index);
            let value = backend.get(b"row_static", &key).unwrap().unwrap();
            assert_eq!(&*value, index.to_be_bytes().as_slice());
        }
    }
}
