use super::{scoped_key, StorageBackend};
use std::path::PathBuf;
use std::sync::Arc;
use surrealkv::{Tree, TreeBuilder};
use tokio::runtime::{Builder, Runtime};

pub struct SurrealKvBackend {
    tree: Tree,
    runtime: Arc<Runtime>,
}

impl SurrealKvBackend {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;
        let tree = {
            let _guard = runtime.enter();
            TreeBuilder::new()
                .with_path(path)
                .build()
                .map_err(|err| err.to_string())?
        };
        Ok(Self {
            tree,
            runtime: Arc::new(runtime),
        })
    }
}

impl StorageBackend for SurrealKvBackend {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let scoped = scoped_key(ns, key);
        let _guard = self.runtime.enter();
        let tx = self.tree.begin().map_err(|err| err.to_string())?;
        tx.get(&scoped)
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .map_err(|err| err.to_string())
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let _guard = self.runtime.enter();
        let mut tx = self.tree.begin().map_err(|err| err.to_string())?;
        tx.set(&scoped, &val).map_err(|err| err.to_string())?;
        self.runtime
            .block_on(tx.commit())
            .map_err(|err| err.to_string())
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let _guard = self.runtime.enter();
        let mut tx = self.tree.begin().map_err(|err| err.to_string())?;
        let mut value = tx
            .get(&scoped)
            .map_err(|err| err.to_string())?
            .unwrap_or_default();
        let before = value.clone();
        f(&mut value);
        if value != before {
            tx.set(&scoped, &value).map_err(|err| err.to_string())?;
            self.runtime
                .block_on(tx.commit())
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let _guard = self.runtime.enter();
        let mut tx = self.tree.begin().map_err(|err| err.to_string())?;
        tx.delete(&scoped).map_err(|err| err.to_string())?;
        self.runtime
            .block_on(tx.commit())
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
        let backend = SurrealKvBackend::open(dir.path().to_path_buf()).unwrap();

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
