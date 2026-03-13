use super::{scoped_key, StorageBackend};
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use turso::{params, Builder as TursoBuilder, Connection, Value};

const CREATE_TABLE_SQL: &str =
    "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)";
const SELECT_VALUE_SQL: &str = "SELECT v FROM kv WHERE k = ?1";
const DELETE_VALUE_SQL: &str = "DELETE FROM kv WHERE k = ?1";
const UPSERT_VALUE_SQL: &str =
    "INSERT INTO kv (k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v";

pub struct TursoBackend {
    connection: Connection,
    runtime: Arc<Runtime>,
}

impl TursoBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;

        let database = runtime
            .block_on(TursoBuilder::new_local(path.as_ref().to_string_lossy().as_ref()).build())
            .map_err(|err| err.to_string())?;
        let connection = database.connect().map_err(|err| err.to_string())?;
        runtime
            .block_on(connection.execute(CREATE_TABLE_SQL, ()))
            .map_err(|err| err.to_string())?;

        Ok(Self {
            connection,
            runtime: Arc::new(runtime),
        })
    }

    fn load_value(&self, scoped: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let mut rows = self
            .runtime
            .block_on(
                self.connection
                    .query(SELECT_VALUE_SQL, params![scoped.to_vec()]),
            )
            .map_err(|err| err.to_string())?;
        let row = self
            .runtime
            .block_on(rows.next())
            .map_err(|err| err.to_string())?;

        match row {
            Some(row) => match row.get_value(0).map_err(|err| err.to_string())? {
                Value::Blob(bytes) => Ok(Some(bytes)),
                other => Err(format!("unexpected Turso value type: {other:?}")),
            },
            None => Ok(None),
        }
    }

    fn store_value(&self, scoped: &[u8], value: &[u8]) -> Result<(), String> {
        self.runtime
            .block_on(
                self.connection
                    .execute(UPSERT_VALUE_SQL, params![scoped.to_vec(), value.to_vec()]),
            )
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

impl StorageBackend for TursoBackend {
    fn get(&self, ns: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let scoped = scoped_key(ns, key);
        self.load_value(&scoped)
    }

    fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        self.store_value(&scoped_key(ns, key), &val)
    }

    fn update(&self, ns: &[u8], key: &[u8], f: &mut dyn FnMut(&mut Vec<u8>)) -> Result<(), String> {
        let scoped = scoped_key(ns, key);
        let mut value = self.load_value(&scoped)?.unwrap_or_default();
        let before = value.clone();
        f(&mut value);
        if value != before {
            self.store_value(&scoped, &value)?;
        }
        Ok(())
    }

    fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        self.runtime
            .block_on(
                self.connection
                    .execute(DELETE_VALUE_SQL, params![scoped_key(ns, key)]),
            )
            .map(|_| ())
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
        let backend = TursoBackend::open(dir.path().join("store.db")).unwrap();

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
