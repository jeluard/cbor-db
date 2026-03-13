use crate::backend::StorageBackend;
use std::fmt;
use std::ops::Deref;

#[derive(Debug)]
pub enum StoreError {
    NotFound,
    BackendError(String),
    InvalidPathRange {
        start: usize,
        len: usize,
        total: usize,
    },
}

impl std::error::Error for StoreError {}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::NotFound => write!(f, "Key not found"),
            StoreError::BackendError(e) => write!(f, "Backend error: {}", e),
            StoreError::InvalidPathRange { start, len, total } => write!(
                f,
                "Path range [{}..{}) exceeds buffer length {}",
                start,
                start + len,
                total
            ),
        }
    }
}

pub struct Store<B: StorageBackend> {
    backend: B,
}

#[derive(Clone, Debug)]
pub struct Bytes(Vec<u8>);

impl Bytes {
    pub fn new(data: Vec<u8>) -> Self {
        Bytes(data)
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0[..]
    }
}

impl<B: StorageBackend> Store<B> {
    pub fn open(backend: B) -> Result<Self, String> {
        Ok(Self { backend })
    }

    pub fn insert(&self, ns: &[u8], key: &[u8], val: Vec<u8>) -> Result<(), String> {
        self.backend.insert(ns, key, val)
    }

    pub fn update(
        &self,
        ns: &[u8],
        key: &[u8],
        f: &mut dyn FnMut(&mut Vec<u8>),
    ) -> Result<(), String> {
        self.backend.update(ns, key, f)
    }

    pub fn update_at_path(
        &self,
        ns: &[u8],
        key: &[u8],
        path: &[usize],
        f: &mut dyn FnMut(&mut [u8]),
    ) -> Result<(), String> {
        let mut navigation_error = None;
        self.backend.update(
            ns,
            key,
            &mut |value| match crate::navigator::navigate_to_offset(value, path) {
                Ok((start, len)) => {
                    let end = start + len;
                    f(&mut value[start..end]);
                }
                Err(err) => navigation_error = Some(err.to_string()),
            },
        )?;

        if let Some(err) = navigation_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    pub fn delete(&self, ns: &[u8], key: &[u8]) -> Result<(), String> {
        self.backend.delete(ns, key)
    }

    pub fn get(&self, ns: &[u8], key: &[u8]) -> Result<Bytes, StoreError> {
        self.backend
            .get(ns, key)
            .map_err(StoreError::BackendError)?
            .ok_or(StoreError::NotFound)
            .map(Bytes::new)
    }

    pub fn get_range(
        &self,
        ns: &[u8],
        key: &[u8],
        byte_range: (usize, usize),
    ) -> Result<Bytes, StoreError> {
        let data = self
            .backend
            .get(ns, key)
            .map_err(StoreError::BackendError)?
            .ok_or(StoreError::NotFound)?;

        let (start, len) = byte_range;
        let end = start.checked_add(len).ok_or(StoreError::InvalidPathRange {
            start,
            len,
            total: data.len(),
        })?;
        if end > data.len() {
            return Err(StoreError::InvalidPathRange {
                start,
                len,
                total: data.len(),
            });
        }

        Ok(Bytes::new(data[start..end].to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory_backend::MemoryBackend;
    use crate::backend::StorageBackend;

    struct FailingUpdateBackend;

    impl StorageBackend for FailingUpdateBackend {
        fn get(&self, _ns: &[u8], _key: &[u8]) -> Result<Option<Vec<u8>>, String> {
            Ok(None)
        }

        fn insert(&self, _ns: &[u8], _key: &[u8], _val: Vec<u8>) -> Result<(), String> {
            Ok(())
        }

        fn update(
            &self,
            _ns: &[u8],
            _key: &[u8],
            _f: &mut dyn FnMut(&mut Vec<u8>),
        ) -> Result<(), String> {
            Err("update failed".to_string())
        }

        fn delete(&self, _ns: &[u8], _key: &[u8]) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn rejects_out_of_bounds_path_ranges() {
        let store = Store::open(MemoryBackend::new()).unwrap();
        store.insert(b"ns", b"key", b"abc".to_vec()).unwrap();

        let err = store.get_range(b"ns", b"key", (8, 4)).unwrap_err();

        assert!(matches!(
            err,
            StoreError::InvalidPathRange {
                start: 8,
                len: 4,
                total: 3,
            }
        ));
    }

    #[test]
    fn updates_nested_cbor_slice_from_path() {
        let store = Store::open(MemoryBackend::new()).unwrap();
        let value: &[u8] = &[0x82, 0x01, 0x82, 0x43, 0xAA, 0xBB, 0xCC, 0x18, 0x2A];
        store.insert(b"ns", b"key", value.to_vec()).unwrap();

        store
            .update_at_path(b"ns", b"key", &[1, 0], &mut |slice| {
                if slice.len() > 1 {
                    slice[1] ^= 0xFF;
                }
            })
            .unwrap();

        let updated = store.get(b"ns", b"key").unwrap();
        assert_eq!(
            updated.as_ref(),
            &[0x82, 0x01, 0x82, 0x43, 0x55, 0xBB, 0xCC, 0x18, 0x2A]
        );
    }

    #[test]
    fn propagates_update_errors() {
        let store = Store::open(FailingUpdateBackend).unwrap();

        let err = store.update(b"ns", b"key", &mut |_| {}).unwrap_err();

        assert_eq!(err, "update failed");
    }
}
