//! Point-read parquet file reader with retained local file descriptors.
//!
//! The default read path opens/stats/closes the parquet file on every `get_range`
//! (via `object_store::LocalFileSystem`). For the get-by-row-id point read that is a
//! per-get syscall cost, amplified by the many small files a merges-off composite index
//! accumulates. [`PointReader`] serves data byte-ranges from a **retained open fd**
//! (`pread` via `read_exact_at`, no re-open) for local files, cached process-wide and
//! keyed by absolute path, and falls back to the object store for anything non-local.
//!
//! Safety/lifecycle: parquet generation files are immutable and uniquely named, so a
//! retained fd stays valid until its generation is dropped; the cache is bounded (LRU-ish
//! eviction) to cap fd usage. A retained handle is only used when the resolved local file
//! exists AND its length matches the catalog's `ObjectMeta.size` (guards a stale/wrong path);
//! otherwise the object-store path is used — never a wrong result.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use prost::bytes::Bytes;

/// Cap on retained open file descriptors.
const MAX_OPEN_FILES: usize = 512;
/// Process-wide cache of open, retained local parquet file descriptors, keyed by absolute path.
static OPEN_FILES: Lazy<DashMap<String, Arc<File>>> = Lazy::new(DashMap::new);

/// Returns a cached (or freshly opened) retained fd for `path`, but only if the file exists
/// and its length matches `expected_size` (guards a stale or mis-resolved path). `None` on any
/// failure → caller falls back to the object store.
fn get_or_open_verified(path: &std::path::Path, expected_size: u64) -> Option<Arc<File>> {
    let key = path.to_str()?.to_string();
    if let Some(entry) = OPEN_FILES.get(&key) {
        return Some(Arc::clone(entry.value()));
    }
    let file = File::open(path).ok()?;
    match file.metadata() {
        Ok(m) if m.len() == expected_size => {}
        _ => return None, // missing / size mismatch → don't retain, fall back
    }
    let arc = Arc::new(file);
    if OPEN_FILES.len() >= MAX_OPEN_FILES {
        // Simple bound: drop one arbitrary entry. Files are unique + immutable, so eviction only
        // costs a future re-open, never correctness.
        // NB: bind the victim key in its OWN statement so the `iter()` shard read-guard is dropped
        // at the `;` BEFORE `remove()` — otherwise `if let`'s temporary-lifetime extension keeps the
        // read guard alive across `remove()` and, when the key hashes to the same shard, the thread
        // self-deadlocks waiting for the shard write lock it already read-locks.
        let victim = OPEN_FILES.iter().next().map(|e| e.key().clone());
        if let Some(k) = victim {
            OPEN_FILES.remove(&k);
        }
    }
    OPEN_FILES.insert(key, Arc::clone(&arc));
    Some(arc)
}

/// An [`AsyncFileReader`] for a single point read: a retained local fd when the file could be
/// opened locally, otherwise the object store. `get_metadata` is never invoked because callers
/// build via `ParquetRecordBatchStreamBuilder::new_with_metadata` (metadata supplied directly).
pub enum PointReader {
    Local(Arc<File>),
    Object(ParquetObjectReader),
}

impl AsyncFileReader for PointReader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        match self {
            PointReader::Local(file) => {
                let file = Arc::clone(file);
                async move {
                    let len = (range.end - range.start) as usize;
                    let mut buf = vec![0u8; len];
                    file.read_exact_at(&mut buf, range.start)
                        .map_err(|e| ParquetError::External(Box::new(e)))?;
                    Ok(Bytes::from(buf))
                }
                .boxed()
            }
            PointReader::Object(r) => r.get_bytes(range),
        }
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        match self {
            PointReader::Local(file) => {
                let file = Arc::clone(file);
                async move {
                    let mut out = Vec::with_capacity(ranges.len());
                    for range in ranges {
                        let len = (range.end - range.start) as usize;
                        let mut buf = vec![0u8; len];
                        file.read_exact_at(&mut buf, range.start)
                            .map_err(|e| ParquetError::External(Box::new(e)))?;
                        out.push(Bytes::from(buf));
                    }
                    Ok(out)
                }
                .boxed()
            }
            PointReader::Object(r) => r.get_byte_ranges(ranges),
        }
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        // Unreachable: point reads use new_with_metadata, so the builder never asks the
        // reader for metadata.
        async {
            Err(ParquetError::General(
                "PointReader: metadata is supplied via new_with_metadata".to_string(),
            ))
        }
        .boxed()
    }
}

/// Builds a [`PointReader`] for `(store, location, size)`: a retained local fd when the file
/// resolves to an existing local path of the expected size, else the object store.
pub fn make_point_reader(store: Arc<dyn ObjectStore>, location: ObjPath, size: u64) -> PointReader {
    // LocalFileSystem::new() is rooted at "/"; path_to_filesystem yields the absolute path when
    // the store's location is absolute (the composite get path uses a default local store).
    if let Ok(fs_path) = LocalFileSystem::new().path_to_filesystem(&location) {
        if let Some(file) = get_or_open_verified(&fs_path, size) {
            return PointReader::Local(file);
        }
    }
    PointReader::Object(ParquetObjectReader::new(store, location).with_file_size(size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::parquet::arrow::async_reader::AsyncFileReader;

    #[tokio::test]
    async fn make_point_reader_uses_retained_fd_for_local_file() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let write = |name: &str, len: usize| {
            let path = dir.path().join(name);
            let payload: Vec<u8> = (0..len as u32).map(|i| (i % 251) as u8).collect();
            std::fs::write(&path, &payload).unwrap();
            // production path shape: absolute path → object_store Path with leading slash dropped.
            (ObjPath::from(path.to_str().unwrap().trim_start_matches('/')), payload)
        };

        // Correct size → retained local fd, and the read returns the right bytes.
        let (loc, payload) = write("gen_ok.bin", 4096);
        let mut r = make_point_reader(Arc::clone(&store), loc, payload.len() as u64);
        assert!(matches!(r, PointReader::Local(_)), "expected retained local fd");
        let bytes = r.get_bytes(100..228).await.unwrap();
        assert_eq!(&bytes[..], &payload[100..228], "retained-fd read mismatch");

        // Size mismatch (distinct path, cache miss) → guarded, falls back to the object store.
        let (loc2, payload2) = write("gen_badsize.bin", 2048);
        let r2 = make_point_reader(Arc::clone(&store), loc2, payload2.len() as u64 + 1);
        assert!(matches!(r2, PointReader::Object(_)), "size mismatch must fall back");

        // Nonexistent path → object store fallback.
        let r3 = make_point_reader(Arc::clone(&store), ObjPath::from("no/such/file.bin"), 10);
        assert!(matches!(r3, PointReader::Object(_)), "missing file must fall back");
    }
}
