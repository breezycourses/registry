//! Object storage as the source of truth (the Continuity model): blobs and
//! manifests are immutable objects, and each repo's tiny mutable state lives in
//! one index object updated via compare-and-swap. Two backends: a local
//! filesystem "bucket" (dev / benchmarks / single node) and real S3-compatible
//! storage via the `object_store` crate.

use async_trait::async_trait;
use object_store::ObjectStore as _;
use object_store::ObjectStoreExt as _;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum Fetch {
    NotModified,
    New(Vec<u8>, String),
    Missing,
}

#[derive(Debug)]
pub enum Cas {
    Ok(String),
    Conflict,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &str) -> anyhow::Result<Option<(Vec<u8>, String)>>;
    /// Conditional GET: `etag` from a previous read; NotModified is the fast path.
    async fn get_if_none_match(&self, key: &str, etag: &str) -> anyhow::Result<Fetch>;
    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<String>;
    /// CAS: `etag` None means create-only (fail if the key exists).
    async fn put_if_match(&self, key: &str, bytes: &[u8], etag: Option<&str>)
        -> anyhow::Result<Cas>;
    async fn head(&self, key: &str) -> anyhow::Result<bool>;
    async fn delete(&self, key: &str) -> anyhow::Result<()>;
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>>;
    async fn put_file(&self, key: &str, path: &Path) -> anyhow::Result<()>;
    /// Returns false if the key doesn't exist.
    async fn get_to_file(&self, key: &str, path: &Path) -> anyhow::Result<bool>;
}

fn content_etag(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(&sha2::Sha256::digest(bytes)[..16])
}

// ---------------------------------------------------------------------------
// Filesystem bucket. CAS safety across processes comes from an exclusive flock
// on a per-key lock file; etags are content hashes kept in a sidecar.
// ---------------------------------------------------------------------------

pub struct FsObjectStore {
    root: PathBuf,
}

impl FsObjectStore {
    pub fn new(root: &str) -> anyhow::Result<Self> {
        std::fs::create_dir_all(root)?;
        Ok(FsObjectStore { root: PathBuf::from(root) })
    }

    fn path_of(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn etag_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.__etag"))
    }

    fn lock_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.__lock"))
    }

    fn read_etag(&self, key: &str) -> Option<String> {
        std::fs::read_to_string(self.etag_path(key)).ok()
    }

    /// Every write goes through a uniquely-named temp file + rename, so
    /// concurrent writers of the same key can never observe (or destroy) each
    /// other's half-written state.
    fn rename_into_place(&self, bytes: &[u8], dst: &PathBuf) -> anyhow::Result<()> {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.root.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, dst)?;
        Ok(())
    }

    fn write_atomic(&self, key: &str, bytes: &[u8]) -> anyhow::Result<String> {
        let etag = content_etag(bytes);
        self.rename_into_place(bytes, &self.path_of(key))?;
        self.rename_into_place(etag.as_bytes(), &self.etag_path(key))?;
        Ok(etag)
    }

    /// All the sync bodies run on the blocking pool so flock waits and file
    /// copies never stall the async runtime.
    async fn blocking<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(FsObjectStore) -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let this = FsObjectStore { root: self.root.clone() };
        tokio::task::spawn_blocking(move || f(this)).await?
    }
}

#[async_trait]
impl ObjectStore for FsObjectStore {
    async fn get(&self, key: &str) -> anyhow::Result<Option<(Vec<u8>, String)>> {
        let key = key.to_string();
        self.blocking(move |s| match std::fs::read(s.path_of(&key)) {
            Ok(bytes) => {
                let etag = s.read_etag(&key).unwrap_or_else(|| content_etag(&bytes));
                Ok(Some((bytes, etag)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        })
        .await
    }

    async fn get_if_none_match(&self, key: &str, etag: &str) -> anyhow::Result<Fetch> {
        let (key, etag) = (key.to_string(), etag.to_string());
        self.blocking(move |s| {
            if s.read_etag(&key).as_deref() == Some(etag.as_str()) {
                return Ok(Fetch::NotModified);
            }
            match std::fs::read(s.path_of(&key)) {
                Ok(bytes) => {
                    let new_etag = s.read_etag(&key).unwrap_or_else(|| content_etag(&bytes));
                    if new_etag == etag {
                        Ok(Fetch::NotModified)
                    } else {
                        Ok(Fetch::New(bytes, new_etag))
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Fetch::Missing),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<String> {
        let (key, bytes) = (key.to_string(), bytes.to_vec());
        self.blocking(move |s| s.write_atomic(&key, &bytes)).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        bytes: &[u8],
        etag: Option<&str>,
    ) -> anyhow::Result<Cas> {
        let (key, bytes, etag) = (key.to_string(), bytes.to_vec(), etag.map(String::from));
        self.blocking(move |s| {
            use fs2::FileExt;
            let lock_path = s.lock_path(&key);
            if let Some(parent) = lock_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_path)?;
            lock.lock_exclusive()?;
            let current = s.read_etag(&key);
            let outcome = match (etag.as_deref(), current.as_deref()) {
                (None, None) => Cas::Ok(s.write_atomic(&key, &bytes)?),
                (Some(expected), Some(actual)) if expected == actual => {
                    Cas::Ok(s.write_atomic(&key, &bytes)?)
                }
                _ => Cas::Conflict,
            };
            fs2::FileExt::unlock(&lock)?;
            Ok(outcome)
        })
        .await
    }

    async fn head(&self, key: &str) -> anyhow::Result<bool> {
        let key = key.to_string();
        self.blocking(move |s| Ok(s.path_of(&key).exists())).await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let key = key.to_string();
        self.blocking(move |s| {
            for p in [s.path_of(&key), s.etag_path(&key), s.lock_path(&key)] {
                match std::fs::remove_file(p) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(())
        })
        .await
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let prefix = prefix.to_string();
        self.blocking(move |s| {
            let mut out = vec![];
            let base = s.root.clone();
            let start = base.join(&prefix);
            if !start.exists() {
                return Ok(out);
            }
            let mut stack = vec![start];
            while let Some(dir) = stack.pop() {
                for entry in std::fs::read_dir(&dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if path.is_dir() {
                        stack.push(path);
                    } else if !fname.starts_with(".tmp-")
                        && !fname.ends_with(".__etag")
                        && !fname.ends_with(".__lock")
                    {
                        if let Ok(rel) = path.strip_prefix(&base) {
                            out.push(rel.to_string_lossy().to_string());
                        }
                    }
                }
            }
            Ok(out)
        })
        .await
    }

    async fn put_file(&self, key: &str, path: &Path) -> anyhow::Result<()> {
        let (key, path) = (key.to_string(), path.to_path_buf());
        self.blocking(move |s| {
            let dst = s.path_of(&key);
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let tmp = s.root.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
            std::fs::copy(&path, &tmp)?;
            std::fs::rename(&tmp, &dst)?;
            Ok(())
        })
        .await
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> anyhow::Result<bool> {
        let (key, path) = (key.to_string(), path.to_path_buf());
        self.blocking(move |s| match std::fs::copy(s.path_of(&key), &path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// S3-compatible backend (AWS, MinIO, R2, ...) via the object_store crate.
// CAS uses conditional PUT (If-Match / If-None-Match), which S3 has supported
// natively since late 2024.
// ---------------------------------------------------------------------------

pub struct S3Store {
    inner: object_store::aws::AmazonS3,
}

impl S3Store {
    pub fn new(cfg: &crate::config::ObjectStorageCfg) -> anyhow::Result<Self> {
        use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
        let mut b = AmazonS3Builder::from_env()
            .with_bucket_name(cfg.bucket.as_deref().unwrap_or("breezy"))
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(endpoint) = &cfg.endpoint {
            b = b
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false);
        }
        if let Some(region) = &cfg.region {
            b = b.with_region(region);
        }
        if let Some(ak) = &cfg.access_key {
            b = b.with_access_key_id(ak);
        }
        if let Some(sk) = &cfg.secret_key {
            b = b.with_secret_access_key(sk);
        }
        Ok(S3Store { inner: b.build()? })
    }
}

fn opath(key: &str) -> object_store::path::Path {
    object_store::path::Path::from(key)
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn get(&self, key: &str) -> anyhow::Result<Option<(Vec<u8>, String)>> {
        match self.inner.get(&opath(key)).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone().unwrap_or_default();
                let bytes = r.bytes().await?;
                Ok(Some((bytes.to_vec(), etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_if_none_match(&self, key: &str, etag: &str) -> anyhow::Result<Fetch> {
        let opts = object_store::GetOptions {
            if_none_match: Some(etag.to_string()),
            ..Default::default()
        };
        match self.inner.get_opts(&opath(key), opts).await {
            Ok(r) => {
                let new_etag = r.meta.e_tag.clone().unwrap_or_default();
                let bytes = r.bytes().await?;
                Ok(Fetch::New(bytes.to_vec(), new_etag))
            }
            Err(object_store::Error::NotModified { .. }) => Ok(Fetch::NotModified),
            Err(object_store::Error::NotFound { .. }) => Ok(Fetch::Missing),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<String> {
        let r = self
            .inner
            .put(&opath(key), object_store::PutPayload::from(bytes.to_vec()))
            .await?;
        Ok(r.e_tag.unwrap_or_default())
    }

    async fn put_if_match(
        &self,
        key: &str,
        bytes: &[u8],
        etag: Option<&str>,
    ) -> anyhow::Result<Cas> {
        use object_store::{PutMode, PutOptions, UpdateVersion};
        let mode = match etag {
            None => PutMode::Create,
            Some(e) => PutMode::Update(UpdateVersion {
                e_tag: Some(e.to_string()),
                version: None,
            }),
        };
        let opts = PutOptions { mode, ..Default::default() };
        match self
            .inner
            .put_opts(&opath(key), object_store::PutPayload::from(bytes.to_vec()), opts)
            .await
        {
            Ok(r) => Ok(Cas::Ok(r.e_tag.unwrap_or_default())),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => Ok(Cas::Conflict),
            // Some backends (MinIO in single-disk mode) reject `If-None-Match: *`
            // creates with a bogus NotFound. Fall back to head+put: not atomic,
            // but only the very first write of a brand-new repo index takes this
            // path, and only on those backends (AWS S3 and R2 create atomically).
            Err(object_store::Error::NotFound { .. }) if etag.is_none() => {
                if self.head(key).await? {
                    return Ok(Cas::Conflict);
                }
                let r = self
                    .inner
                    .put(&opath(key), object_store::PutPayload::from(bytes.to_vec()))
                    .await?;
                Ok(Cas::Ok(r.e_tag.unwrap_or_default()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn head(&self, key: &str) -> anyhow::Result<bool> {
        match self.inner.head(&opath(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self.inner.delete(&opath(key)).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        use futures_util::TryStreamExt;
        let prefix = opath(prefix.trim_end_matches('/'));
        let items: Vec<_> = self.inner.list(Some(&prefix)).try_collect().await?;
        Ok(items.into_iter().map(|m| m.location.to_string()).collect())
    }

    async fn put_file(&self, key: &str, path: &Path) -> anyhow::Result<()> {
        use tokio::io::AsyncReadExt;
        let meta = tokio::fs::metadata(path).await?;
        // Single PUT for small files; multipart streaming above 16 MiB.
        if meta.len() <= 16 * 1024 * 1024 {
            let bytes = tokio::fs::read(path).await?;
            self.inner
                .put(&opath(key), object_store::PutPayload::from(bytes))
                .await?;
            return Ok(());
        }
        let mut upload = self.inner.put_multipart(&opath(key)).await?;
        let mut file = tokio::fs::File::open(path).await?;
        let mut buf = vec![0u8; 8 * 1024 * 1024];
        loop {
            let mut filled = 0;
            while filled < buf.len() {
                let n = file.read(&mut buf[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }
            use object_store::MultipartUpload;
            upload
                .put_part(object_store::PutPayload::from(buf[..filled].to_vec()))
                .await?;
            if filled < buf.len() {
                break;
            }
        }
        use object_store::MultipartUpload;
        upload.complete().await?;
        Ok(())
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> anyhow::Result<bool> {
        use futures_util::StreamExt;
        use tokio::io::AsyncWriteExt;
        match self.inner.get(&opath(key)).await {
            Ok(r) => {
                let mut stream = r.into_stream();
                let mut f = tokio::fs::File::create(path).await?;
                while let Some(chunk) = stream.next().await {
                    f.write_all(&chunk?).await?;
                }
                f.flush().await?;
                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fs_cas() {
        let dir = tempdir();
        let store = FsObjectStore::new(&dir).unwrap();
        // Create-only succeeds once.
        let e1 = match store.put_if_match("a/index.json", b"v1", None).await.unwrap() {
            Cas::Ok(e) => e,
            Cas::Conflict => panic!("create failed"),
        };
        assert!(matches!(
            store.put_if_match("a/index.json", b"v1b", None).await.unwrap(),
            Cas::Conflict
        ));
        // Update with the right etag succeeds; a stale etag conflicts.
        let e2 = match store.put_if_match("a/index.json", b"v2", Some(&e1)).await.unwrap() {
            Cas::Ok(e) => e,
            Cas::Conflict => panic!("update failed"),
        };
        assert!(matches!(
            store.put_if_match("a/index.json", b"v3", Some(&e1)).await.unwrap(),
            Cas::Conflict
        ));
        // Conditional GET: NotModified on current etag, New on stale.
        assert!(matches!(
            store.get_if_none_match("a/index.json", &e2).await.unwrap(),
            Fetch::NotModified
        ));
        assert!(matches!(
            store.get_if_none_match("a/index.json", &e1).await.unwrap(),
            Fetch::New(_, _)
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    fn tempdir() -> String {
        let dir = std::env::temp_dir().join(format!("breezy-os-test-{}", uuid::Uuid::new_v4()));
        dir.to_string_lossy().to_string()
    }
}
