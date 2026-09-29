use std::path::PathBuf;

/// Content-addressed blob store on the local filesystem.
/// Layout: <root>/blobs/sha256/ab/abcd… and <root>/staging/<uuid> for in-flight uploads.
/// An S3 implementation can slot in behind these same six methods later.
#[derive(Clone)]
pub struct Store {
    root: PathBuf,
}

impl Store {
    pub fn new(data_dir: &str) -> anyhow::Result<Store> {
        let root = PathBuf::from(data_dir);
        std::fs::create_dir_all(root.join("staging"))?;
        std::fs::create_dir_all(root.join("blobs"))?;
        Ok(Store { root })
    }

    pub fn blob_path(&self, digest: &str) -> PathBuf {
        let (algo, hex) = digest.split_once(':').unwrap_or(("sha256", digest));
        self.root.join("blobs").join(algo).join(&hex[..2]).join(hex)
    }

    pub fn staging_path(&self, uuid: &str) -> PathBuf {
        self.root.join("staging").join(uuid)
    }

    pub async fn create_staging(&self, uuid: &str) -> std::io::Result<()> {
        tokio::fs::File::create(self.staging_path(uuid)).await?;
        Ok(())
    }

    /// Move a verified staging file into the content-addressed location. Returns its size.
    pub async fn commit(&self, uuid: &str, digest: &str) -> std::io::Result<u64> {
        let src = self.staging_path(uuid);
        let dst = self.blob_path(digest);
        let size = tokio::fs::metadata(&src).await?.len();
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tokio::fs::metadata(&dst).await.is_ok() {
            // Blob already present (dedup) — drop the duplicate upload. But
            // first refresh the kept file's mtime: GC's local-cache sweep
            // reclaims row-less files older than the grace window, and the
            // file we are about to reuse may be exactly such a leftover with
            // its row still to be written by our caller. A fresh mtime puts
            // it back inside the window until the row lands. If the sweep
            // unlinked it between the existence check and the touch, fall
            // through to the rename so the upload still ends up in place.
            match touch(&dst).await {
                Ok(()) => tokio::fs::remove_file(&src).await?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tokio::fs::rename(&src, &dst).await?
                }
                Err(e) => return Err(e),
            }
        } else {
            tokio::fs::rename(&src, &dst).await?;
        }
        Ok(size)
    }

    /// The directory the content-addressed blob files live under.
    pub fn blobs_root(&self) -> PathBuf {
        self.root.join("blobs")
    }

    pub async fn open(&self, digest: &str) -> std::io::Result<tokio::fs::File> {
        tokio::fs::File::open(self.blob_path(digest)).await
    }

    pub async fn delete(&self, digest: &str) -> std::io::Result<()> {
        match tokio::fs::remove_file(self.blob_path(digest)).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    pub async fn delete_staging(&self, uuid: &str) {
        let _ = tokio::fs::remove_file(self.staging_path(uuid)).await;
    }
}

/// Set `path`'s mtime to now without rewriting its contents.
async fn touch(path: &std::path::Path) -> std::io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)?
            .set_modified(std::time::SystemTime::now())
    })
    .await?
}
