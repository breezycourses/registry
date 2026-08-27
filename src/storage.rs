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
            // Blob already present (dedup) — drop the duplicate upload.
            tokio::fs::remove_file(&src).await?;
        } else {
            tokio::fs::rename(&src, &dst).await?;
        }
        Ok(size)
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
