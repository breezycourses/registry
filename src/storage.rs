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
    ///
    /// The placement step runs under the DB write lock, the same lock GC's
    /// local-cache sweep holds while it decides a row-less file is garbage
    /// and unlinks it. Serializing the two means either the sweep ran first
    /// (the file is gone, we rename a fresh one in) or we ran first (the
    /// file's mtime is fresh, so the sweep's re-stat skips it): a commit
    /// can never land between the sweep's check and its unlink, and the row
    /// our caller writes afterwards always points at a file.
    pub async fn commit(&self, uuid: &str, digest: &str) -> std::io::Result<u64> {
        let src = self.staging_path(uuid);
        let dst = self.blob_path(digest);
        let size = tokio::fs::metadata(&src).await?.len();
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let (s, d) = (src.clone(), dst.clone());
        let placed = tokio::task::spawn_blocking(move || place(&s, &d)).await??;
        if let Some(touch_err) = placed {
            // The existing file can't be refreshed (owned by another user,
            // say) and the sweep may reclaim it before our caller's row
            // lands. Replace it — but only with bytes proven to match the
            // digest, because a read-through fill's staged copy is
            // unverified and a bad bucket response must never overwrite a
            // good cached blob. The hash runs outside the lock: it reads the
            // whole blob, and nothing about it needs serializing. Only the
            // rename does, and that is one syscall.
            let actual = {
                let s = src.clone();
                tokio::task::spawn_blocking(move || sha256_of_file(&s)).await??
            };
            if actual != digest {
                let _ = tokio::fs::remove_file(&src).await;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cannot refresh {}: {touch_err}; staged bytes do not match {digest}",
                        dst.display()
                    ),
                ));
            }
            tokio::task::spawn_blocking(move || {
                let _serialized = crate::db::write_lock();
                std::fs::rename(&src, &dst)
            })
            .await??;
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

/// The locked half of `commit`: put `src` in place at `dst`, or refresh the
/// mtime of the file already there. Returns the touch error when the existing
/// file could be neither refreshed nor found gone, leaving `src` in place for
/// the caller to verify and rename over outside the lock.
fn place(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<Option<std::io::Error>> {
    let _serialized = crate::db::write_lock();
    if std::fs::metadata(dst).is_err() {
        std::fs::rename(src, dst)?;
        return Ok(None);
    }
    // Blob already present (dedup) — drop the duplicate upload. But first
    // refresh the kept file's mtime: the sweep reclaims row-less files older
    // than the grace window, and the file we are about to reuse may be
    // exactly such a leftover with its row still to be written by our
    // caller. A fresh mtime puts it back inside the window until the row
    // lands. The touch goes by path (utimensat), which needs ownership, not
    // write permission, so a read-only cache is fine. If the file vanished
    // since the check the sweep took it: rename ours in.
    match touch(dst) {
        Ok(()) => {
            std::fs::remove_file(src)?;
            Ok(None)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::rename(src, dst)?;
            Ok(None)
        }
        Err(e) => Ok(Some(e)),
    }
}

/// Set `path`'s mtime to now without rewriting its contents.
fn touch(path: &std::path::Path) -> std::io::Result<()> {
    filetime::set_file_mtime(path, filetime::FileTime::now())
}

fn sha256_of_file(path: &std::path::Path) -> std::io::Result<String> {
    use sha2::Digest;
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}
