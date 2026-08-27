//! Object storage as source of truth (Continuity-style).
//!
//! Immutable content — blobs (`blobs/<digest>`) and manifest bytes
//! (`manifests/<digest>`) — is written once and never coordinated. The only
//! mutable state per repository is one small JSON index object
//! (`repos/<name>/index.json`): the registry's equivalent of git refs. Every
//! mutation is a compare-and-swap on that object's ETag, which makes writes
//! per-repo linearizable with no consensus, no leases, and no database.
//! `repos/<name>/log/<version>.json` records each accepted mutation for
//! provenance. SQLite is a rebuildable materialized view of the indexes;
//! replicas validate reads with one conditional GET (NotModified = fast path).

use crate::db::{self};
use crate::objectstore::{Cas, Fetch, ObjectStore};
use crate::AppRef;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

pub fn index_key(repo: &str) -> String {
    format!("repos/{repo}/index.json")
}
fn log_key(repo: &str, version: i64) -> String {
    format!("repos/{repo}/log/{version:010}.json")
}
pub fn manifest_key(digest: &str) -> String {
    format!("manifests/{digest}")
}
pub fn blob_key(digest: &str) -> String {
    format!("blobs/{digest}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub media_type: String,
    pub size: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<String>,
    pub created_at: i64,
    #[serde(default)]
    pub blob_refs: Vec<String>,
    #[serde(default)]
    pub manifest_refs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagEntry {
    pub digest: String,
    pub pushed_at: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexDoc {
    pub version: i64,
    #[serde(default)]
    pub manifests: BTreeMap<String, ManifestEntry>,
    #[serde(default)]
    pub tags: BTreeMap<String, TagEntry>,
}

#[derive(Debug, Serialize)]
pub struct LogInfo {
    pub action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

async fn repo_lock(app: &AppRef, repo: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut map = app.repo_locks.lock().await;
    map.entry(repo.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// Apply a mutation to a repo's index via CAS, retrying on conflicts (another
/// replica may be writing the same repo). On success the SQLite cache is synced
/// and a log entry is written. Returns Ok(Err(msg)) for client-level failures
/// (missing child manifest, deleting a tag that doesn't exist, ...).
pub async fn mutate<F>(
    app: &AppRef,
    repo: &str,
    by: Option<&str>,
    apply: F,
) -> anyhow::Result<Result<(), String>>
where
    F: Fn(&mut IndexDoc) -> Result<LogInfo, String>,
{
    let os = app.object.as_ref().expect("mutate requires object mode");
    let lock = repo_lock(app, repo).await;
    let _guard = lock.lock().await;

    // Retry until a deadline rather than a fixed count: one conflict round-trip
    // costs ~1ms on local fs but ~500ms on a WAN bucket, so attempts are the
    // wrong unit. Jittered backoff decorrelates competing replicas.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut attempt: u64 = 0;
    loop {
        let (mut doc, etag) = match os.get(&index_key(repo)).await? {
            Some((bytes, etag)) => (serde_json::from_slice::<IndexDoc>(&bytes)?, Some(etag)),
            None => (IndexDoc::default(), None),
        };
        let info = match apply(&mut doc) {
            Ok(info) => info,
            Err(msg) => return Ok(Err(msg)),
        };
        doc.version += 1;
        let bytes = serde_json::to_vec(&doc)?;
        match os.put_if_match(&index_key(repo), &bytes, etag.as_deref()).await? {
            Cas::Ok(new_etag) => {
                // Provenance write is fire-and-forget: the CAS'd index is the
                // durable truth, the log is history — don't tax push latency
                // with an extra storage round-trip.
                let entry = serde_json::json!({
                    "version": doc.version,
                    "action": info.action,
                    "tag": info.tag,
                    "digest": info.digest,
                    "by": by,
                    "at": db::now(),
                });
                let (os2, key) = (os.clone(), log_key(repo, doc.version));
                let (repo2, version) = (repo.to_string(), doc.version);
                tokio::spawn(async move {
                    if let Err(e) = os2.put(&key, entry.to_string().as_bytes()).await {
                        tracing::warn!("log write failed for {repo2} v{version}: {e}");
                    }
                });
                sync_repo(app, repo, doc, new_etag).await?;
                return Ok(Ok(()));
            }
            Cas::Conflict => {
                if std::time::Instant::now() >= deadline {
                    anyhow::bail!("CAS on {repo} did not converge before deadline");
                }
                attempt += 1;
                let jitter = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos() as u64
                    % 47;
                tracing::debug!("CAS conflict on {repo}, attempt {attempt}");
                tokio::time::sleep(std::time::Duration::from_millis(
                    (attempt * 15).min(200) + jitter,
                ))
                .await;
            }
        }
    }
}

/// Make the SQLite cache exactly mirror `doc`. Fetches any manifest payloads
/// the cache doesn't have yet (manifest bytes are immutable, keyed by digest).
pub async fn sync_repo(app: &AppRef, repo: &str, doc: IndexDoc, etag: String) -> anyhow::Result<()> {
    use crate::schema::manifests as m;
    let os = app.object.as_ref().unwrap();

    let repo2 = repo.to_string();
    let have: HashSet<String> = db::run(&app.pool, move |conn| {
        let Some(rid) = db::repo_id(conn, &repo2)? else {
            return Ok(HashSet::new());
        };
        Ok(m::table
            .filter(m::repo_id.eq(rid))
            .select(m::digest)
            .load::<String>(conn)?
            .into_iter()
            .collect())
    })
    .await?;

    // Fetch missing payloads concurrently — sequential round-trips dominate
    // rebuild time on a WAN bucket.
    use futures_util::StreamExt;
    let missing: Vec<String> = doc
        .manifests
        .keys()
        .filter(|d| !have.contains(*d))
        .cloned()
        .collect();
    let fetched: Vec<(String, anyhow::Result<Option<(Vec<u8>, String)>>)> =
        futures_util::stream::iter(missing.into_iter().map(|digest| {
            let os = os.clone();
            async move {
                let result = os.get(&manifest_key(&digest)).await;
                (digest, result)
            }
        }))
        .buffer_unordered(16)
        .collect()
        .await;
    let mut payloads: HashMap<String, Vec<u8>> = HashMap::new();
    for (digest, result) in fetched {
        match result? {
            Some((bytes, _)) => {
                payloads.insert(digest, bytes);
            }
            None => tracing::warn!("index of {repo} references missing manifest {digest}"),
        }
    }

    let repo2 = repo.to_string();
    db::run_write(&app.pool, move |conn| {
        use crate::schema::{manifest_refs as r, manifests as m, repos as rp, tags as t};
        conn.immediate_transaction::<_, anyhow::Error, _>(|conn| {
            let rid = db::get_or_create_repo(conn, &repo2)?;
            diesel::update(rp::table.filter(rp::id.eq(rid)))
                .set((rp::index_etag.eq(&etag), rp::index_version.eq(doc.version)))
                .execute(conn)?;

            // Drop rows the index no longer has.
            let keep: Vec<&String> = doc.manifests.keys().collect();
            diesel::delete(t::table.filter(t::repo_id.eq(rid))).execute(conn)?;
            let existing: Vec<(i64, String)> = m::table
                .filter(m::repo_id.eq(rid))
                .select((m::id, m::digest))
                .load(conn)?;
            for (mid, digest) in &existing {
                if !keep.iter().any(|k| *k == digest) {
                    diesel::delete(m::table.filter(m::id.eq(mid))).execute(conn)?;
                }
            }

            // Insert what's new.
            for (digest, entry) in &doc.manifests {
                let exists = existing.iter().any(|(_, d)| d == digest);
                if !exists {
                    let Some(payload) = payloads.get(digest) else { continue };
                    diesel::insert_into(m::table)
                        .values((
                            m::repo_id.eq(rid),
                            m::digest.eq(digest),
                            m::media_type.eq(&entry.media_type),
                            m::payload.eq(payload),
                            m::size.eq(entry.size),
                            m::subject_digest.eq(&entry.subject),
                            m::artifact_type.eq(&entry.artifact_type),
                            m::annotations.eq(&entry.annotations),
                            m::created_at.eq(entry.created_at),
                        ))
                        .on_conflict((m::repo_id, m::digest))
                        .do_nothing()
                        .execute(conn)?;
                    let mid: i64 = m::table
                        .filter(m::repo_id.eq(rid).and(m::digest.eq(digest)))
                        .select(m::id)
                        .first(conn)?;
                    diesel::delete(r::table.filter(r::manifest_id.eq(mid))).execute(conn)?;
                    let mut rows = vec![];
                    for d in &entry.blob_refs {
                        rows.push((r::manifest_id.eq(mid), r::child_digest.eq(d.clone()), r::kind.eq("blob")));
                    }
                    for d in &entry.manifest_refs {
                        rows.push((r::manifest_id.eq(mid), r::child_digest.eq(d.clone()), r::kind.eq("manifest")));
                    }
                    if !rows.is_empty() {
                        diesel::insert_into(r::table).values(rows).execute(conn)?;
                    }
                }
            }

            // Tags mirror the doc exactly.
            for (tag, entry) in &doc.tags {
                let mid: Option<i64> = m::table
                    .filter(m::repo_id.eq(rid).and(m::digest.eq(&entry.digest)))
                    .select(m::id)
                    .first(conn)
                    .optional()?;
                if let Some(mid) = mid {
                    diesel::insert_into(t::table)
                        .values((
                            t::repo_id.eq(rid),
                            t::name.eq(tag),
                            t::manifest_id.eq(mid),
                            t::pushed_at.eq(entry.pushed_at),
                        ))
                        .on_conflict((t::repo_id, t::name))
                        .do_update()
                        .set((t::manifest_id.eq(mid), t::pushed_at.eq(entry.pushed_at)))
                        .execute(conn)?;
                }
            }
            Ok(Ok::<_, anyhow::Error>(()))
        })?
    })
    .await?;
    Ok(())
}

/// One conditional GET against the repo's index; NotModified (the common case)
/// costs a single round-trip and nothing else.
pub async fn refresh_repo(app: &AppRef, repo: &str) -> anyhow::Result<()> {
    let Some(os) = &app.object else { return Ok(()) };
    use crate::schema::repos as rp;
    let repo2 = repo.to_string();
    let cached: Option<Option<String>> = db::run(&app.pool, move |conn| {
        Ok(rp::table
            .filter(rp::name.eq(&repo2))
            .select(rp::index_etag)
            .first::<Option<String>>(conn)
            .optional()?)
    })
    .await?;

    match cached.flatten() {
        Some(etag) => match os.get_if_none_match(&index_key(repo), &etag).await? {
            Fetch::NotModified => Ok(()),
            Fetch::New(bytes, new_etag) => {
                let doc: IndexDoc = serde_json::from_slice(&bytes)?;
                sync_repo(app, repo, doc, new_etag).await
            }
            Fetch::Missing => sync_repo(app, repo, IndexDoc::default(), String::new()).await,
        },
        None => match os.get(&index_key(repo)).await? {
            Some((bytes, etag)) => {
                let doc: IndexDoc = serde_json::from_slice(&bytes)?;
                sync_repo(app, repo, doc, etag).await
            }
            None => Ok(()),
        },
    }
}

/// Same as refresh_repo but never fails the request path: bucket trouble means
/// serving the (still-correct-as-of-last-sync) cache.
pub async fn refresh_repo_soft(app: &AppRef, repo: &str) {
    if let Err(e) = refresh_repo(app, repo).await {
        tracing::warn!("index refresh for {repo} failed, serving cached state: {e}");
    }
}

/// Rebuild the whole cache from the bucket: list every repo index and sync it.
/// This is what makes replicas disposable.
pub async fn rebuild_all(app: &AppRef) -> anyhow::Result<usize> {
    let Some(os) = &app.object else { return Ok(0) };
    use futures_util::StreamExt;
    let keys = os.list("repos/").await?;
    let names: Vec<String> = keys
        .iter()
        .filter_map(|k| {
            k.strip_prefix("repos/")
                .and_then(|k| k.strip_suffix("/index.json"))
                .map(String::from)
        })
        .collect();
    let count = names.len();
    let results: Vec<anyhow::Result<()>> = futures_util::stream::iter(names.into_iter().map(|n| {
        let app = app.clone();
        async move { refresh_repo(&app, &n).await }
    }))
    .buffer_unordered(8)
    .collect()
    .await;
    for r in results {
        r?;
    }
    Ok(count)
}

/// Blob read-through: make sure the blob is on local disk (and in the cache DB),
/// downloading from the bucket on miss. Returns its size, or None if unknown.
pub async fn ensure_blob_local(app: &AppRef, digest: &str) -> anyhow::Result<Option<i64>> {
    use crate::schema::blobs as b;
    let digest2 = digest.to_string();
    let cached: Option<i64> = db::run(&app.pool, move |conn| {
        Ok(b::table
            .filter(b::digest.eq(&digest2))
            .select(b::size)
            .first::<i64>(conn)
            .optional()?)
    })
    .await?;
    if cached.is_some() {
        return Ok(cached);
    }
    let Some(os) = &app.object else { return Ok(None) };

    let staging_id = format!("fill-{}", uuid::Uuid::new_v4());
    let staging = app.store.staging_path(&staging_id);
    if !os.get_to_file(&blob_key(digest), &staging).await? {
        return Ok(None);
    }
    let size = app.store.commit(&staging_id, digest).await? as i64;
    let digest2 = digest.to_string();
    db::run_write(&app.pool, move |conn| {
        diesel::insert_into(b::table)
            .values((b::digest.eq(&digest2), b::size.eq(size), b::created_at.eq(db::now())))
            .on_conflict(b::digest)
            .do_nothing()
            .execute(conn)?;
        Ok(())
    })
    .await?;
    Ok(Some(size))
}

/// Does this blob exist anywhere we can see (cache or bucket)?
pub async fn blob_exists(app: &AppRef, digest: &str) -> anyhow::Result<bool> {
    use crate::schema::blobs as b;
    let digest2 = digest.to_string();
    let cached: bool = db::run(&app.pool, move |conn| {
        Ok(b::table
            .filter(b::digest.eq(&digest2))
            .select(b::digest)
            .first::<String>(conn)
            .optional()?
            .is_some())
    })
    .await?;
    if cached {
        return Ok(true);
    }
    match &app.object {
        Some(os) => os.head(&blob_key(digest)).await,
        None => Ok(false),
    }
}
