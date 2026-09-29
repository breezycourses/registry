use crate::db;
use crate::AppRef;
use diesel::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, serde::Serialize)]
pub struct GcReport {
    pub dry_run: bool,
    pub manifests_deleted: usize,
    pub blobs_deleted: usize,
    pub bytes_freed: i64,
    /// Objects no repo index references at all — the mark phase above only sees
    /// what an index recorded, so these need a bucket listing to find. Orphans
    /// are deleted only after a previous sweep marked them in `gc/pending.json`
    /// — a shared ledger that also makes `blob_exists` fail closed on them, so
    /// no replica can adopt a marked object mid-sweep.
    pub orphan_manifests_deleted: usize,
    pub orphan_blobs_deleted: usize,
    pub orphan_bytes_freed: i64,
    /// Candidates recorded (or still pending) this run — next run's deletions.
    pub orphans_marked: usize,
    /// Object mode only: files in the local blob cache whose digest has no
    /// `blobs` row and no manifest reference. The cache is filled read-through and the DB is rebuilt
    /// from the bucket, so once a bucket index drops a blob its row vanishes
    /// on the next rebuild while the file stays — nothing above ever sees it
    /// again. Only files older than the grace window are taken.
    pub local_cache_files_deleted: usize,
    pub local_cache_bytes_freed: i64,
}

/// Mark & sweep. A manifest is kept if it is tagged, referenced by a kept index,
/// or is a referrer (subject) of a kept manifest — so cosign signatures survive
/// as long as what they sign does. Nothing younger than the grace window is ever
/// touched, which makes GC safe against concurrent pushes.
///
/// In object mode the bucket is the source of truth: GC first re-syncs the cache
/// from every repo index, removes victims from the indexes via CAS, and then
/// deletes the unreferenced manifest/blob objects. Blob row deletion is guarded
/// by a NOT EXISTS re-check so a ref added mid-sweep rescues its blob.
///
/// The final phase lists the bucket itself. The index-driven sweep cannot see
/// objects no index ever recorded — a blob uploaded by a push that was then
/// abandoned, a manifest object whose index CAS was rejected, a delete that
/// failed mid-sweep — so they would otherwise accumulate forever.
pub async fn run(app: &AppRef, dry_run: bool) -> anyhow::Result<GcReport> {
    if app.object.is_some() {
        crate::truth::rebuild_all(app).await?;
    }
    let grace = app.cfg.gc_grace_seconds;

    // Phase 1: mark. No deletions here.
    let (victims, blob_victims, known_manifests, known_blobs) = db::run(&app.pool, move |conn| {
        use crate::schema::{blobs as b, manifest_refs as r, manifests as m, repos as rp, tags as t};
        let all: Vec<(i64, i64, String, Option<String>, i64)> = m::table
            .select((m::id, m::repo_id, m::digest, m::subject_digest, m::created_at))
            .load(conn)?;
        let refs: Vec<(i64, String, String)> = r::table
            .select((r::manifest_id, r::child_digest, r::kind))
            .load(conn)?;
        let tagged: Vec<i64> = t::table.select(t::manifest_id).load(conn)?;
        let repo_names: HashMap<i64, String> = rp::table
            .select((rp::id, rp::name))
            .load::<(i64, String)>(conn)?
            .into_iter()
            .collect();

        let id_by_repo_digest: HashMap<(i64, &str), i64> = all
            .iter()
            .map(|(id, rid, d, _, _)| ((*rid, d.as_str()), *id))
            .collect();
        let repo_of: HashMap<i64, i64> = all.iter().map(|(id, rid, ..)| (*id, *rid)).collect();
        let digest_of: HashMap<i64, &str> =
            all.iter().map(|(id, _, d, ..)| (*id, d.as_str())).collect();

        let mut kept: HashSet<i64> = tagged.into_iter().collect();
        loop {
            let before = kept.len();
            for (mid, child, kind) in &refs {
                if kind == "manifest" && kept.contains(mid) {
                    if let Some(rid) = repo_of.get(mid) {
                        if let Some(cid) = id_by_repo_digest.get(&(*rid, child.as_str())) {
                            kept.insert(*cid);
                        }
                    }
                }
            }
            let kept_digests: HashSet<(i64, &str)> = kept
                .iter()
                .filter_map(|id| Some((*repo_of.get(id)?, *digest_of.get(id)?)))
                .collect();
            for (id, rid, _, subject, _) in &all {
                if let Some(s) = subject {
                    if kept_digests.contains(&(*rid, s.as_str())) {
                        kept.insert(*id);
                    }
                }
            }
            if kept.len() == before {
                break;
            }
        }

        let cutoff = db::now() - grace;
        // (manifest id, repo name, digest)
        let victims: Vec<(i64, String, String)> = all
            .iter()
            .filter(|(id, _, _, _, created)| !kept.contains(id) && *created <= cutoff)
            .filter_map(|(id, rid, d, _, _)| {
                Some((*id, repo_names.get(rid)?.clone(), d.clone()))
            })
            .collect();
        let victim_set: HashSet<i64> = victims.iter().map(|(id, _, _)| *id).collect();

        // Blob liveness as if the victims are already gone — correct for both
        // dry runs and real runs.
        let live_blobs: HashSet<&str> = refs
            .iter()
            .filter(|(mid, _, kind)| kind == "blob" && !victim_set.contains(mid))
            .map(|(_, d, _)| d.as_str())
            .collect();
        let all_blobs: Vec<(String, i64, i64)> =
            b::table.select((b::digest, b::size, b::created_at)).load(conn)?;
        let blob_victims: Vec<(String, i64)> = all_blobs
            .iter()
            .filter(|(d, _, created)| !live_blobs.contains(d.as_str()) && *created <= cutoff)
            .map(|(d, s, _)| (d.clone(), *s))
            .collect();

        // Every digest the indexes (or the blob-row ledger) account for —
        // kept or victim. Phase 3's bucket listing treats anything OUTSIDE
        // these sets as an orphan, so every recorded row must be in here: a
        // young unreferenced row is not a phase-2 victim, but deleting its
        // bucket object would leave a row blob_exists trusts pointing at
        // nothing. The guarded phase-2 delete removes row, file, and object
        // together once the row ages past grace.
        let known_manifests: HashSet<String> =
            all.iter().map(|(_, _, d, _, _)| d.clone()).collect();
        let mut known_blobs: HashSet<String> =
            live_blobs.iter().map(|d| d.to_string()).collect();
        known_blobs.extend(all_blobs.iter().map(|(d, _, _)| d.clone()));

        Ok((victims, blob_victims, known_manifests, known_blobs))
    })
    .await?;

    let mut report = GcReport {
        dry_run,
        manifests_deleted: victims.len(),
        blobs_deleted: blob_victims.len(),
        bytes_freed: blob_victims.iter().map(|(_, s)| s).sum(),
        orphan_manifests_deleted: 0,
        orphan_blobs_deleted: 0,
        orphan_bytes_freed: 0,
        orphans_marked: 0,
        local_cache_files_deleted: 0,
        local_cache_bytes_freed: 0,
    };
    if dry_run {
        (
            report.orphan_manifests_deleted,
            report.orphan_blobs_deleted,
            report.orphan_bytes_freed,
            report.orphans_marked,
        ) = sweep_orphans(app, &known_manifests, &known_blobs, true).await?;
        (report.local_cache_files_deleted, report.local_cache_bytes_freed) =
            sweep_local_cache(app, true).await?;
        return Ok(report);
    }

    // Phase 2: sweep.
    if app.object.is_some() {
        // Remove victims from each repo's index (CAS); mutate() re-syncs the
        // cache, which deletes the corresponding rows.
        let mut by_repo: HashMap<String, Vec<String>> = HashMap::new();
        for (_, repo, digest) in &victims {
            by_repo.entry(repo.clone()).or_default().push(digest.clone());
        }
        for (repo, digests) in by_repo {
            let outcome = crate::truth::mutate(app, &repo, Some("gc"), move |doc| {
                for d in &digests {
                    doc.manifests.remove(d);
                }
                Ok(crate::truth::LogInfo { action: "gc", tag: None, digest: None })
            })
            .await?;
            if let Err(msg) = outcome {
                tracing::warn!("gc index update for {repo} rejected: {msg}");
            }
        }
        // A manifest object is deletable once no repo's index mentions it.
        let victim_digests: Vec<String> =
            victims.iter().map(|(_, _, d)| d.clone()).collect::<HashSet<_>>().into_iter().collect();
        let vd = victim_digests.clone();
        let still_used: HashSet<String> = db::run(&app.pool, move |conn| {
            use crate::schema::manifests as m;
            Ok(m::table
                .filter(m::digest.eq_any(&vd))
                .select(m::digest)
                .load::<String>(conn)?
                .into_iter()
                .collect())
        })
        .await?;
        if let Some(os) = &app.object {
            for d in &victim_digests {
                if !still_used.contains(d) {
                    if let Err(e) = os.delete(&crate::truth::manifest_key(d)).await {
                        tracing::warn!("gc: failed to delete manifest object {d}: {e}");
                    }
                }
            }
        }
    } else {
        let ids: Vec<i64> = victims.iter().map(|(id, _, _)| *id).collect();
        db::run_write(&app.pool, move |conn| {
            use crate::schema::manifests as m;
            for chunk in ids.chunks(500) {
                diesel::delete(m::table.filter(m::id.eq_any(chunk))).execute(conn)?;
            }
            Ok(())
        })
        .await?;
    }

    // Blob rows: guarded delete — NOT EXISTS re-checks references at delete
    // time, so a blob re-referenced mid-sweep is rescued.
    let digests: Vec<String> = blob_victims.iter().map(|(d, _)| d.clone()).collect();
    let actually_deleted: Vec<String> = db::run_write(&app.pool, move |conn| {
        use crate::schema::{blobs as b, manifest_refs as r};
        let mut deleted = vec![];
        for d in &digests {
            let n = diesel::delete(
                b::table.filter(
                    b::digest.eq(d).and(diesel::dsl::not(diesel::dsl::exists(
                        r::table.filter(r::child_digest.eq(d).and(r::kind.eq("blob"))),
                    ))),
                ),
            )
            .execute(conn)?;
            if n > 0 {
                deleted.push(d.clone());
            }
        }
        Ok(deleted)
    })
    .await?;

    for digest in &actually_deleted {
        if let Err(e) = app.store.delete(digest).await {
            tracing::warn!("gc: failed to delete blob file {digest}: {e}");
        }
        if let Some(os) = &app.object {
            if let Err(e) = os.delete(&crate::truth::blob_key(digest)).await {
                tracing::warn!("gc: failed to delete blob object {digest}: {e}");
            }
        }
    }

    (
        report.orphan_manifests_deleted,
        report.orphan_blobs_deleted,
        report.orphan_bytes_freed,
        report.orphans_marked,
    ) = sweep_orphans(app, &known_manifests, &known_blobs, false).await?;
    (report.local_cache_files_deleted, report.local_cache_bytes_freed) =
        sweep_local_cache(app, false).await?;
    Ok(report)
}

/// Candidates are checked and unlinked this many at a time: the walk buffers
/// at most this many entries before draining them, and the DB write lock is
/// held for one query, a re-stat and a few dozen unlinks — milliseconds —
/// never for the whole cache.
const LOCAL_SWEEP_BATCH: usize = 100;

/// Phase 4, object mode only: the local blob cache. Files under
/// `<data>/blobs/` are a read-through cache of bucket objects; the `blobs`
/// table is rebuilt from the bucket indexes on boot and before every sweep.
/// Phase 2 unlinks a file only when it deletes that file's row — but a blob
/// dropped from a bucket index (by GC, retention, or another replica) simply
/// has no row after the next rebuild, so no phase above ever names it again
/// and its file sits on disk for good. This phase walks the directory and
/// removes every file whose digest has neither a row nor a manifest
/// reference.
///
/// Safety is the same shape as the rest of the module. Nothing younger than
/// `gc_grace_seconds` is touched (strictly: a file must be older than the
/// cutoff, so with a zero grace a file committed this second still stands).
/// An upload commits its file before its row is written, with the bucket
/// PUT in between, and a read-through fill does the same, so the grace must
/// cover that gap — the assumption phase 2 already makes about pushes. The
/// accounting check, a re-stat of the mtime and the unlink run inside the DB
/// write lock, and `Store::commit` places (or touches) a file under the same
/// lock, so a commit can never land between "unaccounted" and the unlink:
/// it either ran first, leaving a fresh mtime the re-stat respects, or runs
/// after, finding no file and renaming a fresh one in. A pull that has the
/// file open keeps reading its inode; a pull that arrives after the unlink
/// has no row either and re-fetches from the bucket. The walk streams: at
/// most one batch of candidates is buffered, and the directory is read
/// outside the lock. `staging/` is never visited.
///
/// Local mode is skipped outright: there the blob directory *is* the source
/// of truth, and a row-less file can't be proven garbage.
///
/// Returns (files_deleted, bytes_freed).
async fn sweep_local_cache(app: &AppRef, dry_run: bool) -> anyhow::Result<(usize, i64)> {
    if app.object.is_none() {
        return Ok((0, 0));
    }
    let cutoff = db::now() - app.cfg.gc_grace_seconds;
    let pool = app.pool.clone();
    let store = app.store.clone();
    tokio::task::spawn_blocking(move || {
        let mut batch: Vec<(String, i64)> = Vec::with_capacity(LOCAL_SWEEP_BATCH);
        let (mut files, mut bytes) = (0usize, 0i64);
        let mut drain = |batch: &mut Vec<(String, i64)>| -> anyhow::Result<()> {
            let (f, b) = sweep_batch(&pool, &store, batch, cutoff, dry_run)?;
            files += f;
            bytes += b;
            batch.clear();
            Ok(())
        };
        // <blobs>/<algo>/<xx>/<hex>
        for algo in read_dirs(&store.blobs_root()) {
            let Some(algo_name) = algo.file_name().and_then(|n| n.to_str()).map(String::from)
            else {
                continue;
            };
            for shard in read_dirs(&algo) {
                let Ok(entries) = std::fs::read_dir(&shard) else { continue };
                for entry in entries.flatten() {
                    let Ok(meta) = entry.metadata() else { continue };
                    if !meta.is_file() || mtime_secs(&meta) >= cutoff {
                        continue;
                    }
                    let Some(hex) = entry.file_name().to_str().map(String::from) else {
                        continue;
                    };
                    batch.push((format!("{algo_name}:{hex}"), meta.len() as i64));
                    if batch.len() >= LOCAL_SWEEP_BATCH {
                        drain(&mut batch)?;
                    }
                }
            }
        }
        drain(&mut batch)?;
        Ok((files, bytes))
    })
    .await?
}

/// One batch of the local-cache sweep: which of these digests are unaccounted
/// for, and (unless dry-running) unlink their files. Runs under the DB write
/// lock when it deletes; see `sweep_local_cache` for why. A dry run re-stats
/// too, so its preview is the real sweep's decision, not the walk's snapshot.
fn sweep_batch(
    pool: &db::DbPool,
    store: &crate::storage::Store,
    batch: &[(String, i64)],
    cutoff: i64,
    dry_run: bool,
) -> anyhow::Result<(usize, i64)> {
    use crate::schema::{blobs as b, manifest_refs as r};
    let _serialized = (!dry_run).then(db::write_lock);
    let mut conn = pool.get()?;
    let digests: Vec<&str> = batch.iter().map(|(d, _)| d.as_str()).collect();
    // A row or a manifest reference both mean "accounted", exactly as in
    // `still_unaccounted`: a referenced blob has no row until a pull fills
    // it, and its file must survive either way.
    let mut accounted: HashSet<String> = b::table
        .filter(b::digest.eq_any(&digests))
        .select(b::digest)
        .load::<String>(&mut conn)?
        .into_iter()
        .collect();
    accounted.extend(
        r::table
            .filter(r::child_digest.eq_any(&digests).and(r::kind.eq("blob")))
            .select(r::child_digest)
            .load::<String>(&mut conn)?,
    );
    let (mut files, mut bytes) = (0usize, 0i64);
    for (digest, size) in batch {
        if accounted.contains(digest) {
            continue;
        }
        // Re-stat under the lock: a commit that dedup'd onto this file since
        // the walk refreshed its mtime, or replaced it outright.
        let path = store.blob_path(digest);
        match std::fs::metadata(&path) {
            Ok(m) if mtime_secs(&m) >= cutoff => continue,
            Ok(_) => {}
            Err(_) => continue,
        }
        if dry_run {
            files += 1;
            bytes += size;
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                files += 1;
                bytes += size;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!("gc: failed to delete cached blob {digest}: {e}"),
        }
    }
    Ok((files, bytes))
}

fn read_dirs(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default()
}

fn mtime_secs(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(i64::MAX)
}

/// Phase 3: objects no index ever recorded. The mark phase only sees what the
/// indexes know — a blob uploaded by a push that was then abandoned, a manifest
/// object whose index CAS was rejected, an object delete that failed mid-sweep
/// — none of these leave a row, so the sweep above can never collect them and
/// they accumulate for the life of the bucket. List the two content prefixes
/// to find them; `known_*` are the digests the indexes (or the blob ledger)
/// account for.
///
/// Two sightings before a delete, and the sightings live in the bucket
/// (`gc/pending.json`, CAS'd like a repo index) — not in this replica's
/// SQLite — because the race they guard against is cross-replica. The mark is
/// what makes deletion safe: `blob_exists` answers false for a pending digest
/// on every replica, so a push can never commit an index reference to a marked
/// orphan. The client's only path forward is to re-upload the blob, and the
/// upload clears the mark and refreshes the object — an adoption that lands
/// visibly instead of racing the delete.
///
/// Deletion itself is claimed: the sweep CAS-moves a mark into `deleting`
/// before touching the object. A rescue that sees the claim waits it out and
/// re-PUTs only after it clears, so its write is ordered after the delete it
/// could otherwise interleave with — the CAS is what turns "narrow window"
/// into "impossible". Claims expire after CLAIM_TTL_SECS so a crashed sweep
/// can't wedge a digest.
///
/// Returns (manifests_deleted, blobs_deleted, bytes_freed, marked).
async fn sweep_orphans(
    app: &AppRef,
    known_manifests: &HashSet<String>,
    known_blobs: &HashSet<String>,
    dry_run: bool,
) -> anyhow::Result<(usize, usize, i64, usize)> {
    let Some(os) = &app.object else {
        return Ok((0, 0, 0, 0));
    };
    let now = db::now();
    let cutoff = now - app.cfg.gc_grace_seconds;

    // Everything under the content prefixes that no index or ledger accounts
    // for — candidates, of any age. Young objects get recorded too: an
    // in-flight push's blob is indistinguishable from an abandoned one until
    // the index CAS lands, and recording early just means the next sweep can
    // collect a real orphan rather than mark it again.
    let mut candidates: Vec<(String, &'static str, i64, i64)> = vec![];
    for (prefix, kind, known) in [
        ("manifests/", "manifest", known_manifests),
        ("blobs/", "blob", known_blobs),
    ] {
        for e in os.list(prefix).await? {
            let Some(digest) = e.key.strip_prefix(prefix) else {
                continue;
            };
            if known.contains(digest) {
                continue;
            }
            candidates.push((e.key, kind, e.modified, e.size));
        }
    }

    // Read once up front: marks written by this run are never visible to this
    // run's delete decisions, which is what makes the two sightings strict
    // even when the grace window is zero.
    let pending = crate::truth::read_pending(os).await?;
    // Claim ownership for this sweep — the fencing token for renew/release.
    let owner = uuid::Uuid::new_v4().to_string();

    let (mut manifests, mut blobs, mut bytes, mut marked) = (0usize, 0usize, 0i64, 0usize);
    let mut deleted: HashSet<String> = HashSet::new();
    let mut rescued: HashSet<String> = HashSet::new();
    for (key, kind, modified, size) in &candidates {
        let digest = key.split_once('/').map(|(_, d)| d).unwrap_or(key.as_str());
        // Deletable means the mark itself has aged past the grace window (so
        // a previous sweep sighted it) and the object has too. The mark alone
        // is not enough: an object can be rewritten after being sighted.
        let deletable = *modified <= cutoff
            && pending.digests.get(digest).is_some_and(|seen| *seen <= cutoff);
        if !deletable {
            marked += 1;
            continue;
        }
        if !dry_run {
            // The write half of gc_lock spans check-and-delete; pushes hold
            // the read half across their manifest PUT + existence checks +
            // index CAS, so no same-replica reference commits inside it.
            let _guard = app.gc_lock.write().await;
            // Claim the mark before touching the object. The claim is the
            // linearization point against rescues: a re-upload that sees the
            // claim waits for its release and only then re-PUTs, so a rescue's
            // write can never land inside our delete window — and if the mark
            // is already gone, a rescue committed first and we stand down.
            match crate::truth::claim_pending(app, digest, &owner).await {
                Ok(true) => {}
                Ok(false) => {
                    rescued.insert(key.clone());
                    continue;
                }
                Err(e) => {
                    tracing::warn!("gc: failed to claim orphan mark for {key}: {e}");
                    continue;
                }
            }
            // The listing's mtime is stale the moment an object is rewritten;
            // re-stat so a re-PUT since the listing keeps the object.
            match os.stat(key).await? {
                // Already gone — another sweep's delete beat us to it.
                None => {
                    crate::truth::release_pending(app, digest, &owner, false).await?;
                    deleted.insert(key.clone());
                }
                // Re-PUT since the listing — release the claim back to a
                // fresh mark so the new object gets its own window.
                Some(m) if m > cutoff => {
                    crate::truth::release_pending(app, digest, &owner, true).await?;
                    marked += 1;
                    continue;
                }
                Some(_) => {
                    if !still_unaccounted(app, key).await? {
                        crate::truth::release_pending(app, digest, &owner, true).await?;
                        rescued.insert(key.clone());
                        continue;
                    }
                    // Fencing: a stall past the claim TTL lets a rescuer
                    // force-clear our claim and re-PUT, so the claim is
                    // re-verified-and-refreshed at the last instant. If it's
                    // no longer ours, the delete must not run — whatever the
                    // rescuer wrote must stay.
                    match crate::truth::renew_pending(app, digest, &owner).await {
                        Ok(true) => {}
                        Ok(false) => {
                            rescued.insert(key.clone());
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!("gc: failed to renew claim for {key}: {e}");
                            continue;
                        }
                    }
                    match os.delete(key).await {
                        Ok(()) => {
                            crate::truth::release_pending(app, digest, &owner, false)
                                .await?;
                            deleted.insert(key.clone());
                        }
                        Err(e) => {
                            // Relist so the next sweep retries the delete.
                            if let Err(e2) =
                                crate::truth::release_pending(app, digest, &owner, true)
                                    .await
                            {
                                tracing::warn!("gc: failed to release claim for {key}: {e2}");
                            }
                            tracing::warn!("gc: failed to delete orphan object {key}: {e}");
                            continue;
                        }
                    }
                }
            }
        }
        match *kind {
            "manifest" => manifests += 1,
            _ => {
                blobs += 1;
                bytes += *size;
            }
        }
    }

    if dry_run {
        return Ok((manifests, blobs, bytes, marked));
    }

    // Rewrite the ledger to the surviving candidate set: first sightings get a
    // fresh timestamp, rescued and deleted digests drop out so a re-orphaned
    // object starts its observation window over, and failed deletes keep their
    // mark so the next sweep retries them. Written by CAS, so a concurrent
    // sweep's marks and a concurrent push's clears merge rather than clobber —
    // with one deliberate exception: a digest that was marked in our up-front
    // read but is missing from the doc we merge into was cleared by a rescue
    // mid-run, and re-adding it would falsely re-doom a freshly uploaded
    // object. Those stay out.
    let keep: HashSet<String> = candidates
        .iter()
        .filter(|(key, ..)| !deleted.contains(key) && !rescued.contains(key))
        .filter_map(|(key, ..)| key.split_once('/').map(|(_, d)| d.to_string()))
        .collect();
    let upfront = pending.digests.keys().cloned().collect::<HashSet<_>>();
    crate::truth::update_pending(app, move |doc| {
        // Stale claims are a dead sweep's residue — expire them so their
        // digests can be re-marked below.
        doc.deleting
            .retain(|_, c| now - c.at < crate::truth::CLAIM_TTL_SECS);
        doc.digests
            .retain(|d, _| keep.contains(d) || !upfront.contains(d));
        for d in &keep {
            // Being deleted by another sweep right now — leave it alone.
            if doc.deleting.contains_key(d) {
                continue;
            }
            if upfront.contains(d) && !doc.digests.contains_key(d) {
                continue;
            }
            doc.digests.entry(d.clone()).or_insert(now);
        }
    })
    .await?;
    Ok((manifests, blobs, bytes, marked))
}

/// The delete-time guard for phase 3: re-check the index state this replica
/// holds *now*. A same-replica push completing mid-sweep writes these rows via
/// mutate's sync_repo; a cross-replica push is caught by the two-observation
/// window (its index lands in the next sweep's rebuild).
async fn still_unaccounted(app: &AppRef, key: &str) -> anyhow::Result<bool> {
    let Some((prefix, digest)) = key.split_once('/') else {
        return Ok(false);
    };
    let (prefix, digest) = (prefix.to_string(), digest.to_string());
    db::run(&app.pool, move |conn| {
        use crate::schema::{blobs as b, manifest_refs as r, manifests as m};
        use diesel::dsl::exists;
        Ok(match prefix.as_str() {
            "manifests" => {
                !diesel::select(exists(m::table.filter(m::digest.eq(&digest))))
                    .get_result::<bool>(conn)?
            }
            "blobs" => {
                // A row or a reference both mean "accounted": the row feeds
                // blob_exists even when no manifest points at the object.
                let rowed: bool =
                    diesel::select(exists(b::table.filter(b::digest.eq(&digest))))
                        .get_result(conn)?;
                let referenced: bool = diesel::select(exists(
                    r::table.filter(r::child_digest.eq(&digest).and(r::kind.eq("blob"))),
                ))
                .get_result(conn)?;
                !(rowed || referenced)
            }
            _ => false,
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objectstore::{FsObjectStore, ObjectStore};
    use crate::storage::Store;
    use crate::truth::{blob_key, index_key, manifest_key, IndexDoc, ManifestEntry, TagEntry};
    use crate::{config::Config, App};
    use std::sync::Arc;

    fn sha(c: char) -> String {
        format!("sha256:{}", c.to_string().repeat(64))
    }

    /// An app in object mode backed by a filesystem bucket under `root`.
    fn app(root: &std::path::Path, grace: i64) -> (AppRef, Arc<FsObjectStore>) {
        let data = root.join("data");
        let bucket = root.join("bucket");
        std::fs::create_dir_all(&data).unwrap();
        let os = Arc::new(FsObjectStore::new(bucket.to_str().unwrap()).unwrap());
        let app: AppRef = Arc::new(App {
            pool: db::init(data.to_str().unwrap()).unwrap(),
            store: Store::new(data.to_str().unwrap()).unwrap(),
            cfg: Config {
                gc_grace_seconds: grace,
                ..Config::default()
            },
            object: Some(os.clone()),
            repo_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            gc_lock: tokio::sync::RwLock::new(()),
        });
        (app, os)
    }

    /// One tagged manifest (M1 -> blob B1) in `team/app`'s index, plus a
    /// manifest and a blob that exist only as objects — the abandoned-push /
    /// failed-delete residue the index-driven sweep cannot see.
    async fn seed(os: &FsObjectStore) {
        let (m1, b1, m2, b2) = (sha('a'), sha('b'), sha('c'), sha('d'));
        os.put(&manifest_key(&m1), b"{\"schemaVersion\":2}").await.unwrap();
        os.put(&blob_key(&b1), b"live layer").await.unwrap();
        os.put(&manifest_key(&m2), b"{\"schemaVersion\":2}").await.unwrap();
        os.put(&blob_key(&b2), b"orphaned layer").await.unwrap();

        let mut doc = IndexDoc::default();
        doc.manifests.insert(
            m1.clone(),
            ManifestEntry {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                size: 18,
                subject: None,
                artifact_type: None,
                annotations: None,
                created_at: db::now(),
                blob_refs: vec![b1],
                manifest_refs: vec![],
            },
        );
        doc.tags
            .insert("latest".into(), TagEntry { digest: m1, pushed_at: db::now() });
        let bytes = serde_json::to_vec(&doc).unwrap();
        os.put(&index_key("team/app"), &bytes).await.unwrap();
    }

    /// Rewrite `team/app`'s index with an extra manifest, as if a push had
    /// just committed it.
    async fn adopt(os: &FsObjectStore, child_blob: &str) {
        let m3 = sha('e');
        os.put(&manifest_key(&m3), b"{\"schemaVersion\":2}").await.unwrap();
        let (bytes, _) = os.get(&index_key("team/app")).await.unwrap().unwrap();
        let mut doc: IndexDoc = serde_json::from_slice(&bytes).unwrap();
        doc.manifests.insert(
            m3.clone(),
            ManifestEntry {
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                size: 18,
                subject: None,
                artifact_type: None,
                annotations: None,
                created_at: db::now(),
                blob_refs: vec![child_blob.to_string()],
                manifest_refs: vec![],
            },
        );
        doc.tags
            .insert("adopted".into(), TagEntry { digest: m3, pushed_at: db::now() });
        let bytes = serde_json::to_vec(&doc).unwrap();
        os.put(&index_key("team/app"), &bytes).await.unwrap();
    }

    #[tokio::test]
    async fn orphans_are_marked_then_deleted_on_the_next_sweep() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 0);
        seed(&os).await;

        // First sighting records, never deletes: the push that owns it may
        // simply not have committed its index yet.
        let r1 = run(&app, false).await.unwrap();
        assert_eq!(r1.orphan_manifests_deleted, 0);
        assert_eq!(r1.orphan_blobs_deleted, 0);
        assert_eq!(r1.orphans_marked, 2);
        assert!(os.head(&manifest_key(&sha('c'))).await.unwrap());
        assert!(os.head(&blob_key(&sha('d'))).await.unwrap());

        // A dry run previews what the second sighting will delete.
        let dry = run(&app, true).await.unwrap();
        assert_eq!(dry.orphan_manifests_deleted, 1);
        assert_eq!(dry.orphan_blobs_deleted, 1);
        assert_eq!(dry.orphan_bytes_freed, "orphaned layer".len() as i64);
        assert!(os.head(&manifest_key(&sha('c'))).await.unwrap());

        let r2 = run(&app, false).await.unwrap();
        assert_eq!(r2.orphan_manifests_deleted, 1);
        assert_eq!(r2.orphan_blobs_deleted, 1);
        assert!(!os.head(&manifest_key(&sha('c'))).await.unwrap());
        assert!(!os.head(&blob_key(&sha('d'))).await.unwrap());
        // What the index references stays.
        assert!(os.head(&manifest_key(&sha('a'))).await.unwrap());
        assert!(os.head(&blob_key(&sha('b'))).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_push_adopting_an_orphan_between_sweeps_rescues_it() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 0);
        seed(&os).await;
        run(&app, false).await.unwrap(); // mark the orphans

        // Mid-sweeps an index commit lands referencing the orphan blob —
        // written straight into the index here, bypassing the push path
        // (which the pending mark would have rejected), to prove the
        // delete-time unaccounted re-check is a real second line of defence.
        adopt(&os, &sha('d')).await;

        let report = run(&app, false).await.unwrap();
        // The manifest orphan still goes; the adopted blob is live now.
        assert_eq!(report.orphan_blobs_deleted, 0);
        assert!(os.head(&blob_key(&sha('d'))).await.unwrap());
        assert!(!os.head(&manifest_key(&sha('c'))).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_fresh_blob_row_protects_an_old_bucket_object() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 3600);
        seed(&os).await;

        // A pull through ensure_blob_local wrote a young ledger row for a
        // blob object that is itself old. Phase 2 can't take the row (grace),
        // so the object must not be swept either — the row feeds blob_exists.
        let orphan = sha('d');
        let path = root.join("bucket").join(blob_key(&orphan));
        let old = filetime::FileTime::from_unix_time(db::now() - 7200, 0);
        filetime::set_file_mtime(&path, old).unwrap();
        let d = orphan.clone();
        db::run_write(&app.pool, move |conn| {
            use crate::schema::blobs as b;
            diesel::insert_into(b::table)
                .values((b::digest.eq(&d), b::size.eq(14), b::created_at.eq(db::now())))
                .execute(conn)?;
            Ok(())
        })
        .await
        .unwrap();

        // Neither sweep may touch it.
        run(&app, false).await.unwrap();
        run(&app, false).await.unwrap();
        assert!(os.head(&blob_key(&orphan)).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn delete_time_guard_sees_references_the_listing_predates() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, _os) = app(&root, 0);

        // The orphan blob is unaccounted.
        let key = blob_key(&sha('d'));
        assert!(still_unaccounted(&app, &key).await.unwrap());

        // Then a manifest commits referencing it — the rows sync_repo writes.
        let (m9, d) = (sha('e'), sha('d'));
        db::run_write(&app.pool, move |conn| {
            use crate::schema::{manifest_refs as r, manifests as m};
            let rid = db::get_or_create_repo(conn, "team/app")?;
            diesel::insert_into(m::table)
                .values((
                    m::repo_id.eq(rid),
                    m::digest.eq(&m9),
                    m::media_type.eq("application/vnd.oci.image.manifest.v1+json"),
                    m::payload.eq(b"{\"schemaVersion\":2}".as_slice()),
                    m::size.eq(18),
                    m::created_at.eq(db::now()),
                ))
                .execute(conn)?;
            let mid: i64 = m::table
                .filter(m::repo_id.eq(rid).and(m::digest.eq(&m9)))
                .select(m::id)
                .first(conn)?;
            diesel::insert_into(r::table)
                .values((r::manifest_id.eq(mid), r::child_digest.eq(&d), r::kind.eq("blob")))
                .execute(conn)?;
            Ok(())
        })
        .await
        .unwrap();

        assert!(!still_unaccounted(&app, &key).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_marked_orphan_is_not_adoptable_until_reuploaded() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 0);
        seed(&os).await;
        run(&app, false).await.unwrap(); // mark the orphans

        // The object exists but its mark makes blob_exists answer false on
        // every replica, so a manifest push can never adopt it — the client's
        // only path is re-uploading the blob.
        let orphan = sha('d');
        assert!(os.head(&blob_key(&orphan)).await.unwrap());
        assert!(!crate::truth::blob_exists(&app, &orphan).await.unwrap());

        // Which is exactly what the upload finalize does: fresh object, mark
        // cleared, row written — after which adoption is safe again.
        os.put(&blob_key(&orphan), b"orphaned layer").await.unwrap();
        crate::truth::clear_pending(&app, &orphan).await.unwrap();
        let d = orphan.clone();
        db::run_write(&app.pool, move |conn| {
            use crate::schema::blobs as b;
            diesel::insert_into(b::table)
                .values((b::digest.eq(&d), b::size.eq(14), b::created_at.eq(db::now())))
                .execute(conn)?;
            Ok(())
        })
        .await
        .unwrap();
        assert!(crate::truth::blob_exists(&app, &orphan).await.unwrap());

        // ...and the push the client retries commits a manifest over it.
        adopt(&os, &orphan).await;

        let report = run(&app, false).await.unwrap();
        assert_eq!(report.orphan_blobs_deleted, 0);
        assert!(os.head(&blob_key(&orphan)).await.unwrap());
        assert!(!os.head(&manifest_key(&sha('c'))).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    /// Delegates to FsObjectStore, but the first CAS write to the pending
    /// ledger is preempted by a mark-clearing write — a push's rescue racing
    /// the sweep's ledger rewrite.
    struct RescueStore {
        inner: Arc<FsObjectStore>,
        victim: String,
        fired: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl ObjectStore for RescueStore {
        async fn get(&self, key: &str) -> anyhow::Result<Option<(Vec<u8>, String)>> {
            self.inner.get(key).await
        }
        async fn get_if_none_match(
            &self,
            key: &str,
            etag: &str,
        ) -> anyhow::Result<crate::objectstore::Fetch> {
            self.inner.get_if_none_match(key, etag).await
        }
        async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<String> {
            self.inner.put(key, bytes).await
        }
        async fn put_if_match(
            &self,
            key: &str,
            bytes: &[u8],
            etag: Option<&str>,
        ) -> anyhow::Result<crate::objectstore::Cas> {
            if key == crate::truth::PENDING_KEY
                && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let (b, _) = self.inner.get(key).await?.unwrap();
                let mut doc: crate::truth::PendingDoc = serde_json::from_slice(&b)?;
                doc.digests.remove(&self.victim);
                self.inner.put(key, &serde_json::to_vec(&doc)?).await?;
            }
            self.inner.put_if_match(key, bytes, etag).await
        }
        async fn head(&self, key: &str) -> anyhow::Result<bool> {
            self.inner.head(key).await
        }
        async fn stat(&self, key: &str) -> anyhow::Result<Option<i64>> {
            self.inner.stat(key).await
        }
        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.inner.delete(key).await
        }
        async fn list(&self, prefix: &str) -> anyhow::Result<Vec<crate::objectstore::Listed>> {
            self.inner.list(prefix).await
        }
        async fn put_file(&self, key: &str, path: &std::path::Path) -> anyhow::Result<()> {
            self.inner.put_file(key, path).await
        }
        async fn get_to_file(&self, key: &str, path: &std::path::Path) -> anyhow::Result<bool> {
            self.inner.get_to_file(key, path).await
        }
    }

    #[tokio::test]
    async fn a_concurrent_rescue_is_not_resurrected_by_the_ledger_rewrite() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 3600);
        seed(&os).await;
        run(&app, false).await.unwrap(); // first sweep marks both orphans

        // This run's ledger rewrite races a rescue of the blob: the CAS
        // conflict must make the merge drop the cleared mark for good rather
        // than re-adding it from the up-front snapshot.
        let victim = sha('d');
        let rescue = Arc::new(RescueStore {
            inner: os,
            victim: victim.clone(),
            fired: std::sync::atomic::AtomicBool::new(false),
        });
        let data = root.join("data");
        let app2: AppRef = Arc::new(App {
            pool: app.pool.clone(),
            store: Store::new(data.to_str().unwrap()).unwrap(),
            cfg: app.cfg.clone(),
            object: Some(rescue.clone()),
            repo_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            gc_lock: tokio::sync::RwLock::new(()),
        });
        run(&app2, false).await.unwrap();

        let as_dyn: Arc<dyn ObjectStore> = rescue;
        let doc = crate::truth::read_pending(&as_dyn).await.unwrap();
        assert!(!doc.digests.contains_key(&victim));
        assert!(doc.digests.contains_key(&sha('c')));

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_rescue_waits_out_a_deletion_claim_then_rewrites() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 0);
        seed(&os).await;
        run(&app, false).await.unwrap(); // mark the orphans

        // A sweep claims the blob — its delete is imminent.
        let orphan = sha('d');
        assert!(crate::truth::claim_pending(&app, &orphan, "sweep-1").await.unwrap());

        // A rescuer arriving mid-claim must wait, not clear-and-write past it:
        // its PUT has to land after the delete, or it IS the delete's target.
        let app2 = app.clone();
        let o = orphan.clone();
        let rescue =
            tokio::spawn(async move { crate::truth::clear_pending(&app2, &o).await });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(!rescue.is_finished());

        // The delete completes and the claim is released.
        os.delete(&blob_key(&orphan)).await.unwrap();
        crate::truth::release_pending(&app, &orphan, "sweep-1", false).await.unwrap();
        assert!(rescue.await.unwrap().unwrap());

        // The rescuer's caller re-PUTs; the object is back and unmarked.
        os.put(&blob_key(&orphan), b"orphaned layer").await.unwrap();
        assert!(crate::truth::blob_exists(&app, &orphan).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_stalled_sweeps_late_delete_is_fenced_off() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 0);
        seed(&os).await;

        // A sweep claimed the orphan blob, then stalled past the claim TTL.
        let orphan = sha('d');
        let mut doc = crate::truth::PendingDoc::default();
        doc.deleting.insert(
            orphan.clone(),
            crate::truth::Claim {
                at: db::now() - crate::truth::CLAIM_TTL_SECS - 1,
                owner: "dead-sweep".into(),
            },
        );
        os.put(crate::truth::PENDING_KEY, &serde_json::to_vec(&doc).unwrap())
            .await
            .unwrap();

        // The rescuer force-clears the stale claim and rewrites the object.
        assert!(crate::truth::clear_pending(&app, &orphan).await.unwrap());
        os.put(&blob_key(&orphan), b"rescued layer").await.unwrap();

        // The dead sweep wakes and asks to renew — the claim isn't its own
        // anymore, so the delete it was about to run is fenced off.
        assert!(!crate::truth::renew_pending(&app, &orphan, "dead-sweep").await.unwrap());
        assert!(os.head(&blob_key(&orphan)).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn orphan_sweep_respects_the_grace_window() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 3600);
        seed(&os).await;

        // The orphans were written moments ago: inside the grace window they
        // are indistinguishable from an in-flight push, so even on their
        // second sighting they stay.
        run(&app, false).await.unwrap();
        let report = run(&app, false).await.unwrap();
        assert_eq!(report.orphan_manifests_deleted, 0);
        assert_eq!(report.orphan_blobs_deleted, 0);
        assert!(os.head(&manifest_key(&sha('c'))).await.unwrap());
        assert!(os.head(&blob_key(&sha('d'))).await.unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    /// Write a file into the local blob cache directly — the leftover of a
    /// read-through fill whose blob has since dropped out of every bucket
    /// index — aged `age` seconds into the past.
    fn cache_file(app: &AppRef, digest: &str, body: &[u8], age: i64) -> std::path::PathBuf {
        let path = app.store.blob_path(digest);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, body).unwrap();
        let t = filetime::FileTime::from_unix_time(db::now() - age, 0);
        filetime::set_file_mtime(&path, t).unwrap();
        path
    }

    async fn insert_blob_row(app: &AppRef, digest: &str, size: i64) {
        let d = digest.to_string();
        db::run_write(&app.pool, move |conn| {
            use crate::schema::blobs as b;
            diesel::insert_into(b::table)
                .values((b::digest.eq(&d), b::size.eq(size), b::created_at.eq(db::now())))
                .execute(conn)?;
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn local_cache_sweep_reclaims_old_rowless_files_only() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 3600);
        seed(&os).await;

        // Three cache files: an old one with no row (the leak), a young one
        // with no row (a fill or upload still committing), and an old one
        // that a manifest references. Plus a staging file,
        // which is never the sweep's business.
        let leaked = cache_file(&app, &sha('1'), b"leaked layer", 7200);
        let young = cache_file(&app, &sha('2'), b"in-flight", 0);
        let live = cache_file(&app, &sha('b'), b"live layer", 7200);
        let staging = app.store.staging_path("fill-test");
        std::fs::write(&staging, b"partial").unwrap();
        let t = filetime::FileTime::from_unix_time(db::now() - 7200, 0);
        filetime::set_file_mtime(&staging, t).unwrap();

        // Dry run reports the leak and touches nothing.
        let dry = run(&app, true).await.unwrap();
        assert_eq!(dry.local_cache_files_deleted, 1);
        assert_eq!(dry.local_cache_bytes_freed, "leaked layer".len() as i64);
        assert!(leaked.exists());

        let report = run(&app, false).await.unwrap();
        assert_eq!(report.local_cache_files_deleted, 1);
        assert_eq!(report.local_cache_bytes_freed, "leaked layer".len() as i64);
        assert!(!leaked.exists());
        assert!(young.exists());
        assert!(live.exists());
        assert!(staging.exists());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn local_cache_sweep_keeps_a_file_whose_row_is_unreferenced() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, os) = app(&root, 3600);
        seed(&os).await;

        // A row without a manifest reference is still a row: phase 2 owns
        // its lifecycle (row, file and object go together once it ages
        // out), so the cache sweep must not take the file from under it.
        let rowed = cache_file(&app, &sha('3'), b"rowed layer", 7200);
        insert_blob_row(&app, &sha('3'), 11).await;

        let report = run(&app, false).await.unwrap();
        assert_eq!(report.local_cache_files_deleted, 0);
        assert!(rowed.exists());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn local_cache_sweep_does_not_run_in_local_mode() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let data = root.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let app: AppRef = Arc::new(App {
            pool: db::init(data.to_str().unwrap()).unwrap(),
            store: Store::new(data.to_str().unwrap()).unwrap(),
            cfg: Config { gc_grace_seconds: 0, ..Config::default() },
            object: None,
            repo_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            gc_lock: tokio::sync::RwLock::new(()),
        });

        // In local mode the blob directory is the truth. A row-less file is
        // not provably garbage, so even an ancient one stays.
        let file = cache_file(&app, &sha('4'), b"local truth", 7200);
        let report = run(&app, false).await.unwrap();
        assert_eq!(report.local_cache_files_deleted, 0);
        assert_eq!(report.local_cache_bytes_freed, 0);
        assert!(file.exists());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_dedup_commit_refreshes_the_kept_files_mtime() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, _os) = app(&root, 3600);

        // An upload of a digest whose leaked file is still on disk dedups
        // onto it. The file must come back inside the grace window, or the
        // sweep could unlink it before the upload's row is written.
        let leaked = cache_file(&app, &sha('5'), b"same bytes", 7200);
        app.store.create_staging("up-1").await.unwrap();
        std::fs::write(app.store.staging_path("up-1"), b"same bytes").unwrap();
        app.store.commit("up-1", &sha('5')).await.unwrap();

        let meta = std::fs::metadata(&leaked).unwrap();
        assert!(mtime_secs(&meta) > db::now() - 60);
        assert!(!app.store.staging_path("up-1").exists());

        let report = run(&app, false).await.unwrap();
        assert_eq!(report.local_cache_files_deleted, 0);
        assert!(leaked.exists());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_dedup_commit_onto_a_read_only_file_replaces_it() {
        let root = std::env::temp_dir().join(format!("breezy-gc-test-{}", uuid::Uuid::new_v4()));
        let (app, _os) = app(&root, 3600);

        // An operator made the cache read-only. The touch can't refresh the
        // mtime, so the staged copy is renamed over the old file instead —
        // the upload still succeeds and the file is still fresh.
        let leaked = cache_file(&app, &sha('6'), b"same bytes", 7200);
        let mut perms = std::fs::metadata(&leaked).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&leaked, perms).unwrap();
        app.store.create_staging("up-2").await.unwrap();
        std::fs::write(app.store.staging_path("up-2"), b"same bytes").unwrap();
        app.store.commit("up-2", &sha('6')).await.unwrap();

        let meta = std::fs::metadata(&leaked).unwrap();
        assert!(mtime_secs(&meta) > db::now() - 60);
        assert!(!app.store.staging_path("up-2").exists());
        assert_eq!(std::fs::read(&leaked).unwrap(), b"same bytes");

        std::fs::remove_dir_all(&root).ok();
    }
}
