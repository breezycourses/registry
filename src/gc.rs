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
pub async fn run(app: &AppRef, dry_run: bool) -> anyhow::Result<GcReport> {
    if app.object.is_some() {
        crate::truth::rebuild_all(app).await?;
    }
    let grace = app.cfg.gc_grace_seconds;

    // Phase 1: mark. No deletions here.
    let (victims, blob_victims) = db::run(&app.pool, move |conn| {
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
            .into_iter()
            .filter(|(d, _, created)| !live_blobs.contains(d.as_str()) && *created <= cutoff)
            .map(|(d, s, _)| (d, s))
            .collect();
        Ok((victims, blob_victims))
    })
    .await?;

    let report = GcReport {
        dry_run,
        manifests_deleted: victims.len(),
        blobs_deleted: blob_victims.len(),
        bytes_freed: blob_victims.iter().map(|(_, s)| s).sum(),
    };
    if dry_run {
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
        db::run(&app.pool, move |conn| {
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
    let actually_deleted: Vec<String> = db::run(&app.pool, move |conn| {
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
    Ok(report)
}
