//! Retention: the policy layer above GC.
//!
//! GC (gc.rs) is mechanism — it collects whatever is unreachable from the
//! tags, and is deliberately unable to decide that a tagged image has outlived
//! its usefulness. This module is the policy that makes that decision: on a
//! schedule, untag the mechanically-minted tags nothing will come back for,
//! then run one GC pass so the bytes actually leave.
//!
//! The policy is bounded on purpose. Only tags matching `tag_pattern` are ever
//! candidates (default: a bare 40-hex commit SHA — the shape CI mints on every
//! push and nobody types by hand); anything a human named is out of scope, and
//! `protect` pins individual `repo:tag` pairs for the cases only the operator
//! can know about, like a deployment frozen on an old build. Within the
//! candidates, the newest `keep_newest` survive (the rollback window), and so
//! does anything pushed within `keep_days` — recency as an independent guard,
//! so a burst of pushes cannot age yesterday's rollback target out of the
//! count-based window — and so does any candidate whose manifest a
//! non-candidate tag also names: `app:production` pointing at the same
//! manifest as `app:<sha>` is the operator saying that build is in use, and
//! the deployment pulls it by the sha, so untagging the sha would leave the
//! manifest alive and the deployment unable to pull it.
//!
//! Off by default. A registry that starts deleting images because it was
//! upgraded — rather than because its operator wrote a policy — is a data-loss
//! bug wearing a feature's name, so `[retention] enabled = true` is the opt-in,
//! and a sweep with the default pattern touches nothing a human pushed.

use std::collections::{HashMap, HashSet};

use diesel::prelude::*;

use crate::config::RetentionCfg;
use crate::{db, gc, AppRef};

#[derive(Debug, serde::Serialize)]
pub struct RetentionReport {
    pub dry_run: bool,
    /// Tags removed, per repository.
    pub deleted: HashMap<String, Vec<String>>,
    pub tags_deleted: usize,
    pub tags_kept: usize,
    /// The GC pass that followed, when one ran.
    pub gc: Option<gc::GcReport>,
}

/// One tag of a repository, as retention sees it: its name, when it was
/// pushed, and the manifest it names.
pub type TagRow = (String, i64, i64);

/// Which of one repository's tags the policy gives up on.
///
/// Pure, so the policy is testable without a database: `tags` is every tag in
/// the repo, and the result is the subset to delete. Everything here is a
/// *keep* rule — a tag survives if ANY rule wants it — because the failure
/// mode that matters is deleting something needed, and a disjunction of keeps
/// fails toward keeping.
pub fn select_victims(
    repo: &str,
    tags: &[TagRow],
    now: i64,
    cfg: &RetentionCfg,
    pattern: &regex::Regex,
) -> Vec<String> {
    // Manifests a non-candidate tag names. A candidate pointing at one is in
    // use under another name, and whoever pulls it by the candidate's name
    // (a chart pinned to the sha) must still be able to.
    let held: HashSet<i64> = tags
        .iter()
        .filter(|(name, _, _)| !pattern.is_match(name))
        .map(|(_, _, manifest)| *manifest)
        .collect();

    let mut candidates: Vec<&TagRow> = tags
        .iter()
        .filter(|(name, _, _)| pattern.is_match(name))
        .collect();
    // Newest first; ties broken by name so the order is total and a re-run
    // selects identically.
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let cutoff = now - cfg.keep_days * 86_400;
    candidates
        .iter()
        .enumerate()
        .filter(|(i, (name, pushed, manifest))| {
            *i >= cfg.keep_newest
                && *pushed < cutoff
                && !held.contains(manifest)
                && !cfg.protect.iter().any(|p| p == &format!("{repo}:{name}"))
        })
        .map(|(_, (name, _, _))| name.clone())
        .collect()
}

/// One sweep: select victims in every repository, untag them, GC.
///
/// Deletion goes through the same paths a client's DELETE takes — one CAS'd
/// index mutation per repo in object mode, tag rows in local mode — so the
/// provenance log records the sweep (`action: "retention"`) and a concurrent
/// push loses nothing: a tag re-pushed between selection and mutation simply
/// isn't in the index snapshot the CAS rewrites, and the younger-than-grace
/// manifest it points at is untouchable by the GC that follows.
pub async fn run(app: &AppRef, dry_run: bool) -> anyhow::Result<RetentionReport> {
    let cfg = app.cfg.retention.clone();
    let pattern = regex::Regex::new(&cfg.tag_pattern)
        .map_err(|e| anyhow::anyhow!("retention.tag_pattern does not parse: {e}"))?;

    // In object mode the bucket is the truth; selecting from a stale cache
    // could resurrect a tag another replica already deleted, or miss one it
    // pushed. GC does the same re-sync for the same reason.
    if app.object.is_some() {
        crate::truth::rebuild_all(app).await?;
    }

    let all: Vec<(String, String, i64, i64)> = db::run(&app.pool, move |conn| {
        use crate::schema::{repos as rp, tags as t};
        Ok(t::table
            .inner_join(rp::table.on(rp::id.eq(t::repo_id)))
            .select((rp::name, t::name, t::pushed_at, t::manifest_id))
            .load(conn)?)
    })
    .await?;

    let mut by_repo: HashMap<String, Vec<TagRow>> = HashMap::new();
    for (repo, tag, pushed, manifest) in all {
        by_repo.entry(repo).or_default().push((tag, pushed, manifest));
    }

    let now = db::now();
    let total_tags: usize = by_repo.values().map(Vec::len).sum();
    let mut deleted: HashMap<String, Vec<String>> = HashMap::new();

    for (repo, tags) in &by_repo {
        let victims = select_victims(repo, tags, now, &cfg, &pattern);
        if victims.is_empty() {
            continue;
        }

        if !dry_run {
            untag(app, repo, &victims).await?;
        }
        deleted.insert(repo.clone(), victims);
    }

    let tags_deleted: usize = deleted.values().map(Vec::len).sum();
    let gc_report = if dry_run || !cfg.run_gc {
        None
    } else {
        Some(gc::run(app, false).await?)
    };

    Ok(RetentionReport {
        dry_run,
        deleted,
        tags_deleted,
        tags_kept: total_tags - tags_deleted,
        gc: gc_report,
    })
}

/// Removes a batch of tags from one repository, through the same write path a
/// client DELETE uses.
async fn untag(app: &AppRef, repo: &str, victims: &[String]) -> anyhow::Result<()> {
    if app.object.is_some() {
        let batch: HashSet<String> = victims.iter().cloned().collect();
        let outcome = crate::truth::mutate(app, repo, Some("retention"), move |doc| {
            for tag in &batch {
                doc.tags.remove(tag);
            }
            Ok(crate::truth::LogInfo {
                action: "retention",
                tag: None,
                digest: None,
            })
        })
        .await?;
        if let Err(msg) = outcome {
            anyhow::bail!("retention index update for {repo} rejected: {msg}");
        }
        return Ok(());
    }

    let (repo2, names) = (repo.to_string(), victims.to_vec());
    db::run_write(&app.pool, move |conn| {
        use crate::schema::tags as t;
        let Some(rid) = db::repo_id(conn, &repo2)? else {
            return Ok(());
        };
        diesel::delete(t::table.filter(t::repo_id.eq(rid).and(t::name.eq_any(&names))))
            .execute(conn)?;
        Ok(())
    })
    .await
}

/// The background loop `serve` spawns when retention is enabled.
///
/// First run happens one full interval after boot, not immediately: a crash
/// loop must not become a sweep loop, and the operator who just enabled the
/// policy gets one interval to dry-run it by hand via the API first.
pub async fn retention_loop(app: AppRef) {
    let interval = app.cfg.retention.interval_seconds.max(60) as u64;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        match run(&app, false).await {
            Ok(report) => tracing::info!(
                "retention: deleted {} tags across {} repos, kept {}{}",
                report.tags_deleted,
                report.deleted.len(),
                report.tags_kept,
                report
                    .gc
                    .map(|g| format!(
                        "; gc freed {} bytes ({} manifests, {} blobs); local cache freed {} bytes ({} files)",
                        g.bytes_freed,
                        g.manifests_deleted,
                        g.blobs_deleted,
                        g.local_cache_bytes_freed,
                        g.local_cache_files_deleted
                    ))
                    .unwrap_or_default(),
            ),
            Err(e) => tracing::error!("retention sweep failed: {e:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(keep_newest: usize, keep_days: i64, protect: Vec<String>) -> RetentionCfg {
        RetentionCfg {
            enabled: true,
            interval_seconds: 86_400,
            keep_newest,
            keep_days,
            tag_pattern: r"^[0-9a-f]{40}$".into(),
            protect,
            run_gc: true,
        }
    }

    fn sha(n: u8) -> String {
        format!("{:040x}", n)
    }

    /// A tag on its own manifest, numbered after it, so no two collide
    /// unless a test builds a shared one on purpose.
    fn tag(name: impl Into<String>, pushed: i64, manifest: i64) -> (String, i64, i64) {
        (name.into(), pushed, manifest)
    }

    const DAY: i64 = 86_400;
    const NOW: i64 = 1_000 * DAY;

    fn re(cfg: &RetentionCfg) -> regex::Regex {
        regex::Regex::new(&cfg.tag_pattern).unwrap()
    }

    #[test]
    fn keeps_the_newest_n_and_deletes_the_rest() {
        // 5 old SHA tags, newest-2 window, no recency rescue.
        let tags: Vec<TagRow> = (0..5)
            .map(|i| tag(sha(i), NOW - 30 * DAY + i as i64, i as i64))
            .collect();
        let c = cfg(2, 2, vec![]);
        let victims = select_victims("team/app", &tags, NOW, &c, &re(&c));
        // Newest two are sha(4), sha(3); the rest go.
        assert_eq!(victims.len(), 3);
        assert!(!victims.contains(&sha(4)));
        assert!(!victims.contains(&sha(3)));
    }

    #[test]
    fn recency_rescues_beyond_the_count_window() {
        // Ten tags pushed an hour ago: all inside keep_days, none deleted even
        // though keep_newest is 1. A merge burst must not eat its own tail.
        let tags: Vec<TagRow> = (0..10).map(|i| tag(sha(i), NOW - 3_600, i as i64)).collect();
        let c = cfg(1, 2, vec![]);
        assert!(select_victims("team/app", &tags, NOW, &c, &re(&c)).is_empty());
    }

    #[test]
    fn human_named_tags_are_never_candidates() {
        let tags = vec![
            tag("latest", NOW - 100 * DAY, 10),
            tag("v1.2.3", NOW - 100 * DAY, 11),
            tag(sha(1), NOW - 100 * DAY, 1),
        ];
        // keep_newest 0: even with no rollback window, only the SHA is up for
        // deletion — deleting `latest` would take the tag every deploy pulls.
        let c = cfg(0, 2, vec![]);
        let victims = select_victims("team/app", &tags, NOW, &c, &re(&c));
        assert_eq!(victims, vec![sha(1)]);
    }

    #[test]
    fn protect_pins_a_tag_the_policy_would_take() {
        // The case only the operator can know: a deployment frozen on an old
        // build whose tag has aged out of every automatic window.
        let tags: Vec<TagRow> = (0..5)
            .map(|i| tag(sha(i), NOW - 30 * DAY + i as i64, i as i64))
            .collect();
        let c = cfg(1, 2, vec![format!("team/app:{}", sha(0))]);
        let victims = select_victims("team/app", &tags, NOW, &c, &re(&c));
        assert!(!victims.contains(&sha(0)));
        // Protection is per-repo: the same tag name in another repo still goes.
        let other = select_victims("team/other", &tags, NOW, &c, &re(&c));
        assert!(other.contains(&sha(0)));
    }

    #[test]
    fn an_alias_keeps_the_sha_tag_of_its_manifest() {
        // The deploy story: `production` is a second name for the manifest
        // sha(0) names, an old build every other window has given up on. The
        // chart pulls it by the sha, so the sha must stay; its neighbours
        // with no alias still go.
        let mut tags: Vec<TagRow> = (0..5)
            .map(|i| tag(sha(i), NOW - 30 * DAY + i as i64, i as i64))
            .collect();
        tags.push(tag("production", NOW - 30 * DAY, 0));
        let c = cfg(1, 2, vec![]);
        let victims = select_victims("team/app", &tags, NOW, &c, &re(&c));
        assert!(!victims.contains(&sha(0)));
        assert!(!victims.contains(&"production".to_string()));
        assert_eq!(victims, vec![sha(3), sha(2), sha(1)]);
    }

    #[test]
    fn a_shared_manifest_between_two_candidates_rescues_neither() {
        // Two sha tags on one manifest (a re-push of an identical build) hold
        // each other up no more than one would: only an alias counts.
        let tags = vec![
            tag(sha(0), NOW - 30 * DAY, 0),
            tag(sha(1), NOW - 30 * DAY + 1, 0),
            tag(sha(2), NOW - 30 * DAY + 2, 2),
        ];
        let c = cfg(1, 2, vec![]);
        let victims = select_victims("team/app", &tags, NOW, &c, &re(&c));
        assert_eq!(victims, vec![sha(1), sha(0)]);
    }

    #[test]
    fn selection_is_deterministic_under_equal_timestamps() {
        // Same pushed_at everywhere: the name tiebreak makes two runs agree on
        // which tags sit inside the newest-N window.
        let tags: Vec<TagRow> = (0..6).map(|i| tag(sha(i), NOW - 30 * DAY, i as i64)).collect();
        let c = cfg(3, 2, vec![]);
        let a = select_victims("team/app", &tags, NOW, &c, &re(&c));
        let b = select_victims("team/app", &tags, NOW, &c, &re(&c));
        assert_eq!(a, b);
        assert_eq!(a.len(), 3);
    }

    #[test]
    fn an_empty_repo_selects_nothing() {
        let c = cfg(10, 2, vec![]);
        assert!(select_victims("team/app", &[], NOW, &c, &re(&c)).is_empty());
    }
}
