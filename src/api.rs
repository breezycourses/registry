use crate::auth::{authorize, Action, Identity, Role};
use crate::db;
use crate::oci::errors::internal;
use crate::AppRef;
use crate::{gc, retention};
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Json, Response};
use axum::Extension;
use diesel::prelude::*;
use std::collections::HashMap;

pub async fn whoami(State(_app): State<AppRef>, Extension(id): Extension<Identity>) -> Response {
    let role = match id.role {
        Role::Admin => "admin",
        Role::Push => "push",
        Role::Pull => "pull",
        Role::Anonymous => "anonymous",
    };
    Json(serde_json::json!({ "username": id.username, "role": role })).into_response()
}

pub async fn stats(State(app): State<AppRef>, Extension(id): Extension<Identity>) -> Response {
    if let Err(resp) = authorize(&app, &id, Action::Pull) {
        return resp;
    }
    let mode = if app.object.is_some() {
        "object"
    } else {
        "local"
    };
    let result = db::run(&app.pool, move |conn| {
        use crate::schema::{blobs as b, manifests as m, repos as rp, tags as t};
        use diesel::dsl::count_star;
        let repos: i64 = rp::table.select(count_star()).get_result(conn)?;
        let tags: i64 = t::table.select(count_star()).get_result(conn)?;
        let manifests: i64 = m::table.select(count_star()).get_result(conn)?;
        let blobs: i64 = b::table.select(count_star()).get_result(conn)?;
        let storage: Option<i64> = b::table
            .select(diesel::dsl::sql::<
                diesel::sql_types::Nullable<diesel::sql_types::BigInt>,
            >("SUM(size)"))
            .get_result(conn)?;
        Ok(serde_json::json!({
            "repos": repos,
            "tags": tags,
            "manifests": manifests,
            "blobs": blobs,
            "storageBytes": storage.unwrap_or(0),
        }))
    })
    .await;
    match result {
        Ok(mut v) => {
            v["mode"] = serde_json::Value::String(mode.into());
            Json(v).into_response()
        }
        Err(e) => internal(e),
    }
}

pub async fn repos(State(app): State<AppRef>, Extension(id): Extension<Identity>) -> Response {
    if let Err(resp) = authorize(&app, &id, Action::Pull) {
        return resp;
    }
    use crate::schema::{blobs as b, manifest_refs as r, manifests as m, repos as rp, tags as t};
    let result = db::run(&app.pool, move |conn| {
        let repo_rows: Vec<(i64, String)> = rp::table.select((rp::id, rp::name)).load(conn)?;
        let mut out: Vec<serde_json::Value> = vec![];
        for (rid, name) in repo_rows {
            let tag_count: i64 = t::table
                .filter(t::repo_id.eq(rid))
                .count()
                .get_result(conn)?;
            let last_pushed: Option<i64> = t::table
                .filter(t::repo_id.eq(rid))
                .select(diesel::dsl::max(t::pushed_at))
                .first(conn)?;
            let blob_digests: Vec<String> = r::table
                .inner_join(m::table)
                .filter(m::repo_id.eq(rid).and(r::kind.eq("blob")))
                .select(r::child_digest)
                .distinct()
                .load(conn)?;
            let mut size: i64 = 0;
            for chunk in blob_digests.chunks(500) {
                let sizes: Vec<i64> = b::table
                    .filter(b::digest.eq_any(chunk))
                    .select(b::size)
                    .load(conn)?;
                size += sizes.iter().sum::<i64>();
            }
            out.push(serde_json::json!({
                "name": name,
                "tags": tag_count,
                "sizeBytes": size,
                "lastPushed": last_pushed,
            }));
        }
        out.sort_by_key(|v| v["name"].as_str().unwrap_or("").to_string());
        Ok(out)
    })
    .await;
    match result {
        Ok(list) => Json(serde_json::json!({ "repos": list })).into_response(),
        Err(e) => internal(e),
    }
}

pub async fn tags(
    State(app): State<AppRef>,
    Extension(id): Extension<Identity>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Err(resp) = authorize(&app, &id, Action::Pull) {
        return resp;
    }
    let Some(repo) = params.get("repo").cloned() else {
        return Json(serde_json::json!({ "error": "repo parameter required" })).into_response();
    };
    use crate::schema::{blobs as b, manifest_refs as r, manifests as m, tags as t};
    let result = db::run(&app.pool, move |conn| {
        let Some(rid) = db::repo_id(conn, &repo)? else {
            return Ok(serde_json::json!({ "repo": repo, "tags": [] }));
        };
        let tag_rows: Vec<(String, i64, i64)> = t::table
            .filter(t::repo_id.eq(rid))
            .order(t::name.asc())
            .select((t::name, t::manifest_id, t::pushed_at))
            .load(conn)?;

        let mut out = vec![];
        for (tag_name, mid, pushed_at) in tag_rows {
            let (digest, media_type, msize): (String, String, i64) = m::table
                .filter(m::id.eq(mid))
                .select((m::digest, m::media_type, m::size))
                .first(conn)?;

            // Total size: this manifest's blobs, plus (one level down) the blobs of
            // any child manifests when this is a multi-arch index.
            let mut blob_digests: Vec<String> = r::table
                .filter(r::manifest_id.eq(mid).and(r::kind.eq("blob")))
                .select(r::child_digest)
                .load(conn)?;
            let child_manifest_digests: Vec<String> = r::table
                .filter(r::manifest_id.eq(mid).and(r::kind.eq("manifest")))
                .select(r::child_digest)
                .load(conn)?;
            for child in &child_manifest_digests {
                let cid: Option<i64> = m::table
                    .filter(m::repo_id.eq(rid).and(m::digest.eq(child)))
                    .select(m::id)
                    .first(conn)
                    .optional()?;
                if let Some(cid) = cid {
                    let mut child_blobs: Vec<String> = r::table
                        .filter(r::manifest_id.eq(cid).and(r::kind.eq("blob")))
                        .select(r::child_digest)
                        .load(conn)?;
                    blob_digests.append(&mut child_blobs);
                }
            }
            blob_digests.sort();
            blob_digests.dedup();
            let mut total: i64 = msize;
            for chunk in blob_digests.chunks(500) {
                let sizes: Vec<i64> = b::table
                    .filter(b::digest.eq_any(chunk))
                    .select(b::size)
                    .load(conn)?;
                total += sizes.iter().sum::<i64>();
            }

            out.push(serde_json::json!({
                "name": tag_name,
                "digest": digest,
                "mediaType": media_type,
                "size": total,
                "pushedAt": pushed_at,
                "isIndex": !child_manifest_digests.is_empty(),
            }));
        }
        Ok(serde_json::json!({ "repo": repo, "tags": out }))
    })
    .await;
    match result {
        Ok(v) => Json(v).into_response(),
        Err(e) => internal(e),
    }
}

pub async fn gc_run(
    State(app): State<AppRef>,
    Extension(id): Extension<Identity>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Err(resp) = authorize(&app, &id, Action::Admin) {
        return resp;
    }
    let dry_run = params
        .get("dry_run")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    match gc::run(&app, dry_run).await {
        Ok(report) => Json(serde_json::json!(report)).into_response(),
        Err(e) => internal(e),
    }
}

/// One retention sweep, on demand. The manual counterpart of the loop the
/// config schedules — and usable with the loop off, for operators who would
/// rather own the schedule (a CronJob, a CI step) than run a policy daemon.
/// `dry_run` reports the victims without touching anything, which is the
/// intended first call after writing a policy.
pub async fn retention_run(
    State(app): State<AppRef>,
    Extension(id): Extension<Identity>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Err(resp) = authorize(&app, &id, Action::Admin) {
        return resp;
    }
    let dry_run = params
        .get("dry_run")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    match retention::run(&app, dry_run).await {
        Ok(report) => Json(serde_json::json!(report)).into_response(),
        Err(e) => internal(e),
    }
}
