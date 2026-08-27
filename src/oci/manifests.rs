use super::errors::*;
use super::{valid_digest, valid_tag};
use crate::auth::{authorize, Action, Identity};
use crate::db;
use crate::AppRef;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use diesel::prelude::*;
use sha2::Digest;
use std::collections::HashMap;

const MANIFEST_LIMIT: usize = 4 * 1024 * 1024;
const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";

pub async fn put(
    app: &AppRef,
    id: &Identity,
    name: &str,
    reference: &str,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Push) {
        return resp;
    }
    let bytes = match axum::body::to_bytes(body, MANIFEST_LIMIT).await {
        Ok(b) => b,
        Err(_) => {
            return oci_error(
                StatusCode::BAD_REQUEST,
                "MANIFEST_INVALID",
                "manifest too large or unreadable",
            )
        }
    };
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));

    // The reference is either a tag to move, or a digest that must match the content.
    let tag = if reference.starts_with("sha256:") {
        if reference != digest {
            return digest_invalid("provided digest does not match manifest content");
        }
        None
    } else {
        if !valid_tag(reference) {
            return oci_error(StatusCode::BAD_REQUEST, "MANIFEST_INVALID", "invalid tag");
        }
        Some(reference.to_string())
    };

    let parsed: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            return oci_error(
                StatusCode::BAD_REQUEST,
                "MANIFEST_INVALID",
                "manifest is not valid JSON",
            )
        }
    };

    let media_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_string())
        .filter(|v| !v.is_empty() && v != "application/octet-stream")
        .or_else(|| {
            parsed
                .get("mediaType")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .unwrap_or_else(|| OCI_MANIFEST.to_string());

    // Collect references: child manifests (for an index) and blobs (config + layers).
    let mut child_manifests: Vec<String> = vec![];
    let mut child_blobs: Vec<String> = vec![];
    if let Some(list) = parsed.get("manifests").and_then(|v| v.as_array()) {
        for m in list {
            match m.get("digest").and_then(|d| d.as_str()) {
                Some(d) if valid_digest(d) => child_manifests.push(d.to_string()),
                _ => {
                    return oci_error(
                        StatusCode::BAD_REQUEST,
                        "MANIFEST_INVALID",
                        "index entry missing valid digest",
                    )
                }
            }
        }
    }
    if let Some(d) = parsed
        .get("config")
        .and_then(|c| c.get("digest"))
        .and_then(|d| d.as_str())
    {
        if !valid_digest(d) {
            return digest_invalid("config digest invalid");
        }
        child_blobs.push(d.to_string());
    }
    if let Some(layers) = parsed.get("layers").and_then(|v| v.as_array()) {
        for l in layers {
            match l.get("digest").and_then(|d| d.as_str()) {
                Some(d) if valid_digest(d) => child_blobs.push(d.to_string()),
                _ => {
                    return oci_error(
                        StatusCode::BAD_REQUEST,
                        "MANIFEST_INVALID",
                        "layer missing valid digest",
                    )
                }
            }
        }
    }
    child_manifests.sort();
    child_manifests.dedup();
    child_blobs.sort();
    child_blobs.dedup();

    let subject = parsed
        .get("subject")
        .and_then(|s| s.get("digest"))
        .and_then(|d| d.as_str())
        .filter(|d| valid_digest(d))
        .map(String::from);
    let artifact_type = parsed
        .get("artifactType")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            parsed
                .get("config")
                .and_then(|c| c.get("mediaType"))
                .and_then(|v| v.as_str())
                .map(String::from)
        });
    let annotations = parsed.get("annotations").map(|a| a.to_string());

    // Object mode: manifest bytes become an immutable object, then the repo's
    // index is CAS-updated — that PUT is the linearization point.
    if app.object.is_some() {
        let os = app.object.as_ref().unwrap();
        if let Err(e) = os.put(&crate::truth::manifest_key(&digest), &bytes).await {
            return internal(e);
        }
        for d in &child_blobs {
            match crate::truth::blob_exists(app, d).await {
                Ok(true) => {}
                Ok(false) => {
                    return oci_error(
                        StatusCode::BAD_REQUEST,
                        "MANIFEST_BLOB_UNKNOWN",
                        &format!("blob {d} not found"),
                    )
                }
                Err(e) => return internal(e),
            }
        }
        let entry = crate::truth::ManifestEntry {
            media_type,
            size: bytes.len() as i64,
            subject: subject.clone(),
            artifact_type,
            annotations,
            created_at: crate::db::now(),
            blob_refs: child_blobs,
            manifest_refs: child_manifests,
        };
        let (digest2, tag2) = (digest.clone(), tag.clone());
        let result = crate::truth::mutate(app, name, id.username.as_deref(), move |doc| {
            for d in &entry.manifest_refs {
                if !doc.manifests.contains_key(d) {
                    return Err(format!("manifest {d} not found in repository"));
                }
            }
            doc.manifests.insert(digest2.clone(), entry.clone());
            if let Some(t) = &tag2 {
                doc.tags.insert(
                    t.clone(),
                    crate::truth::TagEntry {
                        digest: digest2.clone(),
                        pushed_at: crate::db::now(),
                    },
                );
            }
            Ok(crate::truth::LogInfo {
                action: "push",
                tag: tag2.clone(),
                digest: Some(digest2.clone()),
            })
        })
        .await;
        return match result {
            Ok(Ok(())) => {
                let mut builder = Response::builder()
                    .status(StatusCode::CREATED)
                    .header("location", format!("/v2/{name}/manifests/{digest}"))
                    .header("docker-content-digest", digest.as_str());
                if let Some(s) = subject {
                    builder = builder.header("oci-subject", s);
                }
                builder.body(Body::empty()).unwrap()
            }
            Ok(Err(missing)) => {
                oci_error(StatusCode::BAD_REQUEST, "MANIFEST_BLOB_UNKNOWN", &missing)
            }
            Err(e) => internal(e),
        };
    }

    use crate::schema::{blobs as b, manifest_refs as r, manifests as m, tags as t};
    let (name2, digest2, media2, tag2, subject2) = (
        name.to_string(),
        digest.clone(),
        media_type,
        tag,
        subject.clone(),
    );
    let payload = bytes.to_vec();
    let size = payload.len() as i64;

    let result = db::run(&app.pool, move |conn| {
        conn.immediate_transaction::<_, anyhow::Error, _>(|conn| {
            let rid = db::get_or_create_repo(conn, &name2)?;

            // A manifest may only reference content the registry already has.
            for d in &child_blobs {
                let exists = b::table
                    .filter(b::digest.eq(d))
                    .select(b::digest)
                    .first::<String>(conn)
                    .optional()?
                    .is_some();
                if !exists {
                    return Ok(Err(format!("blob {d} not found")));
                }
            }
            for d in &child_manifests {
                let exists = m::table
                    .filter(m::repo_id.eq(rid).and(m::digest.eq(d)))
                    .select(m::id)
                    .first::<i64>(conn)
                    .optional()?
                    .is_some();
                if !exists {
                    return Ok(Err(format!("manifest {d} not found in repository")));
                }
            }

            diesel::insert_into(m::table)
                .values((
                    m::repo_id.eq(rid),
                    m::digest.eq(&digest2),
                    m::media_type.eq(&media2),
                    m::payload.eq(&payload),
                    m::size.eq(size),
                    m::subject_digest.eq(&subject2),
                    m::artifact_type.eq(&artifact_type),
                    m::annotations.eq(&annotations),
                    m::created_at.eq(db::now()),
                ))
                .on_conflict((m::repo_id, m::digest))
                .do_nothing()
                .execute(conn)?;
            let mid: i64 = m::table
                .filter(m::repo_id.eq(rid).and(m::digest.eq(&digest2)))
                .select(m::id)
                .first(conn)?;

            diesel::delete(r::table.filter(r::manifest_id.eq(mid))).execute(conn)?;
            let mut rows: Vec<_> = vec![];
            for d in &child_blobs {
                rows.push((
                    r::manifest_id.eq(mid),
                    r::child_digest.eq(d.clone()),
                    r::kind.eq("blob"),
                ));
            }
            for d in &child_manifests {
                rows.push((
                    r::manifest_id.eq(mid),
                    r::child_digest.eq(d.clone()),
                    r::kind.eq("manifest"),
                ));
            }
            if !rows.is_empty() {
                diesel::insert_into(r::table).values(rows).execute(conn)?;
            }

            if let Some(tag_name) = &tag2 {
                diesel::insert_into(t::table)
                    .values((
                        t::repo_id.eq(rid),
                        t::name.eq(tag_name),
                        t::manifest_id.eq(mid),
                        t::pushed_at.eq(db::now()),
                    ))
                    .on_conflict((t::repo_id, t::name))
                    .do_update()
                    .set((t::manifest_id.eq(mid), t::pushed_at.eq(db::now())))
                    .execute(conn)?;
            }
            Ok(Ok(()))
        })
    })
    .await;

    match result {
        Ok(Ok(())) => {
            let mut builder = Response::builder()
                .status(StatusCode::CREATED)
                .header("location", format!("/v2/{name}/manifests/{digest}"))
                .header("docker-content-digest", digest.as_str());
            if let Some(s) = subject {
                builder = builder.header("oci-subject", s);
            }
            builder.body(Body::empty()).unwrap()
        }
        Ok(Err(missing)) => oci_error(StatusCode::BAD_REQUEST, "MANIFEST_BLOB_UNKNOWN", &missing),
        Err(e) => internal(e),
    }
}

pub async fn get(
    app: &AppRef,
    id: &Identity,
    name: &str,
    reference: &str,
    head: bool,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Pull) {
        return resp;
    }
    // Object mode: one conditional GET validates the cache against the bucket,
    // so reads are consistent across every replica.
    if app.object.is_some() {
        crate::truth::refresh_repo_soft(app, name).await;
    }
    use crate::schema::{manifests as m, tags as t};
    let (name2, ref2) = (name.to_string(), reference.to_string());
    let row = db::run(&app.pool, move |conn| {
        let Some(rid) = db::repo_id(conn, &name2)? else {
            return Ok(None);
        };
        let found: Option<(Vec<u8>, String, String)> = if ref2.starts_with("sha256:") {
            m::table
                .filter(m::repo_id.eq(rid).and(m::digest.eq(&ref2)))
                .select((m::payload, m::media_type, m::digest))
                .first(conn)
                .optional()?
        } else {
            let mid: Option<i64> = t::table
                .filter(t::repo_id.eq(rid).and(t::name.eq(&ref2)))
                .select(t::manifest_id)
                .first(conn)
                .optional()?;
            match mid {
                Some(mid) => m::table
                    .filter(m::id.eq(mid))
                    .select((m::payload, m::media_type, m::digest))
                    .first(conn)
                    .optional()?,
                None => None,
            }
        };
        Ok(found)
    })
    .await;

    match row {
        Ok(Some((payload, media_type, digest))) => {
            let len = payload.len();
            let builder = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", media_type)
                .header("content-length", len)
                .header("docker-content-digest", digest);
            if head {
                builder.body(Body::empty()).unwrap()
            } else {
                builder.body(Body::from(payload)).unwrap()
            }
        }
        Ok(None) => manifest_unknown(),
        Err(e) => internal(e),
    }
}

pub async fn delete(app: &AppRef, id: &Identity, name: &str, reference: &str) -> Response {
    if let Err(resp) = authorize(app, id, Action::Admin) {
        return resp;
    }
    if app.object.is_some() {
        let is_digest = reference.starts_with("sha256:");
        let ref2 = reference.to_string();
        let result = crate::truth::mutate(app, name, id.username.as_deref(), move |doc| {
            if is_digest {
                if doc.manifests.remove(&ref2).is_none() {
                    return Err("manifest unknown".into());
                }
                let gone = ref2.clone();
                doc.tags.retain(|_, t| t.digest != gone);
                Ok(crate::truth::LogInfo {
                    action: "delete-manifest",
                    tag: None,
                    digest: Some(ref2.clone()),
                })
            } else {
                if doc.tags.remove(&ref2).is_none() {
                    return Err("tag unknown".into());
                }
                Ok(crate::truth::LogInfo {
                    action: "delete-tag",
                    tag: Some(ref2.clone()),
                    digest: None,
                })
            }
        })
        .await;
        return match result {
            Ok(Ok(())) => Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())
                .unwrap(),
            Ok(Err(_)) => manifest_unknown(),
            Err(e) => internal(e),
        };
    }
    use crate::schema::{manifests as m, tags as t};
    let (name2, ref2) = (name.to_string(), reference.to_string());
    let result = db::run(&app.pool, move |conn| {
        conn.immediate_transaction::<_, anyhow::Error, _>(|conn| {
            let Some(rid) = db::repo_id(conn, &name2)? else {
                return Ok(false);
            };
            if ref2.starts_with("sha256:") {
                let mid: Option<i64> = m::table
                    .filter(m::repo_id.eq(rid).and(m::digest.eq(&ref2)))
                    .select(m::id)
                    .first(conn)
                    .optional()?;
                let Some(mid) = mid else { return Ok(false) };
                diesel::delete(t::table.filter(t::manifest_id.eq(mid))).execute(conn)?;
                diesel::delete(m::table.filter(m::id.eq(mid))).execute(conn)?;
                Ok(true)
            } else {
                let n = diesel::delete(t::table.filter(t::repo_id.eq(rid).and(t::name.eq(&ref2))))
                    .execute(conn)?;
                Ok(n > 0)
            }
        })
    })
    .await;
    match result {
        Ok(true) => Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())
            .unwrap(),
        Ok(false) => manifest_unknown(),
        Err(e) => internal(e),
    }
}

pub async fn tags_list(
    app: &AppRef,
    id: &Identity,
    name: &str,
    query: &HashMap<String, String>,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Pull) {
        return resp;
    }
    if app.object.is_some() {
        crate::truth::refresh_repo_soft(app, name).await;
    }
    use crate::schema::tags as t;
    let name2 = name.to_string();
    let n: Option<i64> = query.get("n").and_then(|v| v.parse().ok());
    let last = query.get("last").cloned();

    let result = db::run(&app.pool, move |conn| {
        let Some(rid) = db::repo_id(conn, &name2)? else {
            return Ok(None);
        };
        let mut q = t::table
            .filter(t::repo_id.eq(rid))
            .select(t::name)
            .order(t::name.asc())
            .into_boxed();
        if let Some(last) = &last {
            q = q.filter(t::name.gt(last.clone()));
        }
        if let Some(n) = n {
            q = q.limit(n.max(0));
        }
        Ok(Some(q.load::<String>(conn)?))
    })
    .await;

    match result {
        Ok(Some(tag_names)) => {
            let truncated = n.map_or(false, |n| tag_names.len() as i64 == n && n > 0);
            let body = serde_json::json!({ "name": name, "tags": tag_names });
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json");
            if truncated {
                if let Some(last_tag) = tag_names.last() {
                    builder = builder.header(
                        "link",
                        format!(
                            "</v2/{name}/tags/list?last={last_tag}&n={}>; rel=\"next\"",
                            n.unwrap()
                        ),
                    );
                }
            }
            builder.body(Body::from(body.to_string())).unwrap()
        }
        Ok(None) => name_unknown(),
        Err(e) => internal(e),
    }
}

pub async fn referrers(
    app: &AppRef,
    id: &Identity,
    name: &str,
    digest: &str,
    query: &HashMap<String, String>,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Pull) {
        return resp;
    }
    if !valid_digest(digest) {
        return digest_invalid("invalid digest format");
    }
    if app.object.is_some() {
        crate::truth::refresh_repo_soft(app, name).await;
    }
    use crate::schema::manifests as m;
    let (name2, digest2) = (name.to_string(), digest.to_string());
    let rows = db::run(&app.pool, move |conn| {
        let Some(rid) = db::repo_id(conn, &name2)? else {
            return Ok(vec![]);
        };
        Ok(m::table
            .filter(m::repo_id.eq(rid).and(m::subject_digest.eq(&digest2)))
            .select((
                m::digest,
                m::media_type,
                m::size,
                m::artifact_type,
                m::annotations,
            ))
            .load::<(String, String, i64, Option<String>, Option<String>)>(conn)?)
    })
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => return internal(e),
    };

    let filter = query.get("artifactType").cloned();
    let descriptors: Vec<serde_json::Value> = rows
        .into_iter()
        .filter(|(_, _, _, at, _)| match &filter {
            Some(f) => at.as_deref() == Some(f.as_str()),
            None => true,
        })
        .map(|(d, mt, size, at, ann)| {
            let mut desc = serde_json::json!({
                "mediaType": mt,
                "digest": d,
                "size": size,
            });
            if let Some(at) = at {
                desc["artifactType"] = serde_json::Value::String(at);
            }
            if let Some(ann) = ann {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&ann) {
                    desc["annotations"] = v;
                }
            }
            desc
        })
        .collect();

    let body = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX,
        "manifests": descriptors,
    });
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", OCI_INDEX);
    if filter.is_some() {
        builder = builder.header("oci-filters-applied", "artifactType");
    }
    builder.body(Body::from(body.to_string())).unwrap()
}
