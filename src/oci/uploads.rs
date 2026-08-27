use super::errors::*;
use super::{valid_digest, valid_name};
use crate::auth::{authorize, Action, Identity};
use crate::db;
use crate::AppRef;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use diesel::prelude::*;
use futures_util::StreamExt;
use sha2::Digest;
use std::collections::HashMap;
use std::path::Path;
use tokio::io::AsyncWriteExt;

async fn append_body(path: &Path, body: Body) -> anyhow::Result<u64> {
    let mut f = tokio::fs::OpenOptions::new().append(true).open(path).await?;
    let mut stream = body.into_data_stream();
    let mut n: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let c = chunk?;
        f.write_all(&c).await?;
        n += c.len() as u64;
    }
    f.flush().await?;
    Ok(n)
}

async fn sha256_of_file(path: std::path::PathBuf) -> anyhow::Result<String> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut f = std::fs::File::open(&path)?;
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
    })
    .await?
}

fn accepted(name: &str, uuid: &str, received: i64) -> Response {
    let end = if received > 0 { received - 1 } else { 0 };
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header("location", format!("/v2/{name}/blobs/uploads/{uuid}"))
        .header("range", format!("0-{end}"))
        .header("docker-upload-uuid", uuid)
        .body(Body::empty())
        .unwrap()
}

async fn session(app: &AppRef, uuid: &str) -> anyhow::Result<Option<i64>> {
    use crate::schema::uploads as u;
    let uuid = uuid.to_string();
    db::run(&app.pool, move |conn| {
        Ok(u::table
            .filter(u::uuid.eq(&uuid))
            .select(u::bytes_received)
            .first::<i64>(conn)
            .optional()?)
    })
    .await
}

pub async fn start(
    app: &AppRef,
    id: &Identity,
    name: &str,
    query: &HashMap<String, String>,
    body: Body,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Push) {
        return resp;
    }

    // Cross-repo mount: blobs are globally content-addressed, so a mount is just
    // "does the blob exist" — the `from` repo doesn't matter.
    if let Some(mount) = query.get("mount") {
        if valid_digest(mount) {
            // Blobs are global CAS: cached locally or present in the bucket
            // (possibly pushed via another replica) both count.
            let exists = crate::truth::blob_exists(app, mount).await;
            match exists {
                Ok(true) => {
                    return Response::builder()
                        .status(StatusCode::CREATED)
                        .header("location", format!("/v2/{name}/blobs/{mount}"))
                        .header("docker-content-digest", mount.as_str())
                        .body(Body::empty())
                        .unwrap();
                }
                Ok(false) => {} // fall through to a normal upload session (202)
                Err(e) => return internal(e),
            }
        }
    }

    let uuid = uuid::Uuid::new_v4().to_string();
    {
        use crate::schema::uploads as u;
        let (uuid2, name2) = (uuid.clone(), name.to_string());
        let created = db::run(&app.pool, move |conn| {
            let rid = db::get_or_create_repo(conn, &name2)?;
            diesel::insert_into(u::table)
                .values((
                    u::uuid.eq(&uuid2),
                    u::repo_id.eq(rid),
                    u::bytes_received.eq(0),
                    u::created_at.eq(db::now()),
                ))
                .execute(conn)?;
            Ok(())
        })
        .await;
        if let Err(e) = created {
            return internal(e);
        }
    }
    if let Err(e) = app.store.create_staging(&uuid).await {
        return internal(e);
    }

    // Single-request monolithic push: POST with ?digest= and the body.
    if let Some(digest) = query.get("digest") {
        let staging = app.store.staging_path(&uuid);
        match append_body(&staging, body).await {
            Ok(_) => finalize(app, name, &uuid, digest).await,
            Err(e) => {
                cleanup(app, &uuid).await;
                internal(e)
            }
        }
    } else {
        accepted(name, &uuid, 0)
    }
}

pub async fn patch(
    app: &AppRef,
    id: &Identity,
    name: &str,
    uuid: &str,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Push) {
        return resp;
    }
    let received = match session(app, uuid).await {
        Ok(Some(n)) => n,
        Ok(None) => return upload_unknown(),
        Err(e) => return internal(e),
    };

    // If the client declares a Content-Range, it must continue exactly where we are.
    if let Some(cr) = headers.get("content-range").and_then(|v| v.to_str().ok()) {
        let start = cr.split('-').next().and_then(|s| s.parse::<i64>().ok());
        if start != Some(received) {
            let mut resp = oci_error(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "BLOB_UPLOAD_INVALID",
                "content range does not match upload state",
            );
            let end = if received > 0 { received - 1 } else { 0 };
            resp.headers_mut()
                .insert("range", format!("0-{end}").parse().unwrap());
            return resp;
        }
    }

    let staging = app.store.staging_path(uuid);
    let added = match append_body(&staging, body).await {
        Ok(n) => n as i64,
        Err(e) => return internal(e),
    };
    let total = received + added;
    {
        use crate::schema::uploads as u;
        let uuid2 = uuid.to_string();
        let updated = db::run(&app.pool, move |conn| {
            diesel::update(u::table.filter(u::uuid.eq(&uuid2)))
                .set(u::bytes_received.eq(total))
                .execute(conn)?;
            Ok(())
        })
        .await;
        if let Err(e) = updated {
            return internal(e);
        }
    }
    accepted(name, uuid, total)
}

pub async fn put(
    app: &AppRef,
    id: &Identity,
    name: &str,
    uuid: &str,
    query: &HashMap<String, String>,
    body: Body,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Push) {
        return resp;
    }
    let Some(digest) = query.get("digest") else {
        return digest_invalid("digest query parameter required");
    };
    match session(app, uuid).await {
        Ok(Some(_)) => {}
        Ok(None) => return upload_unknown(),
        Err(e) => return internal(e),
    }
    let staging = app.store.staging_path(uuid);
    if let Err(e) = append_body(&staging, body).await {
        cleanup(app, uuid).await;
        return internal(e);
    }
    finalize(app, name, uuid, digest).await
}

pub async fn status(app: &AppRef, id: &Identity, name: &str, uuid: &str) -> Response {
    if let Err(resp) = authorize(app, id, Action::Push) {
        return resp;
    }
    match session(app, uuid).await {
        Ok(Some(received)) => {
            let mut resp = accepted(name, uuid, received);
            *resp.status_mut() = StatusCode::NO_CONTENT;
            resp
        }
        Ok(None) => upload_unknown(),
        Err(e) => internal(e),
    }
}

pub async fn cancel(app: &AppRef, id: &Identity, _name: &str, uuid: &str) -> Response {
    if let Err(resp) = authorize(app, id, Action::Push) {
        return resp;
    }
    cleanup(app, uuid).await;
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

async fn cleanup(app: &AppRef, uuid: &str) {
    use crate::schema::uploads as u;
    let uuid2 = uuid.to_string();
    let _ = db::run(&app.pool, move |conn| {
        diesel::delete(u::table.filter(u::uuid.eq(&uuid2))).execute(conn)?;
        Ok(())
    })
    .await;
    app.store.delete_staging(uuid).await;
}

async fn finalize(app: &AppRef, name: &str, uuid: &str, digest: &str) -> Response {
    if !valid_digest(digest) || !valid_name(name) {
        cleanup(app, uuid).await;
        return digest_invalid("invalid digest format");
    }
    let actual = match sha256_of_file(app.store.staging_path(uuid)).await {
        Ok(d) => d,
        Err(e) => {
            cleanup(app, uuid).await;
            return internal(e);
        }
    };
    if actual != *digest {
        cleanup(app, uuid).await;
        return digest_invalid("digest does not match uploaded content");
    }
    let size = match app.store.commit(uuid, digest).await {
        Ok(s) => s as i64,
        Err(e) => {
            cleanup(app, uuid).await;
            return internal(e);
        }
    };
    // Object mode: the bucket copy is the durable one; don't acknowledge the
    // push until it's there (local disk is just cache).
    if let Some(os) = &app.object {
        let local = app.store.blob_path(digest);
        if let Err(e) = os.put_file(&crate::truth::blob_key(digest), &local).await {
            let _ = app.store.delete(digest).await;
            return internal(e);
        }
    }
    {
        use crate::schema::blobs as b;
        use crate::schema::uploads as u;
        let (digest2, uuid2) = (digest.to_string(), uuid.to_string());
        let done = db::run(&app.pool, move |conn| {
            diesel::insert_into(b::table)
                .values((
                    b::digest.eq(&digest2),
                    b::size.eq(size),
                    b::created_at.eq(db::now()),
                ))
                .on_conflict(b::digest)
                .do_nothing()
                .execute(conn)?;
            diesel::delete(u::table.filter(u::uuid.eq(&uuid2))).execute(conn)?;
            Ok(())
        })
        .await;
        if let Err(e) = done {
            return internal(e);
        }
    }
    Response::builder()
        .status(StatusCode::CREATED)
        .header("location", format!("/v2/{name}/blobs/{digest}"))
        .header("docker-content-digest", digest)
        .body(Body::empty())
        .unwrap()
}
