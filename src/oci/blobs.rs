use super::errors::*;
use super::valid_digest;
use crate::auth::{authorize, Action, Identity};
use crate::db;
use crate::AppRef;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use diesel::prelude::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

fn parse_range(headers: &HeaderMap, size: i64) -> Option<(i64, i64)> {
    let raw = headers.get("range")?.to_str().ok()?;
    let spec = raw.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    let start: i64 = a.parse().ok()?;
    let end: i64 = if b.is_empty() {
        size - 1
    } else {
        b.parse().ok()?
    };
    if start < 0 || start > end || end >= size {
        return None;
    }
    Some((start, end))
}

pub async fn get(
    app: &AppRef,
    id: &Identity,
    _name: &str,
    digest: &str,
    head: bool,
    headers: &HeaderMap,
) -> Response {
    if let Err(resp) = authorize(app, id, Action::Pull) {
        return resp;
    }
    if !valid_digest(digest) {
        return digest_invalid("invalid digest format");
    }
    // Read-through: serve from local disk, filling the cache from the bucket
    // on miss (object mode). Blobs are immutable, so no freshness check needed.
    let size = match crate::truth::ensure_blob_local(app, digest).await {
        Ok(Some(s)) => s,
        Ok(None) => return blob_unknown(),
        Err(e) => return internal(e),
    };

    if head {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-length", size)
            .header("content-type", "application/octet-stream")
            .header("docker-content-digest", digest)
            .header("accept-ranges", "bytes")
            .body(Body::empty())
            .unwrap();
    }

    let mut file = match app.store.open(digest).await {
        Ok(f) => f,
        Err(e) => return internal(e),
    };

    if let Some((start, end)) = parse_range(headers, size) {
        if let Err(e) = file.seek(std::io::SeekFrom::Start(start as u64)).await {
            return internal(e);
        }
        let len = end - start + 1;
        let stream = ReaderStream::new(file.take(len as u64));
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-length", len)
            .header("content-range", format!("bytes {start}-{end}/{size}"))
            .header("content-type", "application/octet-stream")
            .header("docker-content-digest", digest)
            .body(Body::from_stream(stream))
            .unwrap();
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("content-length", size)
        .header("content-type", "application/octet-stream")
        .header("docker-content-digest", digest)
        .header("accept-ranges", "bytes")
        .body(Body::from_stream(ReaderStream::new(file)))
        .unwrap()
}

pub async fn delete(app: &AppRef, id: &Identity, _name: &str, digest: &str) -> Response {
    if let Err(resp) = authorize(app, id, Action::Admin) {
        return resp;
    }
    if !valid_digest(digest) {
        return digest_invalid("invalid digest format");
    }
    use crate::schema::{blobs as b, manifest_refs as r};
    let digest2 = digest.to_string();
    let result = db::run_write(&app.pool, move |conn| {
        let referenced: i64 = r::table
            .filter(r::child_digest.eq(&digest2).and(r::kind.eq("blob")))
            .count()
            .get_result(conn)?;
        if referenced > 0 {
            return Ok(Err("referenced"));
        }
        let deleted =
            diesel::delete(b::table.filter(b::digest.eq(&digest2))).execute(conn)?;
        Ok(Ok(deleted > 0))
    })
    .await;
    match result {
        Ok(Err(_)) => oci_error(
            StatusCode::CONFLICT,
            "DENIED",
            "blob is referenced by one or more manifests",
        ),
        Ok(Ok(false)) => blob_unknown(),
        Ok(Ok(true)) => {
            if let Err(e) = app.store.delete(digest).await {
                return internal(e);
            }
            if let Some(os) = &app.object {
                if let Err(e) = os.delete(&crate::truth::blob_key(digest)).await {
                    return internal(e);
                }
            }
            Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => internal(e),
    }
}
