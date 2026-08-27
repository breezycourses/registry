pub mod blobs;
pub mod errors;
pub mod manifests;
pub mod uploads;

use crate::auth::{authorize, identity_of, Action};
use crate::AppRef;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use errors::oci_error;
use std::collections::HashMap;

/// OCI paths have the repository name (which itself contains slashes) in the middle,
/// so we parse instead of using the router: find the *rightmost* known marker.
/// The segment after each marker never contains '/', which makes rfind unambiguous
/// even for repos named e.g. "team/blobs".
#[derive(Debug, PartialEq)]
enum Route {
    Base,
    Catalog,
    UploadStart(String),
    Upload(String, String),
    Blob(String, String),
    Manifest(String, String),
    Tags(String),
    Referrers(String, String),
}

fn parse_route(rest: &str) -> Option<Route> {
    if rest.is_empty() {
        return Some(Route::Base);
    }
    if rest == "_catalog" {
        return Some(Route::Catalog);
    }
    let mut best: Option<(usize, u8)> = None; // (index, marker id); longer marker wins ties
    let markers: [(&str, u8); 5] = [
        ("/blobs/uploads/", 0),
        ("/blobs/", 1),
        ("/manifests/", 2),
        ("/referrers/", 3),
        ("/tags/list", 4),
    ];
    for (m, id) in markers {
        let idx = if id == 4 {
            if rest.ends_with(m) { Some(rest.len() - m.len()) } else { None }
        } else {
            rest.rfind(m)
        };
        if let Some(i) = idx {
            // Prefer the rightmost marker; on the same index prefer "/blobs/uploads/"
            // over its prefix "/blobs/".
            if best.map_or(true, |(bi, bid)| i > bi || (i == bi && id < bid)) {
                best = Some((i, id));
            }
        }
    }
    let (i, id) = best?;
    let name = rest[..i].to_string();
    match id {
        0 => {
            let tail = &rest[i + "/blobs/uploads/".len()..];
            if tail.is_empty() {
                Some(Route::UploadStart(name))
            } else {
                Some(Route::Upload(name, tail.to_string()))
            }
        }
        1 => Some(Route::Blob(name, rest[i + "/blobs/".len()..].to_string())),
        2 => Some(Route::Manifest(name, rest[i + "/manifests/".len()..].to_string())),
        3 => Some(Route::Referrers(name, rest[i + "/referrers/".len()..].to_string())),
        4 => Some(Route::Tags(name)),
        _ => None,
    }
}

pub fn valid_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 255 {
        return false;
    }
    name.split('/').all(|comp| {
        !comp.is_empty()
            && comp.chars().next().unwrap().is_ascii_alphanumeric()
            && comp.chars().last().unwrap().is_ascii_alphanumeric()
            && comp
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c))
    })
}

pub fn valid_digest(d: &str) -> bool {
    match d.split_once(':') {
        Some(("sha256", hex)) => hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()),
        _ => false,
    }
}

pub fn valid_tag(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= 128
        && t.chars().next().unwrap().is_ascii_alphanumeric()
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
}

/// Percent-decode a query component. Some clients send `digest=sha256:...`
/// with a literal colon (docker), others encode it as `%3A` (crane and
/// anything else built on Go's url package) — both must parse identically.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn query_map(query: Option<&str>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(q) = query {
        for pair in q.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                map.insert(percent_decode(k), percent_decode(v));
            }
        }
    }
    map
}

pub async fn handle(State(app): State<AppRef>, req: Request) -> Response {
    let identity = identity_of(&req);
    let path = req.uri().path().to_string();
    let rest = path
        .strip_prefix("/v2")
        .map(|s| s.strip_prefix('/').unwrap_or(s))
        .unwrap_or("")
        .to_string();
    let query = query_map(req.uri().query());
    let method = req.method().clone();

    let Some(route) = parse_route(&rest) else {
        return errors::name_unknown();
    };

    // Sharding: redirect anything for a repo this instance doesn't own.
    if let Some(sh) = &app.cfg.sharding {
        let repo = match &route {
            Route::Base | Route::Catalog => None,
            Route::UploadStart(n)
            | Route::Upload(n, _)
            | Route::Blob(n, _)
            | Route::Manifest(n, _)
            | Route::Tags(n)
            | Route::Referrers(n, _) => Some(n.as_str()),
        };
        if let Some(repo) = repo {
            let owner = crate::shard::owner(repo, &sh.shards);
            if owner != sh.self_url {
                let target = format!(
                    "{}{}{}",
                    owner.trim_end_matches('/'),
                    path,
                    req.uri()
                        .query()
                        .map(|q| format!("?{q}"))
                        .unwrap_or_default()
                );
                return Response::builder()
                    .status(StatusCode::TEMPORARY_REDIRECT)
                    .header("location", target)
                    .body(Body::empty())
                    .unwrap();
            }
        }
    }

    // Validate names early.
    if let Route::UploadStart(n)
    | Route::Upload(n, _)
    | Route::Blob(n, _)
    | Route::Manifest(n, _)
    | Route::Tags(n)
    | Route::Referrers(n, _) = &route
    {
        if !valid_name(n) {
            return oci_error(
                StatusCode::BAD_REQUEST,
                "NAME_INVALID",
                "invalid repository name",
            );
        }
    }

    let headers = req.headers().clone();
    let body = req.into_body();

    match (route, method) {
        (Route::Base, Method::GET) | (Route::Base, Method::HEAD) => {
            if let Err(resp) = authorize(&app, &identity, Action::Pull) {
                return resp;
            }
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .header("docker-distribution-api-version", "registry/2.0")
                .body(Body::from("{}"))
                .unwrap()
        }
        (Route::Catalog, Method::GET) => {
            if let Err(resp) = authorize(&app, &identity, Action::Pull) {
                return resp;
            }
            use diesel::prelude::*;
            let result = crate::db::run(&app.pool, |conn| {
                use crate::schema::repos as rp;
                Ok(rp::table
                    .select(rp::name)
                    .order(rp::name.asc())
                    .load::<String>(conn)?)
            })
            .await;
            match result {
                Ok(repos) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "repositories": repos }).to_string(),
                    ))
                    .unwrap(),
                Err(e) => errors::internal(e),
            }
        }
        (Route::UploadStart(name), Method::POST) => {
            uploads::start(&app, &identity, &name, &query, body).await
        }
        (Route::Upload(name, uuid), Method::PATCH) => {
            uploads::patch(&app, &identity, &name, &uuid, &headers, body).await
        }
        (Route::Upload(name, uuid), Method::PUT) => {
            uploads::put(&app, &identity, &name, &uuid, &query, body).await
        }
        (Route::Upload(name, uuid), Method::GET) => {
            uploads::status(&app, &identity, &name, &uuid).await
        }
        (Route::Upload(name, uuid), Method::DELETE) => {
            uploads::cancel(&app, &identity, &name, &uuid).await
        }
        (Route::Blob(name, digest), Method::GET) => {
            blobs::get(&app, &identity, &name, &digest, false, &headers).await
        }
        (Route::Blob(name, digest), Method::HEAD) => {
            blobs::get(&app, &identity, &name, &digest, true, &headers).await
        }
        (Route::Blob(name, digest), Method::DELETE) => {
            blobs::delete(&app, &identity, &name, &digest).await
        }
        (Route::Manifest(name, reference), Method::GET) => {
            manifests::get(&app, &identity, &name, &reference, false).await
        }
        (Route::Manifest(name, reference), Method::HEAD) => {
            manifests::get(&app, &identity, &name, &reference, true).await
        }
        (Route::Manifest(name, reference), Method::PUT) => {
            manifests::put(&app, &identity, &name, &reference, &headers, body).await
        }
        (Route::Manifest(name, reference), Method::DELETE) => {
            manifests::delete(&app, &identity, &name, &reference).await
        }
        (Route::Tags(name), Method::GET) => {
            manifests::tags_list(&app, &identity, &name, &query).await
        }
        (Route::Referrers(name, digest), Method::GET) => {
            manifests::referrers(&app, &identity, &name, &digest, &query).await
        }
        _ => oci_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "UNSUPPORTED",
            "method not allowed",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_decodes_percent_encoding() {
        let m = query_map(Some("digest=sha256%3Aabc&mount=sha256:def"));
        assert_eq!(m["digest"], "sha256:abc");
        assert_eq!(m["mount"], "sha256:def");
    }

    #[test]
    fn routes_parse() {
        assert_eq!(parse_route(""), Some(Route::Base));
        assert_eq!(
            parse_route("team/app/blobs/uploads/"),
            Some(Route::UploadStart("team/app".into()))
        );
        assert_eq!(
            parse_route("team/app/blobs/uploads/abc-123"),
            Some(Route::Upload("team/app".into(), "abc-123".into()))
        );
        assert_eq!(
            parse_route("team/app/blobs/sha256:aa"),
            Some(Route::Blob("team/app".into(), "sha256:aa".into()))
        );
        assert_eq!(
            parse_route("team/app/manifests/v1"),
            Some(Route::Manifest("team/app".into(), "v1".into()))
        );
        assert_eq!(parse_route("a/tags/list"), Some(Route::Tags("a".into())));
        // Repos with marker-like components resolve to the rightmost marker.
        assert_eq!(
            parse_route("a/manifests/b/blobs/sha256:aa"),
            Some(Route::Blob("a/manifests/b".into(), "sha256:aa".into()))
        );
        assert_eq!(
            parse_route("a/blobs/b/manifests/latest"),
            Some(Route::Manifest("a/blobs/b".into(), "latest".into()))
        );
    }
}
