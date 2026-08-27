use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

/// Spec-shaped error body: {"errors":[{"code","message","detail"}]}
pub fn oci_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = serde_json::json!({
        "errors": [{ "code": code, "message": message, "detail": null }]
    });
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

pub fn internal(e: impl std::fmt::Display) -> Response {
    tracing::error!("internal error: {e}");
    oci_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "UNKNOWN",
        "internal error",
    )
}

pub fn name_unknown() -> Response {
    oci_error(
        StatusCode::NOT_FOUND,
        "NAME_UNKNOWN",
        "repository name not known to registry",
    )
}

pub fn blob_unknown() -> Response {
    oci_error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN", "blob unknown to registry")
}

pub fn manifest_unknown() -> Response {
    oci_error(
        StatusCode::NOT_FOUND,
        "MANIFEST_UNKNOWN",
        "manifest unknown to registry",
    )
}

pub fn digest_invalid(msg: &str) -> Response {
    oci_error(StatusCode::BAD_REQUEST, "DIGEST_INVALID", msg)
}

pub fn upload_unknown() -> Response {
    oci_error(
        StatusCode::NOT_FOUND,
        "BLOB_UPLOAD_UNKNOWN",
        "blob upload unknown to registry",
    )
}
