use crate::oci::errors::oci_error;
use crate::AppRef;
use argon2::password_hash::PasswordHash;
use argon2::{Argon2, PasswordVerifier};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Role {
    Anonymous,
    Pull,
    Push,
    Admin,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    Pull,
    Push,
    Admin,
}

#[derive(Clone, Debug)]
pub struct Identity {
    pub username: Option<String>,
    pub role: Role,
}

fn role_from_str(s: &str) -> Role {
    match s {
        "admin" => Role::Admin,
        "pull" => Role::Pull,
        _ => Role::Push,
    }
}

fn password_ok(stored: &str, given: &str) -> bool {
    if stored.starts_with("$argon2") {
        match PasswordHash::new(stored) {
            Ok(hash) => Argon2::default()
                .verify_password(given.as_bytes(), &hash)
                .is_ok(),
            Err(_) => false,
        }
    } else {
        // Plaintext passwords are supported for dev setups only.
        stored.as_bytes() == given.as_bytes()
    }
}

pub fn challenge() -> Response {
    let mut resp = oci_error(
        StatusCode::UNAUTHORIZED,
        "UNAUTHORIZED",
        "authentication required",
    );
    resp.headers_mut().insert(
        "WWW-Authenticate",
        "Basic realm=\"breezy-registry\"".parse().unwrap(),
    );
    resp
}

/// Resolves the request's credentials into an Identity. Rejects only *invalid* credentials;
/// missing credentials become Anonymous and each handler decides via `authorize`.
pub async fn middleware(State(app): State<AppRef>, mut req: Request, next: Next) -> Response {
    // No users configured => open mode, everything is allowed.
    if app.cfg.users.is_empty() {
        req.extensions_mut().insert(Identity {
            username: Some("anonymous-admin".into()),
            role: Role::Admin,
        });
        return next.run(req).await;
    }

    let header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let identity = match header {
        None => Identity {
            username: None,
            role: Role::Anonymous,
        },
        Some(h) => {
            let Some(b64) = h.strip_prefix("Basic ") else {
                return challenge();
            };
            let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) else {
                return challenge();
            };
            let Ok(creds) = String::from_utf8(decoded) else {
                return challenge();
            };
            let Some((user, pass)) = creds.split_once(':') else {
                return challenge();
            };
            match app
                .cfg
                .users
                .iter()
                .find(|u| u.username == user && password_ok(&u.password, pass))
            {
                Some(u) => Identity {
                    username: Some(u.username.clone()),
                    role: role_from_str(&u.role),
                },
                None => return challenge(),
            }
        }
    };

    req.extensions_mut().insert(identity);
    next.run(req).await
}

/// The single authorization decision point.
pub fn authorize(app: &AppRef, id: &Identity, action: Action) -> Result<(), Response> {
    let allowed = match action {
        Action::Pull => app.cfg.public_pull || id.role >= Role::Pull,
        Action::Push => id.role >= Role::Push,
        Action::Admin => id.role >= Role::Admin,
    };
    if allowed {
        Ok(())
    } else if id.username.is_none() {
        Err(challenge())
    } else {
        Err(oci_error(
            StatusCode::FORBIDDEN,
            "DENIED",
            "insufficient permissions",
        ))
    }
}

pub fn identity_of(req: &Request<Body>) -> Identity {
    req.extensions()
        .get::<Identity>()
        .cloned()
        .unwrap_or(Identity {
            username: None,
            role: Role::Anonymous,
        })
}
