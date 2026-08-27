mod api;
mod auth;
mod config;
mod db;
mod gc;
mod oci;
mod schema;
mod shard;
mod storage;
mod objectstore;
mod truth;

use axum::extract::DefaultBodyLimit;
use axum::routing::{any, get, post};
use axum::{middleware, Router};
use config::Config;
use std::sync::Arc;

pub struct App {
    pub pool: db::DbPool,
    pub store: storage::Store,
    pub cfg: Config,
    /// Present in object mode: the bucket that is the source of truth.
    pub object: Option<Arc<dyn objectstore::ObjectStore>>,
    /// Per-repo write serialization within this process (cross-process safety
    /// comes from CAS on the bucket, this just avoids needless conflicts).
    pub repo_locks: tokio::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

pub type AppRef = Arc<App>;

/// The admin dashboard (ui/dist, built with `bun run build`) compiled into the
/// binary — still a single file to deploy.
#[derive(rust_embed::Embed)]
#[folder = "ui/dist"]
struct UiAssets;

async fn ui_assets(uri: axum::http::Uri) -> axum::response::Response {
    use axum::body::Body;
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    let file = UiAssets::get(path).or_else(|| UiAssets::get("index.html"));
    match file {
        Some(f) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            axum::response::Response::builder()
                .header("content-type", mime.as_ref())
                .header(
                    "cache-control",
                    if path.starts_with("assets/") {
                        "public, max-age=31536000, immutable"
                    } else {
                        "no-cache"
                    },
                )
                .body(Body::from(f.data.into_owned()))
                .unwrap()
        }
        None => axum::response::Response::builder()
            .status(404)
            .body(Body::from("not found"))
            .unwrap(),
    }
}

/// Delete upload sessions (and their staging files) abandoned for more than a day.
async fn upload_sweeper(app: AppRef) {
    use crate::schema::uploads as u;
    use diesel::prelude::*;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        let cutoff = db::now() - 86_400;
        let stale = db::run(&app.pool, move |conn| {
            let stale: Vec<String> = u::table
                .filter(u::created_at.lt(cutoff))
                .select(u::uuid)
                .load(conn)?;
            diesel::delete(u::table.filter(u::created_at.lt(cutoff))).execute(conn)?;
            Ok(stale)
        })
        .await;
        match stale {
            Ok(uuids) => {
                for uuid in uuids {
                    app.store.delete_staging(&uuid).await;
                }
            }
            Err(e) => tracing::warn!("upload sweeper failed: {e}"),
        }
    }
}

fn hash_password(pw: &str) -> String {
    use argon2::password_hash::rand_core::OsRng;
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(pw.as_bytes(), &salt)
        .expect("argon2 hashing failed")
        .to_string()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    if let Some(cmd) = args.next() {
        match cmd.as_str() {
            "hash-password" => {
                let pw = args
                    .next()
                    .expect("usage: breezy-registry hash-password <password>");
                println!("{}", hash_password(&pw));
                return Ok(());
            }
            "serve" => {}
            other => {
                eprintln!("unknown command {other:?}; commands: serve (default), hash-password");
                std::process::exit(2);
            }
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "breezy_registry=info".into()),
        )
        .init();

    let cfg = Config::load();
    if let Some(sh) = &cfg.sharding {
        if !sh.shards.contains(&sh.self_url) {
            anyhow::bail!("sharding.self_url must be listed in sharding.shards");
        }
    }
    for u in &cfg.users {
        if !u.password.starts_with("$argon2") {
            tracing::warn!(
                "user {:?} has a plaintext password — use `breezy-registry hash-password` for production",
                u.username
            );
        }
    }

    let pool = db::init(&cfg.data_dir)?;
    let store = storage::Store::new(&cfg.data_dir)?;
    let object: Option<Arc<dyn objectstore::ObjectStore>> = match &cfg.object_storage {
        Some(os_cfg) => {
            if let Some(path) = &os_cfg.path {
                tracing::info!("object mode: filesystem bucket at {path}");
                Some(Arc::new(objectstore::FsObjectStore::new(path)?))
            } else if os_cfg.endpoint.is_some() || os_cfg.bucket.is_some() {
                tracing::info!(
                    "object mode: s3 bucket {:?} at {:?}",
                    os_cfg.bucket,
                    os_cfg.endpoint
                );
                Some(Arc::new(objectstore::S3Store::new(os_cfg)?))
            } else {
                anyhow::bail!("[object_storage] needs either `path` or `endpoint`/`bucket`");
            }
        }
        None => None,
    };
    let app: AppRef = Arc::new(App {
        pool,
        store,
        cfg,
        object,
        repo_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
    });

    if app.object.is_some() {
        let t0 = std::time::Instant::now();
        let repos = truth::rebuild_all(&app).await?;
        tracing::info!(
            "cache synced from object storage: {repos} repos in {:?}",
            t0.elapsed()
        );
    }

    tokio::spawn(upload_sweeper(app.clone()));

    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v2", any(oci::handle))
        .route("/v2/", any(oci::handle))
        .route("/v2/{*rest}", any(oci::handle))
        .route("/api/v1/whoami", get(api::whoami))
        .route("/api/v1/repos", get(api::repos))
        .route("/api/v1/tags", get(api::tags))
        .route("/api/v1/gc", post(api::gc_run))
        .route("/api/v1/stats", get(api::stats));
    let router = if app.cfg.ui {
        router.fallback(get(ui_assets))
    } else {
        router
    };
    let router = router
        .layer(middleware::from_fn_with_state(app.clone(), auth::middleware))
        .layer(DefaultBodyLimit::disable())
        .with_state(app.clone());

    let listener = tokio::net::TcpListener::bind(&app.cfg.listen).await?;
    tracing::info!("breezy-registry listening on {}", app.cfg.listen);
    axum::serve(listener, router).await?;
    Ok(())
}
