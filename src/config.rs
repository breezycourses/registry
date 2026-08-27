use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Allow anonymous pulls. Push always requires a credential when users are configured.
    #[serde(default = "default_true")]
    pub public_pull: bool,
    /// No users configured => open mode (everyone is admin). Meant for local dev only.
    #[serde(default)]
    pub users: Vec<UserCfg>,
    /// GC never deletes rows younger than this, so it can't race an in-flight push.
    #[serde(default = "default_grace")]
    pub gc_grace_seconds: i64,
    /// Serve the embedded admin dashboard at `/`. Off = headless registry
    /// (the /v2 and /api surfaces are unaffected).
    #[serde(default = "default_true")]
    pub ui: bool,
    #[serde(default)]
    pub sharding: Option<Sharding>,
    /// When set, object storage is the source of truth (blobs, manifests, and a
    /// CAS'd per-repo index live in the bucket); SQLite and the local blob dir
    /// become rebuildable caches.
    #[serde(default)]
    pub object_storage: Option<ObjectStorageCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObjectStorageCfg {
    /// Local directory acting as the bucket (dev / single node). Mutually
    /// exclusive with `endpoint`.
    #[serde(default)]
    pub path: Option<String>,
    /// S3-compatible endpoint (AWS, MinIO, R2, ...).
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    /// Falls back to AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserCfg {
    pub username: String,
    /// Either a plaintext password (dev) or an argon2 hash from `breezy-registry hash-password`.
    pub password: String,
    #[serde(default = "default_role")]
    pub role: String, // pull | push | admin
}

#[derive(Debug, Clone, Deserialize)]
pub struct Sharding {
    /// This instance's public base URL, must be listed in `shards`.
    pub self_url: String,
    pub shards: Vec<String>,
}

fn default_listen() -> String {
    "0.0.0.0:5100".into()
}
fn default_data_dir() -> String {
    "./data".into()
}
fn default_true() -> bool {
    true
}
fn default_grace() -> i64 {
    3600
}
fn default_role() -> String {
    "push".into()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: default_listen(),
            data_dir: default_data_dir(),
            public_pull: true,
            users: vec![],
            gc_grace_seconds: default_grace(),
            ui: true,
            sharding: None,
            object_storage: None,
        }
    }
}

impl Config {
    pub fn load() -> Config {
        let path = std::env::var("BREEZY_CONFIG").unwrap_or_else(|_| "breezy.toml".into());
        let cfg = match std::fs::read_to_string(&path) {
            Ok(text) => match toml::from_str(&text) {
                Ok(cfg) => {
                    tracing::info!("loaded config from {path}");
                    cfg
                }
                Err(e) => {
                    eprintln!("invalid config {path}: {e}");
                    std::process::exit(1);
                }
            },
            Err(_) => {
                tracing::warn!("no config file at {path}, using defaults (open mode)");
                Config::default()
            }
        };
        cfg.apply_env()
    }

    /// Environment overrides, so containers can adjust a mounted (read-only,
    /// possibly shared) config file. BREEZY_SELF_URL is how each pod of a
    /// sharded StatefulSet learns its own identity from a common config.
    fn apply_env(mut self) -> Config {
        if let Ok(v) = std::env::var("BREEZY_LISTEN") {
            self.listen = v;
        }
        if let Ok(v) = std::env::var("BREEZY_DATA_DIR") {
            self.data_dir = v;
        }
        if let Ok(v) = std::env::var("BREEZY_PUBLIC_PULL") {
            self.public_pull = v == "true" || v == "1";
        }
        if let Ok(v) = std::env::var("BREEZY_UI") {
            self.ui = v == "true" || v == "1";
        }
        if let Ok(v) = std::env::var("BREEZY_SELF_URL") {
            if let Some(sh) = self.sharding.as_mut() {
                sh.self_url = v;
            }
        }
        self
    }
}
