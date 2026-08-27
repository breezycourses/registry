use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, CustomizeConnection, Pool};
use std::path::Path;

pub type DbPool = Pool<ConnectionManager<SqliteConnection>>;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS repos (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL,
  index_etag TEXT,
  index_version INTEGER
);
CREATE TABLE IF NOT EXISTS blobs (
  digest TEXT PRIMARY KEY,
  size INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
  id INTEGER PRIMARY KEY,
  repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
  digest TEXT NOT NULL,
  media_type TEXT NOT NULL,
  payload BLOB NOT NULL,
  size INTEGER NOT NULL,
  subject_digest TEXT,
  artifact_type TEXT,
  annotations TEXT,
  created_at INTEGER NOT NULL,
  UNIQUE (repo_id, digest)
);
CREATE INDEX IF NOT EXISTS idx_manifests_subject ON manifests(repo_id, subject_digest);
CREATE TABLE IF NOT EXISTS manifest_refs (
  manifest_id INTEGER NOT NULL REFERENCES manifests(id) ON DELETE CASCADE,
  child_digest TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('blob','manifest')),
  PRIMARY KEY (manifest_id, child_digest, kind)
);
CREATE INDEX IF NOT EXISTS idx_refs_child ON manifest_refs(child_digest);
CREATE TABLE IF NOT EXISTS tags (
  repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  manifest_id INTEGER NOT NULL REFERENCES manifests(id),
  pushed_at INTEGER NOT NULL,
  PRIMARY KEY (repo_id, name)
);
CREATE TABLE IF NOT EXISTS uploads (
  uuid TEXT PRIMARY KEY,
  repo_id INTEGER NOT NULL REFERENCES repos(id),
  bytes_received INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
"#;

#[derive(Debug)]
struct Pragmas;

impl CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for Pragmas {
    fn on_acquire(&self, conn: &mut SqliteConnection) -> Result<(), diesel::r2d2::Error> {
        conn.batch_execute(
            "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000; \
             PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL;",
        )
        .map_err(diesel::r2d2::Error::QueryError)
    }
}

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

pub fn init(data_dir: &str) -> anyhow::Result<DbPool> {
    std::fs::create_dir_all(data_dir)?;
    let db_path = Path::new(data_dir).join("breezy.db");
    let manager = ConnectionManager::<SqliteConnection>::new(db_path.to_string_lossy().to_string());
    let pool = Pool::builder()
        .max_size(16)
        .connection_customizer(Box::new(Pragmas))
        .build(manager)?;
    let mut conn = pool.get()?;
    conn.batch_execute(SCHEMA)?;
    // Naive column migrations for pre-existing dev databases; duplicate-column
    // errors mean the column is already there.
    for stmt in [
        "ALTER TABLE repos ADD COLUMN index_etag TEXT",
        "ALTER TABLE repos ADD COLUMN index_version INTEGER",
    ] {
        let _ = conn.batch_execute(stmt);
    }
    Ok(pool)
}

/// SQLite is synchronous; every query runs on the blocking thread pool.
pub async fn run<T, F>(pool: &DbPool, f: F) -> anyhow::Result<T>
where
    F: FnOnce(&mut SqliteConnection) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let pool = pool.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get()?;
        f(&mut conn)
    })
    .await?
}

pub fn get_or_create_repo(conn: &mut SqliteConnection, repo_name: &str) -> anyhow::Result<i64> {
    use crate::schema::repos as r;
    diesel::insert_into(r::table)
        .values((r::name.eq(repo_name), r::created_at.eq(now())))
        .on_conflict(r::name)
        .do_nothing()
        .execute(conn)?;
    Ok(r::table
        .filter(r::name.eq(repo_name))
        .select(r::id)
        .first::<i64>(conn)?)
}

pub fn repo_id(conn: &mut SqliteConnection, repo_name: &str) -> anyhow::Result<Option<i64>> {
    use crate::schema::repos as r;
    Ok(r::table
        .filter(r::name.eq(repo_name))
        .select(r::id)
        .first::<i64>(conn)
        .optional()?)
}
