<p align="center">
  <img src="assets/banner.png" alt="Breezy Registry — a single-binary OCI container registry" width="100%">
</p>

# Breezy Registry

A single-binary OCI container registry in Rust. The reliability profile of a
minimal registry, with the parts of Harbor worth keeping — a web UI, access
control, retention-friendly GC — and none of its ~10-service architecture.

```bash
cargo run                      # open mode on :5100, data in ./data
docker push localhost:5100/team/app:v1
open http://localhost:5100     # web UI
```

## Two modes

**Local mode** (default): SQLite is the source of truth, blobs on local disk.
Zero dependencies, perfect for a single node.

**Object mode** (`[object_storage]` in config): an S3-compatible bucket (AWS,
MinIO, R2, or a local directory) is the source of truth — the architecture
Cursor's Continuity uses for git, applied to a registry. Blobs and manifest
bytes are immutable objects; the only mutable state per repo is one small
`repos/<name>/index.json` updated via **compare-and-swap on its ETag** (the
registry's equivalent of a git ref update). That makes writes per-repo
linearizable with no consensus and no database server. SQLite and the local
blob dir become rebuildable caches: replicas validate reads with one
conditional GET (`NotModified` = fast path), read-through-cache blobs on miss,
and a pod that loses its disk rebuilds from the bucket in milliseconds. Every
accepted write also appends `repos/<name>/log/<version>.json` — full provenance
of who pushed/deleted what, when.

```toml
[object_storage]
endpoint = "http://minio:9000"   # or omit endpoint for AWS S3
bucket = "breezy"
region = "us-east-1"
access_key = "..."               # or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env
secret_key = "..."
# or, for a single node / testing: path = "/var/lib/breezy-bucket"
```

Caveats: MinIO in single-disk mode doesn't support atomic `If-None-Match: *`
creates, so the very first write of a brand-new repo falls back to head+put
(AWS S3 and R2 create atomically); all subsequent updates are true CAS
everywhere. GC in object mode re-syncs all indexes first and uses guarded
deletes, but prefer running it in quiet periods.

## Design

Everything follows from four abstractions:

1. **The blob store** (`storage.rs`) — content-addressed files keyed by digest,
   deduplicated across all repositories. Six methods; an S3 backend slots in later.
2. **The metadata DB** (`schema.rs`) — SQLite (WAL) is the single source of truth.
   Manifests live here as raw bytes (digests are over bytes, never re-serialized);
   blob reachability is computable entirely in SQL, so GC never guesses.
3. **`allowed(identity, action)`** (`auth.rs`) — every authorization decision
   passes through one function. Basic auth directly on `/v2` (Docker clients
   support it natively), roles: `pull` < `push` < `admin`.
4. **`shard_owner(repo)`** (`shard.rs`) — rendezvous hashing over a static shard
   list. Repos never share metadata, so shards need zero coordination: each shard
   is a complete, independent registry that 307-redirects requests it doesn't own.

One process, three API surfaces:

- `/v2/*` — OCI Distribution Spec (pull, push, chunked + monolithic uploads,
  cross-repo mounts, multi-arch indexes, tag pagination, referrers API — so
  cosign signatures and SBOMs work with no extra code).
- `/api/v1/*` — JSON for the UI: repos, tags with sizes, whoami, GC.
- `/` — the embedded admin dashboard (Vite + React, sharing breezy-website's design system), compiled into the binary via rust-embed. Rebuild with `cd ui && bun install && bun run build`; `ui/dist` is committed so cargo-only builds work.

## Configuration

`breezy.toml` (or `BREEZY_CONFIG=/path/to/file`); every field is optional:

```toml
listen = "0.0.0.0:5100"
data_dir = "./data"
public_pull = true        # anonymous pulls allowed
gc_grace_seconds = 3600   # GC never touches anything younger than this

[[users]]
username = "admin"
password = "$argon2id$..."   # from: breezy-registry hash-password <pw>
role = "admin"               # pull | push | admin

[[users]]
username = "ci"
password = "$argon2id$..."
role = "push"

# Optional: static shard list. Deploy the same binary N times, same config
# apart from self_url. Clients that follow redirects (docker, etc.) can talk
# to any shard; or route at the ingress with the same rendezvous hash.
#[sharding]
#self_url = "http://registry-0.internal"
#shards = ["http://registry-0.internal", "http://registry-1.internal"]
```

With no users configured the registry runs in **open mode** (everything allowed) —
for local development only. Plaintext passwords work but log a warning; use
`breezy-registry hash-password` for real deployments. TLS is the ingress's job.

## Garbage collection

`POST /api/v1/gc?dry_run=1` (admin), or the buttons in the UI. Mark & sweep over
the DB: a manifest survives if it is tagged, referenced by a surviving index, or
is a referrer (e.g. a signature) of a surviving manifest. Blobs survive while any
surviving manifest references them. Nothing younger than `gc_grace_seconds` is
ever deleted, which makes GC safe to run while pushes are in flight.

Deleting: `DELETE /v2/<repo>/manifests/<tag>` untags; `DELETE .../manifests/<digest>`
removes the manifest; GC then reclaims unreferenced blobs.

## Deployment

**Docker** — the [Dockerfile](Dockerfile) builds a static musl binary into a
`scratch` image (runs as uid 65532, data in `/data`):

```bash
docker build -t breezy-registry .
docker run -p 5100:5100 -v breezy-data:/data breezy-registry
```

**Helm** — the chart lives in [charts/breezy-registry](charts/breezy-registry):

```bash
helm install registry ./charts/breezy-registry \
  --set ingress.enabled=true \
  --set ingress.host=registry.example.com \
  --set 'users[0].username=admin' \
  --set 'users[0].password=<argon2 hash>' \
  --set 'users[0].role=admin'
```

It deploys a StatefulSet (PVC per pod for SQLite + blobs), a Service, an
optional Ingress (with `proxy-body-size: 0` for nginx — registries stream big
blobs), and renders `breezy.toml` into a Secret (or bring your own via
`existingConfigSecret`). Probes hit `/healthz`. TLS terminates at the ingress.

Environment overrides for containers: `BREEZY_CONFIG`, `BREEZY_LISTEN`,
`BREEZY_DATA_DIR`, `BREEZY_PUBLIC_PULL`, `BREEZY_SELF_URL`.

**Sharded mode**: `--set sharding.enabled=true --set replicaCount=3` runs N
independent shards; each pod learns its identity from `BREEZY_SELF_URL` (pod DNS
via the headless service) and 307-redirects repos it doesn't own. Redirect
targets are in-cluster DNS names, so this suits in-cluster clients; for external
traffic keep one shard or route at the ingress. Changing the shard count
reassigns ~1/N of repositories — drain/copy before shrinking.

## Testing

```bash
cargo test          # unit tests (routing, sharding)
cargo build
./target/debug/breezy-registry &
./tests/e2e.sh      # 35-check OCI flow: uploads, manifests, referrers, GC, errors
```

`tests/e2e.sh` honors `REG=host:port` and `AUTH="-u user:pass"`.

## Not yet built (deliberately)

OIDC SSO + CLI secrets, retention policy rules, project quotas, per-project ACLs
(roles are currently registry-wide), vulnerability scanning, a UI view over the
provenance log. Postgres is deliberately off the roadmap — object mode plus
disposable replicas is the scaling path. The schema and the four abstractions
above were chosen so each of these is an addition, not a rework.
