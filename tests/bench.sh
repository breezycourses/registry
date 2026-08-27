#!/usr/bin/env bash
# Latency benchmark for one running breezy-registry.
# Usage: REG=localhost:5100 LABEL=local N=50 ./tests/bench.sh
set -uo pipefail

REG="${REG:-localhost:5100}"
LABEL="${LABEL:-mode}"
N="${N:-50}"
REPO="bench/app"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

t() { curl -s -o /dev/null -w '%{time_total}' "$@"; }

stats() { # reads seconds on stdin, prints "p50 p95 min" in ms
  python3 -c '
import sys
xs = sorted(float(l) * 1000 for l in sys.stdin if l.strip())
if not xs: print("- - -"); exit()
p = lambda q: xs[min(len(xs)-1, int(q*len(xs)))]
print(f"{p(0.5):.1f} {p(0.95):.1f} {xs[0]:.1f}")'
}

row() { printf "%-10s %-26s %8s %8s %8s\n" "$LABEL" "$1" $2; }

# --- seed: one config blob + one 1MiB layer, reused everywhere
echo -n '{"os":"linux"}' > "$TMP/config.json"
CONFIG_DIGEST="sha256:$(shasum -a 256 "$TMP/config.json" | awk '{print $1}')"
curl -s -o /dev/null -X POST --data-binary @"$TMP/config.json" \
  "http://$REG/v2/$REPO/blobs/uploads/?digest=$CONFIG_DIGEST"
head -c 1048576 /dev/urandom > "$TMP/layer.bin"
LAYER_DIGEST="sha256:$(shasum -a 256 "$TMP/layer.bin" | awk '{print $1}')"
curl -s -o /dev/null -X POST --data-binary @"$TMP/layer.bin" \
  "http://$REG/v2/$REPO/blobs/uploads/?digest=$LAYER_DIGEST"

manifest() { # unique manifest per $1
  cat <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"$CONFIG_DIGEST","size":13},
"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"$LAYER_DIGEST","size":1048576}],
"annotations":{"bench":"$1"}}
EOF
}
manifest seed > "$TMP/m-seed.json"
curl -s -o /dev/null -X PUT -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  --data-binary @"$TMP/m-seed.json" "http://$REG/v2/$REPO/manifests/seed"

printf "%-10s %-26s %8s %8s %8s\n" "mode" "operation" "p50(ms)" "p95(ms)" "min(ms)"

# 1. HTTP baseline
for i in $(seq $N); do t "http://$REG/v2/"; echo; done > "$TMP/base"
row "GET /v2/ (baseline)" "$(stats < "$TMP/base")"

# 2. manifest GET by tag (warm — object modes pay one freshness check here)
for i in $(seq $N); do t "http://$REG/v2/$REPO/manifests/seed"; echo; done > "$TMP/mget"
row "manifest GET (tag, warm)" "$(stats < "$TMP/mget")"

# 3. manifest PUT (tag move; the CAS-linearized write in object modes)
for i in $(seq $N); do
  manifest "iter-$i" > "$TMP/m.json"
  t -X PUT -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
    --data-binary @"$TMP/m.json" "http://$REG/v2/$REPO/manifests/bench-$i"
  echo
done > "$TMP/mput"
row "manifest PUT (new tag)" "$(stats < "$TMP/mput")"

# 4. blob PUT 1MiB monolithic (unique content each time)
for i in $(seq $N); do
  head -c 1048576 /dev/urandom > "$TMP/b.bin"
  D="sha256:$(shasum -a 256 "$TMP/b.bin" | awk '{print $1}')"
  t -X POST --data-binary @"$TMP/b.bin" "http://$REG/v2/$REPO/blobs/uploads/?digest=$D"
  echo
done > "$TMP/bput"
row "blob PUT 1MiB" "$(stats < "$TMP/bput")"

# 5. blob GET 1MiB (warm, local disk)
for i in $(seq $N); do t "http://$REG/v2/$REPO/blobs/$LAYER_DIGEST"; echo; done > "$TMP/bget"
row "blob GET 1MiB (warm)" "$(stats < "$TMP/bget")"

# 6. tags list
for i in $(seq $N); do t "http://$REG/v2/$REPO/tags/list?n=10"; echo; done > "$TMP/tags"
row "tags list" "$(stats < "$TMP/tags")"
