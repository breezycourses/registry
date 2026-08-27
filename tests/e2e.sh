#!/usr/bin/env bash
# End-to-end OCI distribution test against a running breezy-registry.
# Covers: base, monolithic + chunked blob upload, cross-repo mount, manifest push
# (image + multi-arch index), tag/digest pull, tags list + pagination, referrers,
# delete, auth, and GC.
set -uo pipefail

REG="${REG:-localhost:5100}"
AUTH="${AUTH:-}" # e.g. "-u admin:secret"
PASS=0
FAIL=0

check() { # check <desc> <expected> <actual>
  if [ "$2" = "$3" ]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    echo "FAIL: $1 (expected $2, got $3)"
  fi
}

code() { curl -s -o /dev/null -w "%{http_code}" $AUTH "$@"; }

digest_of() { shasum -a 256 "$1" | awk '{print "sha256:"$1}'; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

### base
check "GET /v2/" 200 "$(code "http://$REG/v2/")"

### blobs — monolithic single-POST
echo -n '{"arch":"amd64","os":"linux"}' > "$TMP/config.json"
CONFIG_DIGEST=$(digest_of "$TMP/config.json")
check "monolithic POST" 201 "$(code -X POST --data-binary @"$TMP/config.json" \
  "http://$REG/v2/team/app/blobs/uploads/?digest=$CONFIG_DIGEST")"
check "HEAD blob" 200 "$(code -I "http://$REG/v2/team/app/blobs/$CONFIG_DIGEST")"

### blobs — chunked (POST, PATCH x2, PUT)
head -c 100000 /dev/urandom > "$TMP/layer.bin"
LAYER_DIGEST=$(digest_of "$TMP/layer.bin")
LOC=$(curl -s -D - -o /dev/null $AUTH -X POST "http://$REG/v2/team/app/blobs/uploads/" \
  | awk 'tolower($1)=="location:" {print $2}' | tr -d '\r')
check "POST upload started" "yes" "$([ -n "$LOC" ] && echo yes || echo no)"
head -c 60000 "$TMP/layer.bin" > "$TMP/part1"
tail -c 40000 "$TMP/layer.bin" > "$TMP/part2"
check "PATCH chunk 1" 202 "$(code -X PATCH -H "Content-Range: 0-59999" \
  --data-binary @"$TMP/part1" "http://$REG$LOC")"
check "PATCH wrong offset rejected" 416 "$(code -X PATCH -H "Content-Range: 0-39999" \
  --data-binary @"$TMP/part2" "http://$REG$LOC")"
check "PATCH chunk 2" 202 "$(code -X PATCH -H "Content-Range: 60000-99999" \
  --data-binary @"$TMP/part2" "http://$REG$LOC")"
check "PUT finalize" 201 "$(code -X PUT "http://$REG$LOC?digest=$LAYER_DIGEST")"
curl -s $AUTH "http://$REG/v2/team/app/blobs/$LAYER_DIGEST" > "$TMP/layer.out"
check "blob round-trips" "$LAYER_DIGEST" "$(digest_of "$TMP/layer.out")"

### digest mismatch rejected
check "bad digest rejected" 400 "$(code -X POST --data-binary @"$TMP/config.json" \
  "http://$REG/v2/team/app/blobs/uploads/?digest=sha256:$(printf 'a%.0s' {1..64})")"

### range request
check "range GET" 206 "$(code -H "Range: bytes=0-99" \
  "http://$REG/v2/team/app/blobs/$LAYER_DIGEST")"

### cross-repo mount
check "mount existing blob" 201 "$(code -X POST \
  "http://$REG/v2/team/other/blobs/uploads/?mount=$LAYER_DIGEST&from=team/app")"

### manifest push (image)
cat > "$TMP/manifest.json" <<EOF
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "$CONFIG_DIGEST", "size": $(wc -c < "$TMP/config.json" | tr -d ' ')},
  "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "$LAYER_DIGEST", "size": 100000}]
}
EOF
MANIFEST_DIGEST=$(digest_of "$TMP/manifest.json")
check "PUT manifest by tag" 201 "$(code -X PUT \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  --data-binary @"$TMP/manifest.json" "http://$REG/v2/team/app/manifests/v1")"
check "GET manifest by tag" 200 "$(code "http://$REG/v2/team/app/manifests/v1")"
check "GET manifest by digest" 200 "$(code "http://$REG/v2/team/app/manifests/$MANIFEST_DIGEST")"
check "HEAD manifest" 200 "$(code -I "http://$REG/v2/team/app/manifests/v1")"
GOT_DIGEST=$(curl -s -I $AUTH "http://$REG/v2/team/app/manifests/v1" \
  | awk 'tolower($1)=="docker-content-digest:" {print $2}' | tr -d '\r')
check "digest header matches" "$MANIFEST_DIGEST" "$GOT_DIGEST"
check "manifest with missing blob rejected" 400 "$(code -X PUT \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  --data-binary "{\"schemaVersion\":2,\"config\":{\"digest\":\"sha256:$(printf 'b%.0s' {1..64})\",\"size\":1},\"layers\":[]}" \
  "http://$REG/v2/team/app/manifests/broken")"

### multi-arch index
cat > "$TMP/index.json" <<EOF
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "$MANIFEST_DIGEST", "size": $(wc -c < "$TMP/manifest.json" | tr -d ' '), "platform": {"architecture": "amd64", "os": "linux"}}]
}
EOF
check "PUT index" 201 "$(code -X PUT \
  -H "Content-Type: application/vnd.oci.image.index.v1+json" \
  --data-binary @"$TMP/index.json" "http://$REG/v2/team/app/manifests/multi")"

### tags list + pagination
check "second tag push" 201 "$(code -X PUT \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  --data-binary @"$TMP/manifest.json" "http://$REG/v2/team/app/manifests/v2")"
TAGS=$(curl -s $AUTH "http://$REG/v2/team/app/tags/list")
check "tags list contains v1" yes "$(echo "$TAGS" | grep -q '"v1"' && echo yes || echo no)"
PAGE=$(curl -s $AUTH "http://$REG/v2/team/app/tags/list?n=1")
check "paginated tags returns 1" 1 "$(echo "$PAGE" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["tags"]))')"
PAGE2=$(curl -s $AUTH "http://$REG/v2/team/app/tags/list?n=1&last=multi")
check "pagination continues after last" v1 "$(echo "$PAGE2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["tags"][0])')"

### referrers (cosign-style artifact with subject)
cat > "$TMP/sig.json" <<EOF
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "artifactType": "application/vnd.dev.cosign.simplesigning.v1+json",
  "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "$CONFIG_DIGEST", "size": $(wc -c < "$TMP/config.json" | tr -d ' ')},
  "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "$LAYER_DIGEST", "size": 100000}],
  "subject": {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "$MANIFEST_DIGEST", "size": $(wc -c < "$TMP/manifest.json" | tr -d ' ')}
}
EOF
SIG_DIGEST=$(digest_of "$TMP/sig.json")
check "PUT referrer" 201 "$(code -X PUT \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  --data-binary @"$TMP/sig.json" "http://$REG/v2/team/app/manifests/$SIG_DIGEST")"
REFS=$(curl -s $AUTH "http://$REG/v2/team/app/referrers/$MANIFEST_DIGEST")
check "referrers lists signature" yes "$(echo "$REFS" | grep -q "$SIG_DIGEST" && echo yes || echo no)"
FILTERED=$(curl -s $AUTH "http://$REG/v2/team/app/referrers/$MANIFEST_DIGEST?artifactType=application/x-nonexistent")
check "referrers filter excludes" 0 "$(echo "$FILTERED" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["manifests"]))')"

### management API
check "api repos" 200 "$(code "http://$REG/api/v1/repos")"
check "api tags" 200 "$(code "http://$REG/api/v1/tags?repo=team/app")"

### deletes + GC
check "untag v2" 202 "$(code -X DELETE "http://$REG/v2/team/app/manifests/v2")"
check "v2 gone" 404 "$(code "http://$REG/v2/team/app/manifests/v2")"
check "v1 still there" 200 "$(code "http://$REG/v2/team/app/manifests/v1")"
GC=$(curl -s $AUTH -X POST "http://$REG/api/v1/gc?dry_run=1")
check "gc dry run" yes "$(echo "$GC" | grep -q '"dry_run":true' && echo yes || echo no)"

### errors
check "unknown manifest 404" 404 "$(code "http://$REG/v2/team/app/manifests/nope")"
check "unknown repo tags 404" 404 "$(code "http://$REG/v2/no/such/repo/tags/list")"
check "invalid name 400" 400 "$(code "http://$REG/v2/UPPER/manifests/latest")"

echo
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
