// Hand-maintained Diesel schema. Keep in sync with db::SCHEMA.

diesel::table! {
    repos (id) {
        id -> BigInt,
        name -> Text,
        created_at -> BigInt,
        index_etag -> Nullable<Text>,
        index_version -> Nullable<BigInt>,
    }
}

diesel::table! {
    blobs (digest) {
        digest -> Text,
        size -> BigInt,
        created_at -> BigInt,
    }
}

diesel::table! {
    manifests (id) {
        id -> BigInt,
        repo_id -> BigInt,
        digest -> Text,
        media_type -> Text,
        payload -> Binary,
        size -> BigInt,
        subject_digest -> Nullable<Text>,
        artifact_type -> Nullable<Text>,
        annotations -> Nullable<Text>,
        created_at -> BigInt,
    }
}

diesel::table! {
    manifest_refs (manifest_id, child_digest, kind) {
        manifest_id -> BigInt,
        child_digest -> Text,
        kind -> Text,
    }
}

diesel::table! {
    tags (repo_id, name) {
        repo_id -> BigInt,
        name -> Text,
        manifest_id -> BigInt,
        pushed_at -> BigInt,
    }
}

diesel::table! {
    uploads (uuid) {
        uuid -> Text,
        repo_id -> BigInt,
        bytes_received -> BigInt,
        created_at -> BigInt,
    }
}

diesel::joinable!(manifests -> repos (repo_id));
diesel::joinable!(tags -> repos (repo_id));
diesel::joinable!(uploads -> repos (repo_id));
diesel::joinable!(manifest_refs -> manifests (manifest_id));

diesel::allow_tables_to_appear_in_same_query!(repos, blobs, manifests, manifest_refs, tags, uploads);
