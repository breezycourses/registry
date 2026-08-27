use sha2::{Digest, Sha256};

/// Rendezvous (highest-random-weight) hashing: every node independently agrees on
/// which shard owns a repository, with no coordination and minimal churn when the
/// shard list changes. Repos never share metadata, so this is the entire sharding
/// story — each shard is a fully independent breezy-registry.
pub fn owner<'a>(repo: &str, shards: &'a [String]) -> &'a str {
    shards
        .iter()
        .max_by_key(|shard| {
            let mut h = Sha256::new();
            h.update(shard.as_bytes());
            h.update(b"|");
            h.update(repo.as_bytes());
            let d = h.finalize();
            u64::from_be_bytes(d[..8].try_into().unwrap())
        })
        .map(|s| s.as_str())
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_and_distributed() {
        let shards = vec![
            "http://a".to_string(),
            "http://b".to_string(),
            "http://c".to_string(),
        ];
        let o1 = owner("team/app", &shards);
        let o2 = owner("team/app", &shards);
        assert_eq!(o1, o2);
        // Every shard owns something across a spread of repo names.
        let mut seen = std::collections::HashSet::new();
        for i in 0..100 {
            seen.insert(owner(&format!("repo-{i}"), &shards));
        }
        assert_eq!(seen.len(), 3);
    }
}
