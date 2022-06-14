use crate::{
    lattice::Lattice, messages::Response, metadata::MetadataKey, nodes::kvs::KvsNode, ClientKey,
    Key,
};
use eyre::{anyhow, bail, Context};
use std::{collections::HashSet, time::Instant};

impl KvsNode {
    /// Handles incoming cache_ip messages.
    pub async fn cache_ip_handler(&mut self, serialized: &str) -> eyre::Result<()> {
        let work_start = Instant::now();

        // The response will be a list of cache IPs and their responsible keys.
        let response: Response =
            serde_json::from_str(serialized).context("failed to deserialize KeyResponse")?;

        for tuple in response.tuples {
            // tuple is a key-value pair from the KVS;
            // here, the key is the metadata key for the cache IP,
            // and the value is the list of keys that cache is responsible for.

            if tuple.error.is_none() {
                // Extract the cache ID.
                let cache_id = match &tuple.key {
                    Key::Metadata(MetadataKey::CacheId { key }) => key.clone(),
                    _ => bail!("not a cache_ip metadata key"),
                };

                // Extract the keys that the cache is responsible for.
                let lww_value = tuple
                    .lattice
                    .as_ref()
                    .ok_or_else(|| anyhow!("lattice not set on cache ip tuple"))?
                    .as_lww()?;

                let key_set: HashSet<ClientKey> =
                    serde_json::from_slice(lww_value.reveal().value().as_slice())
                        .context("failed to deserialize StringSet")?;

                // First, update key_to_cache_ips with dropped keys for this cache.

                // Figure out which keys are in the old set of keys for this IP
                // that are not in the new fresh set of keys for this IP.
                // (We can do this by destructively modifying the old set of keys
                // since we don't need it anymore.)
                let old_keys_for_ip = self.cache_id_to_keys.entry(cache_id.clone()).or_default();
                for cache_key in &key_set {
                    old_keys_for_ip.remove(cache_key);
                }
                let deleted_keys = old_keys_for_ip;

                // For the keys that have been deleted from this cache,
                // remove them from the key->caches mapping too.
                for key in deleted_keys.drain() {
                    self.key_to_cache_ids
                        .entry(key.into())
                        .or_default()
                        .remove(&cache_id);
                }

                let cache_keys = deleted_keys;
                assert!(cache_keys.is_empty());

                // Now we can update cache_ip_to_keys,
                // as well as add new keys to key_to_cache_ips.
                for cache_key in key_set {
                    cache_keys.insert(cache_key.clone());
                    self.key_to_cache_ids
                        .entry(cache_key.into())
                        .or_default()
                        .insert(cache_id.clone());
                }
            }

            // We can also get error 1 (key does not exist)
            // or error 2 (node not responsible for key).
            // We just ignore these for now;
            // 1 means the cache has not told the kvs about any keys yet,
            // and 2 will be fixed on our next cached keys update interval.
        }

        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 7);

        Ok(())
    }
}
