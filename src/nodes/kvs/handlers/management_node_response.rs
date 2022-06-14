use crate::{
    messages::{management::NodeSet, request::RequestData, Request, Tier},
    metadata::MetadataKey,
    nodes::kvs::KvsNode,
};
use eyre::Context;

use rand::prelude::SliceRandom;
use std::{collections::HashMap, mem, time::Instant};

impl KvsNode {
    /// Handles incoming management node responses.
    pub async fn management_node_response_handler(&mut self, serialized: &str) -> eyre::Result<()> {
        let work_start = Instant::now();

        // Get the response.
        let func_nodes: NodeSet =
            serde_json::from_str(serialized).context("failed to deserialize StringSet")?;

        // Update extant_caches with the response.
        let mut deleted_caches = mem::take(&mut self.extant_caches);
        for func_node in func_nodes.nodes {
            let func_node = func_node
                .parse()
                .context("failed to parse func_node as ip address")?;
            deleted_caches.remove(&func_node);
            self.extant_caches.insert(func_node);
        }

        // Process deleted caches
        // (cache IPs that we were tracking but were not in the newest list of
        // caches).
        for cache_ip in deleted_caches {
            self.cache_id_to_keys.remove(&cache_ip);
            for cache in self.key_to_cache_ids.values_mut() {
                cache.remove(&cache_ip);
            }
        }

        // Get the cached keys by cache IP.
        // First, prepare the requests for all the IPs we know about
        // and put them in an address request map.
        let mut addr_request_map = HashMap::new();
        for cacheip in &self.extant_caches {
            let key = MetadataKey::CacheId {
                key: cacheip.to_string(),
            };
            let threads = self.hash_ring_util.get_responsible_threads_metadata(
                &key,
                &self.global_hash_rings[&Tier::Memory],
                &self.local_hash_rings[&Tier::Memory],
            )?;

            if threads.is_empty() {
                // In case no servers have joined yet.
                let target_address = threads
                    .choose(&mut rand::thread_rng())
                    .unwrap()
                    .request_topic(&self.zenoh_prefix);
                if let std::collections::hash_map::Entry::Vacant(e) =
                    addr_request_map.entry(target_address)
                {
                    let response_address = self.wt.cache_ip_response_topic(&self.zenoh_prefix);
                    e.insert(Request {
                        request: RequestData::Get {
                            keys: vec![key.into()],
                        },
                        // NB: response_address might not be necessary here
                        // (or in other places where req_id is constructed either).
                        request_id: Some(format!("{}:{}", response_address, self.request_id)),
                        response_address: Some(response_address.to_string()),
                        address_cache_size: Default::default(),
                    });

                    self.request_id += 1;
                }
            }
        }

        // Loop over the address request map and execute all the requests.
        for (address, request) in addr_request_map {
            let serialized_req =
                serde_json::to_string(&request).context("failed to serialize KeyRequest")?;
            self.zenoh
                .put(&address, serialized_req)
                .await
                .map_err(|e| eyre::eyre!(e))?;
        }

        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 8);

        Ok(())
    }
}
