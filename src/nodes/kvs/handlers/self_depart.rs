use crate::{
    messages::{self, Departed, SelfDepart},
    nodes::kvs::KvsNode,
    topics::{KvsThread, MonitoringThread, RoutingThread},
    Key, ALL_TIERS,
};
use eyre::Context;
use std::collections::{HashMap, HashSet};

impl KvsNode {
    /// Handles incoming self depart messages.
    pub async fn self_depart_handler(&mut self, serialized: &str) -> eyre::Result<()> {
        let self_depart: SelfDepart =
            serde_json::from_str(serialized).context("failed to deserialize ResponseTopic")?;
        let ack_topic = self_depart.response_topic;

        log::info!("This node is departing.");
        self.global_hash_rings
            .get_mut(&self.config_data.self_tier)
            .unwrap()
            .remove_node(&self.node_id);

        // thread 0 notifies other nodes in the cluster (of all types) that it is
        // leaving the cluster
        if self.thread_id == 0 {
            let depart_message = messages::Departed {
                tier: self.config_data.self_tier,
                node_id: self.node_id.clone(),
            };

            for hash_ring in self.global_hash_rings.values() {
                for node_id in hash_ring.unique_nodes() {
                    self.zenoh
                        .put(
                            &KvsThread::new(node_id.clone(), 0)
                                .node_depart_topic(&self.zenoh_prefix),
                            serde_json::to_string(&depart_message)
                                .context("failed to serialize depart message")?,
                        )
                        .await
                        .map_err(|e| eyre::eyre!(e))?;
                }
            }

            let notify_message = serde_json::to_string(&messages::Notify::Depart(depart_message))
                .context("failed to serialize notify message")?;

            // notify all routing nodes
            for node_id in &self.membership.routing_node_ids {
                self.zenoh
                    .put(
                        &RoutingThread::new(node_id.clone(), 0).notify_topic(&self.zenoh_prefix),
                        notify_message.as_str(),
                    )
                    .await
                    .map_err(|e| eyre::eyre!(e))?;
            }

            // notify monitoring nodes
            self.zenoh
                .put(
                    &MonitoringThread::notify_topic(&self.zenoh_prefix),
                    notify_message.as_str(),
                )
                .await
                .map_err(|e| eyre::eyre!(e))?;

            // tell all worker threads about the self departure
            for tid in 1..self.config_data.thread_num {
                self.zenoh
                    .put(
                        &KvsThread::new(self.node_id.clone(), tid)
                            .self_depart_topic(&self.zenoh_prefix),
                        serialized,
                    )
                    .await
                    .map_err(|e| eyre::eyre!(e))?;
            }
        }

        let mut addr_keyset_map: HashMap<String, HashSet<Key>> = HashMap::new();

        for key in self.kvs.keys() {
            let threads = self
                .hash_ring_util
                .try_get_responsible_threads(
                    self.wt.replication_response_topic(&self.zenoh_prefix),
                    key.clone(),
                    &self.global_hash_rings,
                    &self.local_hash_rings,
                    &self.key_replication_map,
                    ALL_TIERS,
                    &self.zenoh,
                    &self.zenoh_prefix,
                    &mut self.node_connections,
                )
                .await?;

            if let Some(threads) = threads {
                // since we already removed this node from the hash ring, no need to
                // exclude it explicitly
                for thread in threads {
                    addr_keyset_map
                        .entry(thread.gossip_topic(&self.zenoh_prefix))
                        .or_default()
                        .insert(key.clone());
                }
            } else {
                log::error!("Missing key replication factor in node depart routine");
            }
        }

        self.send_gossip(&addr_keyset_map).await?;
        self.zenoh
            .put(
                &ack_topic,
                serde_json::to_string(&Departed {
                    tier: self.config_data.self_tier,
                    node_id: self.node_id.clone(),
                })?,
            )
            .await
            .map_err(|e| eyre::eyre!(e))?;

        // TODO: make the main loop in `run` exit with `return 0;`
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zenoh::prelude::{Receiver, ZFuture};

    use crate::{
        messages::{Departed, SelfDepart, Tier},
        nodes::kvs::kvs_test_instance,
        zenoh_test_instance, ZenohValueAsString,
    };
    use std::time::Duration;

    #[test]
    fn self_depart() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let self_depart = SelfDepart {
            response_topic: format!("{}/self_depart_test_response_address", zenoh_prefix),
        };

        let mut subscriber = zenoh.subscribe(&self_depart.response_topic).wait().unwrap();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1
        );
        let serialized = serde_json::to_string(&self_depart).unwrap();
        smol::block_on(server.self_depart_handler(&serialized)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 0);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            0
        );

        assert_eq!(
            message.key_expr.try_as_str().unwrap(),
            self_depart.response_topic
        );
        let depart_msg: Departed =
            serde_json::from_str(&message.value.as_string().unwrap()).unwrap();
        assert_eq!(
            depart_msg,
            Departed {
                node_id: server.node_id,
                tier: Tier::Memory
            }
        );
    }

    // TODO: test should add keys and make sure that they are gossiped elsewhere
    // TODO: test should make sure that depart messages are sent to the worker
    // threads
}
