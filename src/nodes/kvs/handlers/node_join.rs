use crate::{hash_ring::tier_name, messages, nodes::kvs::KvsNode, topics::KvsThread};
use eyre::Context;
use std::time::Instant;

impl KvsNode {
    /// Handles incoming node join messages.
    pub async fn node_join_handler(&mut self, message: messages::Join) -> eyre::Result<()> {
        let work_start = Instant::now();

        let serialized =
            serde_json::to_string(&message).context("failed to serialize Join message")?;
        let messages::Join {
            tier,
            node_id: new_server_node_id,
            join_count,
        } = message;

        // update global hash ring
        let inserted = self
            .global_hash_rings
            .entry(tier)
            .or_default()
            .insert_node(new_server_node_id.to_owned(), join_count);

        if inserted {
            log::info!(
                "Received a node join for tier {}. New node is {}. It's join counter is {}.",
                tier_name(tier),
                new_server_node_id,
                join_count
            );

            // only thread 0 communicates with other nodes and receives join messages
            // and it communicates that information to non-0 threads on its own machine
            if self.thread_id == 0 {
                // send my ID to the new server node
                let msg = serde_json::to_string(&messages::Join {
                    tier: self.config_data.self_tier,
                    node_id: self.node_id.clone(),
                    join_count: self.self_join_count,
                })
                .context("failed to serialize join message")?;

                self.zenoh
                    .put(
                        &KvsThread::new(new_server_node_id.to_owned(), 0)
                            .node_join_topic(&self.zenoh_prefix),
                        msg.as_str(),
                    )
                    .await
                    .map_err(|e| eyre::eyre!(e))
                    .context("failed to send ip to new server node")?;

                // gossip the new node address between server nodes to ensure consistency
                for (&tier, hash_ring) in self.global_hash_rings.iter() {
                    for node_id in hash_ring.unique_nodes() {
                        // if the node is not myself and not the newly joined node, send the
                        // ip of the newly joined node in case of a race condition
                        if node_id != &self.node_id && node_id != new_server_node_id.as_str() {
                            self.zenoh
                                .put(
                                    &KvsThread::new(node_id.clone(), 0).node_join_topic(&self.zenoh_prefix),
                                    serialized.as_str(),
                                )
                                .await
                                .map_err(|e| eyre::eyre!(e))
                                .context(
                                    "failed to send ip of new server node in case of race condition",
                                )?;
                        }
                    }

                    log::info!(
                        "Hash ring for tier {} is size {}.",
                        tier_name(tier),
                        hash_ring.len()
                    );
                }

                // tell all worker threads about the new node join
                for tid in 1..self.config_data.thread_num {
                    self.zenoh
                        .put(
                            &KvsThread::new(self.node_id.clone(), tid)
                                .node_join_topic(&self.zenoh_prefix),
                            serialized.as_str(),
                        )
                        .await
                        .map_err(|e| eyre::eyre!(e))
                        .context("failed to send ip of new server node to workers")?;
                }
            }

            if tier == self.config_data.self_tier {
                for key in self.kvs.keys() {
                    let threads = self
                        .hash_ring_util
                        .try_get_responsible_threads(
                            self.wt.replication_response_topic(&self.zenoh_prefix),
                            key.clone(),
                            &self.global_hash_rings,
                            &self.local_hash_rings,
                            &self.key_replication_map,
                            &[self.config_data.self_tier],
                            &self.zenoh,
                            &self.zenoh_prefix,
                            &mut self.node_connections,
                        )
                        .await
                        .map_err(|e| eyre::eyre!(e))
                        .context("failed to get responsible threads")?;

                    if let Some(threads) = threads {
                        // there are two situations in which we gossip data to the joining
                        // node:
                        // 1) if the node is a new node and I am no longer responsible for
                        // the key
                        // 2) if the node is rejoining the cluster, and it is responsible for
                        // the key
                        // NOTE: This is currently inefficient because every server will
                        // gossip the key currently -- we might be able to hack around the
                        // has ring to do it more efficiently, but I'm leaving this here for
                        // now
                        if join_count > 0 {
                            for thread in &threads {
                                if thread.node_id == new_server_node_id {
                                    self.join_gossip_map
                                        .entry(thread.gossip_topic(&self.zenoh_prefix))
                                        .or_default()
                                        .insert(key.clone());
                                }
                            }
                        } else if join_count == 0 && !threads.contains(&self.wt) {
                            self.join_remove_set.insert(key.clone());

                            for thread in &threads {
                                self.join_gossip_map
                                    .entry(thread.gossip_topic(&self.zenoh_prefix))
                                    .or_default()
                                    .insert(key.clone());
                            }
                        }
                    } else {
                        log::error!(
                            "Missing key replication factor in node join \
                                    routine. This should never happen."
                        );
                    }
                }
            }
        }

        let time_elapsed = Instant::now() - work_start;

        self.report_data.record_working_time(time_elapsed, 0);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zenoh::prelude::{Receiver, ZFuture};

    use crate::{
        messages::{self, Tier},
        nodes::kvs::kvs_test_instance,
        zenoh_test_instance, ZenohValueAsString,
    };
    use std::time::Duration;

    #[test]
    fn basic_node_join() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix);

        server.config_data.thread_num = 2;

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1,
        );

        let message = messages::Join {
            tier: Tier::Memory,
            node_id: "127.0.0.2".into(),
            join_count: 0,
        };

        smol::block_on(server.node_join_handler(message.clone())).unwrap();

        let message_1: messages::Join = {
            let raw = subscriber
                .receiver()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            serde_json::from_str(&raw.value.as_string().unwrap()).unwrap()
        };
        assert_eq!(
            message_1,
            messages::Join {
                tier: Tier::Memory,
                node_id: "server_id".into(),
                join_count: 0,
            },
        );
        let message_2 = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let message_2_parsed: messages::Join =
            serde_json::from_str(&message_2.value.as_string().unwrap()).unwrap();
        assert_eq!(message_2_parsed, message);

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 6000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            2,
        );
    }

    #[test]
    fn duplicate_node_join() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix);

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1
        );

        let message = messages::Join {
            tier: Tier::Memory,
            node_id: server.node_id.clone(),
            join_count: 0,
        };
        smol::block_on(server.node_join_handler(message)).unwrap();

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1
        );
    }
}
