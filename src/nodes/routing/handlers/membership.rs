use crate::{
    hash_ring::tier_name,
    messages::{self, Notify},
    nodes::routing::RoutingNode,
    topics::{KvsThread, RoutingThread},
    ALL_TIERS,
};
use eyre::Context;

impl RoutingNode {
    /// Handles incoming [`Notify`][crate::messages::Notify] messages.
    pub async fn membership_handler(&mut self, message: &Notify) -> eyre::Result<()> {
        match message {
            messages::Notify::Join(join_message) => {
                let messages::Join {
                    tier,
                    node_id: new_node_id,
                    join_count,
                } = join_message.clone();

                log::info!(
                    "Received join from server {} in tier {}.",
                    new_node_id,
                    tier_name(tier)
                );

                // update hash ring
                let inserted = self
                    .global_hash_rings
                    .entry(tier)
                    .or_default()
                    .insert_node(new_node_id.clone(), join_count);

                if inserted && self.thread_id == 0 {
                    // gossip the new node address between server nodes to ensure
                    // consistency
                    for hash_ring in self.global_hash_rings.values() {
                        let msg = serde_json::to_string(&join_message)
                            .context("failed to serialize join message")?;

                        for node_id in hash_ring.unique_nodes() {
                            // if the node is not the newly joined node, send the ip of the
                            // newly joined node
                            if node_id != new_node_id.as_str() {
                                self.zenoh
                                    .put(
                                        &KvsThread::new(node_id.clone(), 0)
                                            .node_join_topic(&self.zenoh_prefix),
                                        msg.as_str(),
                                    )
                                    .await
                                    .map_err(|e| eyre::eyre!(e))
                                    .context("failed to send ip of newly joined node")?;
                            }
                        }
                    }

                    // tell all worker threads about the message
                    let serialized = serde_json::to_string(&message)
                        .context("failed to serialize Notify message")?;
                    for tid in 1..self.config_data.routing_thread_count {
                        self.zenoh
                            .put(
                                &RoutingThread::new(self.node_id.clone(), tid)
                                    .notify_topic(&self.zenoh_prefix),
                                serialized.as_str(),
                            )
                            .await
                            .map_err(|e| eyre::eyre!(e))
                            .context("failed to forward join message to worker threads")?;
                    }
                }

                for (&tier, ring) in &self.global_hash_rings {
                    log::info!(
                        "Hash ring for tier {} size is {}.",
                        tier_name(tier),
                        ring.len()
                    );
                }
            }
            messages::Notify::Depart(messages::Departed {
                tier,
                node_id: departed_node_id,
            }) => {
                log::info!("Received depart from server {}.", departed_node_id,);
                self.global_hash_rings
                    .entry(*tier)
                    .and_modify(|e| e.remove_node(&departed_node_id));

                for i in 0.. {
                    let departed_thread = KvsThread {
                        node_id: departed_node_id.clone(),
                        thread_id: i,
                    };
                    if self.node_connections.remove(&departed_thread).is_none() {
                        break;
                    }
                }

                if self.thread_id == 0 {
                    // tell all worker threads about the message
                    let serialized = serde_json::to_string(&message)
                        .context("failed to serialize Notify message")?;
                    for tid in 1..self.config_data.routing_thread_count {
                        self.zenoh
                            .put(
                                &RoutingThread::new(self.node_id.clone(), tid)
                                    .notify_topic(&self.zenoh_prefix),
                                serialized.as_str(),
                            )
                            .await
                            .map_err(|e| eyre::eyre!(e))
                            .context("failed to forward depart message to worker threads")?;
                    }
                }

                for &tier in ALL_TIERS {
                    log::info!(
                        "Hash ring for tier {} size is {}.",
                        tier_name(tier),
                        self.global_hash_rings
                            .get(&tier)
                            .map(|e| e.len())
                            .unwrap_or(0)
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zenoh::prelude::{Receiver, ZFuture};

    use crate::{
        messages::{self, Tier},
        nodes::routing::router_test_instance,
        zenoh_test_instance, ZenohValueAsString,
    };
    use std::time::Duration;

    #[test]
    fn membership() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let mut router = router_test_instance(zenoh.clone(), zenoh_prefix);

        assert_eq!(router.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            router.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1
        );

        let join_message = messages::Join {
            tier: Tier::Memory,
            node_id: "127.0.0.2".into(),
            join_count: 0,
        };
        smol::block_on(router.membership_handler(&messages::Notify::Join(join_message.clone())))
            .unwrap();

        let message: messages::Join = {
            let sample = subscriber
                .receiver()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            serde_json::from_str(&sample.value.as_string().unwrap()).unwrap()
        };

        assert_eq!(message, join_message);

        assert_eq!(router.global_hash_rings[&Tier::Memory].len(), 6000);
        assert_eq!(
            router.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            2
        );
    }
}
