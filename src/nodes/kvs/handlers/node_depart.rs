use std::time::Instant;

use eyre::Context;

use crate::{hash_ring::tier_name, messages, nodes::kvs::KvsNode, topics::KvsThread};

impl KvsNode {
    /// Handles incoming node depart messages.
    pub async fn node_depart_handler(&mut self, message: messages::Departed) -> eyre::Result<()> {
        let work_start = Instant::now();

        let serialized =
            serde_json::to_string(&message).context("failed to serialize Departed message")?;
        let messages::Departed {
            tier,
            node_id: departing_node_id,
        } = message;

        log::info!(
            "Received departure for node {} on tier {}.",
            departing_node_id,
            tier_name(tier)
        );

        // update hash ring
        self.global_hash_rings
            .get_mut(&tier)
            .unwrap()
            .remove_node(&departing_node_id);

        if self.thread_id == 0 {
            // tell all worker threads about the node departure
            for tid in 1..self.config_data.thread_num {
                self.zenoh
                    .put(
                        &KvsThread::new(self.node_id.clone(), tid)
                            .node_depart_topic(&self.zenoh_prefix),
                        serialized.as_str(),
                    )
                    .await
                    .map_err(|e| eyre::eyre!(e))?;
            }

            for (&tier, ring) in &self.global_hash_rings {
                log::info!(
                    "Hash ring for tier {} size is {}.",
                    tier_name(tier),
                    ring.len()
                );
            }
        }

        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 1);

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
    fn simple_node_depart() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();

        let zenoh_clone = zenoh.clone();
        let mut subscriber = zenoh_clone
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let mut server = kvs_test_instance(zenoh, zenoh_prefix);

        let second_node_id = "127.0.0.2".to_owned();
        server.config_data.thread_num = 2;
        server
            .global_hash_rings
            .entry(Tier::Memory)
            .or_default()
            .insert_node(second_node_id.clone(), 0);

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 6000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            2
        );

        let departed = messages::Departed {
            tier: Tier::Memory,
            node_id: second_node_id,
        };
        smol::block_on(server.node_depart_handler(departed.clone())).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let message_parsed: messages::Departed =
            serde_json::from_str(&message.value.as_string().unwrap()).unwrap();
        assert_eq!(message_parsed, departed);

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1
        );
    }

    #[test]
    fn fake_node_depart() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();

        let mut server = kvs_test_instance(zenoh, zenoh_prefix);

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1
        );

        let message = messages::Departed {
            tier: Tier::Memory,
            node_id: "127.0.0.2".to_owned(),
        };
        smol::block_on(server.node_depart_handler(message)).unwrap();

        assert_eq!(server.global_hash_rings[&Tier::Memory].len(), 3000);
        assert_eq!(
            server.global_hash_rings[&Tier::Memory].unique_nodes().len(),
            1
        );
    }
}
