use crate::{
    messages::replication_factor::ReplicationFactorUpdate, nodes::routing::RoutingNode,
    topics::RoutingThread,
};
use eyre::Context;

impl RoutingNode {
    /// Handles incoming replication change messages.
    ///
    /// Messages should be of type [`ReplicationFactorUpdate`].
    pub async fn replication_change_handler(&mut self, serialized: &str) -> eyre::Result<()> {
        log::info!("replication_change_puller");

        if self.thread_id == 0 {
            // tell all worker threads about the replication factor change
            for tid in 1..self.config_data.routing_thread_count {
                self.zenoh
                    .put(
                        &RoutingThread::new(self.node_id.clone(), tid)
                            .replication_change_topic(&self.zenoh_prefix),
                        serialized,
                    )
                    .await
                    .map_err(|e| eyre::eyre!(e))
                    .context("failed to forward replication change message to worker threads")?;
            }
        }

        let update: ReplicationFactorUpdate = serde_json::from_str(serialized)
            .context("failed to parse replication factor update")?;

        for key_rep in &update.updates {
            let key = key_rep.key.clone();
            log::info!("Received a replication factor change for key {}.", key);

            let key_replication_map_entry = self.key_replication_map.entry(key).or_default();

            for global in &key_rep.global {
                key_replication_map_entry
                    .global_replication
                    .insert(global.tier, global.value);
            }

            for local in &key_rep.local {
                key_replication_map_entry
                    .local_replication
                    .insert(local.tier, local.value);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zenoh::prelude::{Receiver, ZFuture};

    use crate::{
        messages::{
            replication_factor::{ReplicationFactor, ReplicationFactorUpdate, ReplicationValue},
            Tier,
        },
        nodes::routing::{router_test_instance, ConfigData},
        zenoh_test_instance, ClientKey, ZenohValueAsString, ALL_TIERS,
    };
    use std::{sync::Arc, time::Duration};

    #[test]
    fn replication_change() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let keys: Vec<_> = ["key0", "key1", "key2"]
            .iter()
            .map(|&k| ClientKey::from(k))
            .collect();

        let mut router = router_test_instance(zenoh.clone(), zenoh_prefix.clone());
        router.config_data = Arc::new(ConfigData {
            routing_thread_count: 3,
            ..(*router.config_data).clone()
        });
        for key in &keys {
            let entry = router.key_replication_map.entry(key.clone()).or_default();

            entry.global_replication.insert(Tier::Memory, 1);
            entry.global_replication.insert(Tier::Disk, 1);
            entry.local_replication.insert(Tier::Memory, 1);
            entry.local_replication.insert(Tier::Disk, 1);
        }

        let mut update = ReplicationFactorUpdate::default();
        for key in &keys {
            let mut rf = ReplicationFactor {
                key: key.clone(),
                global: Default::default(),
                local: Default::default(),
            };

            for &tier in ALL_TIERS {
                let rep_global = ReplicationValue { tier, value: 2 };
                rf.global.push(rep_global);

                let rep_local = ReplicationValue { tier, value: 3 };
                rf.local.push(rep_local);
            }

            update.updates.push(rf);
        }

        let serialized = serde_json::to_string(&update).unwrap();

        smol::block_on(router.replication_change_handler(&serialized)).unwrap();

        let message_1 = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        assert_eq!(message_1.value.as_string().unwrap(), serialized.as_str());
        let message_2 = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        assert_eq!(message_2.value.as_string().unwrap(), serialized.as_str());

        for key in keys {
            assert_eq!(
                router.key_replication_map[&key].global_replication[&Tier::Memory],
                2
            );
            assert_eq!(
                router.key_replication_map[&key].global_replication[&Tier::Disk],
                2
            );
            assert_eq!(
                router.key_replication_map[&key].local_replication[&Tier::Memory],
                3
            );
            assert_eq!(
                router.key_replication_map[&key].local_replication[&Tier::Disk],
                3
            );
        }
    }
}
