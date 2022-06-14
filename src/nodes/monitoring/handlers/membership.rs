use crate::{
    hash_ring::tier_name,
    messages::{self},
    nodes::monitoring::MonitoringNode,
    topics::KvsThread,
};
use eyre::{bail, Context};
use std::time::Instant;

impl<'a> MonitoringNode<'a> {
    pub(in crate::nodes::monitoring) async fn membership_handler(
        &mut self,
        serialized: &str,
    ) -> eyre::Result<()> {
        let parsed =
            serde_json::from_str(serialized).context("failed to deserialize notify message")?;

        match parsed {
            messages::Notify::Join(join_message) => {
                let messages::Join {
                    tier,
                    node_id: new_node_id,
                    join_count,
                } = join_message;

                log::info!(
                    "Received join from server {} in tier {}.",
                    new_node_id,
                    tier_name(tier)
                );

                match tier {
                    messages::Tier::Memory => {
                        self.global_hash_rings
                            .entry(tier)
                            .or_default()
                            .insert_node(new_node_id, join_count); // TODO: 0 instead of join_count?

                        self.new_memory_count = self.new_memory_count.saturating_sub(1);

                        // reset grace period timer
                        self.grace_start = Instant::now();
                    }
                    messages::Tier::Disk => {
                        self.global_hash_rings
                            .entry(tier)
                            .or_default()
                            .insert_node(new_node_id, join_count); // TODO: 0 instead of join_count?

                        self.new_ebs_count = self.new_ebs_count.saturating_sub(1);

                        // reset grace period timer
                        self.grace_start = Instant::now();
                    }
                    messages::Tier::Routing => {
                        self.routing_node_ids.insert(new_node_id);
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
                    .entry(tier)
                    .and_modify(|e| e.remove_node(&departed_node_id));

                match tier {
                    messages::Tier::Memory => {
                        self.memory_storage.remove(&departed_node_id);
                        self.memory_occupancy.remove(&departed_node_id);

                        for access_map in &mut self.key_access_frequency.values_mut() {
                            for i in 0..self.config_data.memory_thread_count {
                                access_map.remove(&KvsThread {
                                    node_id: departed_node_id.clone(),
                                    thread_id: i,
                                });
                            }
                        }
                    }
                    messages::Tier::Disk => {
                        self.ebs_storage.remove(&departed_node_id);
                        self.ebs_occupancy.remove(&departed_node_id);

                        for access_map in self.key_access_frequency.values_mut() {
                            for i in 0..self.config_data.ebs_thread_count {
                                access_map.remove(&KvsThread {
                                    node_id: departed_node_id.clone(),
                                    thread_id: i,
                                });
                            }
                        }
                    }
                    messages::Tier::Routing => bail!("invalid depart message for routing tier"),
                }

                for (&tier, ring) in &self.global_hash_rings {
                    log::info!(
                        "Hash ring for tier {} size is {}.",
                        tier_name(tier),
                        ring.len()
                    );
                }
            }
        }

        self.collect_stats().await?;

        Ok(())
    }
}
