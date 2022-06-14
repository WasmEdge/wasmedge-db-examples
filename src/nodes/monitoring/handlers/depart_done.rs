use crate::{
    hash_ring::tier_name,
    messages::{management::RemoveNode, Departed},
    nodes::monitoring::MonitoringNode,
};
use eyre::{anyhow, bail, Context};
use std::time::Instant;

impl<'a> MonitoringNode<'a> {
    pub(in crate::nodes::monitoring) async fn depart_done_handler(
        &mut self,
        serialized: &str,
    ) -> eyre::Result<()> {
        let departed: Departed =
            serde_json::from_str(serialized).context("failed to deserialize Depart message")?;

        let entry = self
            .departing_node_map
            .get_mut(&departed.node_id)
            .ok_or_else(|| anyhow!("Missing entry in the depart done map."))?;
        *entry = entry
            .checked_sub(1)
            .ok_or_else(|| anyhow!("departing_node_map entry is already 0"))?;

        if *entry == 0 {
            self.departing_node_map.remove(&departed.node_id);

            match departed.tier {
                crate::messages::Tier::Memory => {
                    self.removing_memory_node = false;
                }
                crate::messages::Tier::Disk => {
                    self.removing_ebs_node = false;
                }
                crate::messages::Tier::Routing => {
                    bail!("departing of routing nodes is not supported")
                }
            }

            log::info!(
                "Removing {} node {}.",
                tier_name(departed.tier),
                departed.node_id,
            );

            if let Some(management_node) = &self.management_node {
                self.zenoh
                    .put(
                        &management_node.remove_node_topic(&self.zenoh_prefix),
                        serde_json::to_string(&RemoveNode {
                            departed_node_id: departed.node_id,
                        })?,
                    )
                    .await
                    .map_err(|e| eyre::eyre!(e))?;
            }

            // reset grace period timer
            self.grace_start = Instant::now();
        }

        self.collect_stats().await?;

        Ok(())
    }
}
