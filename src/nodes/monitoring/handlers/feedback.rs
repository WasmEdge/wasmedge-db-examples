use crate::{
    messages::user_feedback::{KeyLatency, UserFeedback},
    nodes::monitoring::{MonitoringNode, SLO_WORST},
};
use eyre::Context;

impl<'a> MonitoringNode<'a> {
    pub(in crate::nodes::monitoring) async fn feedback_handler(
        &mut self,
        serialized: &str,
    ) -> eyre::Result<()> {
        let fb: UserFeedback =
            serde_json::from_str(serialized).context("failed to deserialize notify message")?;

        if fb.finish {
            self.user_latency.remove(&fb.uid);
        } else {
            // collect latency and throughput feedback
            self.user_latency.insert(fb.uid.clone(), fb.latency);
            self.user_throughput.insert(fb.uid, fb.throughput);

            // collect replication factor adjustment factors
            for KeyLatency {
                key,
                latency: observed_key_latency,
            } in fb.key_latency
            {
                match self.latency_miss_ratio.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        let entry = entry.get_mut();
                        entry.0 = (entry.0 * entry.1 as f64 + observed_key_latency / SLO_WORST)
                            / (entry.1 + 1) as f64;
                        entry.1 += 1;
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert((observed_key_latency / SLO_WORST, 1));
                    }
                }
            }
        }

        self.collect_stats().await?;

        Ok(())
    }
}
