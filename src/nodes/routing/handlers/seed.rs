use crate::{
    messages::cluster_membership::{ClusterInfo, TierMembership},
    nodes::routing::RoutingNode,
};

impl RoutingNode {
    /// Returns the current cluster information as a reply to a seed message.
    pub fn seed_handler(&self) -> String {
        log::info!("Received a global hash ring membership request.");

        let mut membership = ClusterInfo {
            tiers: Vec::new(),
            routing_node_ids: self.known_routing_nodes.iter().cloned().collect(),
        };

        for (&tier, hash_ring) in &self.global_hash_rings {
            let mut tier = TierMembership {
                tier_id: tier,
                servers: Default::default(),
            };

            for node_id in hash_ring.unique_nodes() {
                tier.servers.push(node_id.clone());
            }

            membership.tiers.push(tier);
        }

        serde_json::to_string(&membership).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        messages::{cluster_membership::ClusterInfo, Tier},
        nodes::routing::router_test_instance,
        zenoh_test_instance,
    };

    #[test]
    fn seed() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();

        let router = router_test_instance(zenoh, zenoh_prefix);

        assert_eq!(router.global_hash_rings[&Tier::Memory].len(), 3000);

        let serialized = router.seed_handler();

        assert_eq!(router.global_hash_rings[&Tier::Memory].len(), 3000);

        let membership: ClusterInfo = serde_json::from_str(&serialized).unwrap();

        assert_eq!(membership.tiers.len(), 1);

        for tier in &membership.tiers {
            for other in &tier.servers {
                assert_eq!(tier.tier_id, Tier::Memory);
                assert_eq!(other, &router.node_id);
            }
        }
    }
}
