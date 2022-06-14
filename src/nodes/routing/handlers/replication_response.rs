use crate::{
    lattice::Lattice,
    messages::{
        replication_factor::ReplicationFactor, AddressResponse, KeyAddress, Response, TcpMessage,
        Tier,
    },
    metadata::MetadataKey,
    nodes::{routing::RoutingNode, send_tcp_message},
    AnnaError, ClientKey, Key, ALL_TIERS,
};
use eyre::{anyhow, bail, Context};

impl RoutingNode {
    /// Handles incoming replication response messages.
    pub async fn replication_response_handler(&mut self, response: Response) -> eyre::Result<()> {
        // we assume tuple 0 because there should only be one tuple responding to a
        // replication factor request
        let tuple = response
            .tuples
            .get(0)
            .ok_or_else(|| anyhow!("key response has no tuples"))?;

        let key = match &tuple.key {
            Key::Metadata(MetadataKey::Replication { key }) => key.clone(),
            _ => {
                bail!("replication response tuple does not contain a replication metadata key")
            }
        };

        match tuple.error {
            None => {
                let lww_value = tuple
                    .lattice
                    .as_ref()
                    .ok_or_else(|| anyhow!("lattice was none in replication response"))?
                    .as_lww()?;

                let rep_data: ReplicationFactor =
                    serde_json::from_slice(lww_value.reveal().value().as_slice())
                        .context("failed to deserialize ReplicationFactor")?;

                for global in &rep_data.global {
                    self.key_replication_map
                        .entry(key.clone())
                        .or_default()
                        .global_replication
                        .insert(global.tier, global.value);
                }

                for local in &rep_data.local {
                    self.key_replication_map
                        .entry(key.clone())
                        .or_default()
                        .local_replication
                        .insert(local.tier, local.value);
                }
            }
            Some(AnnaError::KeyDoesNotExist) => {
                // this means that the receiving thread was responsible for the metadata
                // but didn't have any values stored -- we use the default rep factor
                self.init_replication(key.clone());
            }
            Some(AnnaError::WrongThread) => {
                // this means that the node that received the rep factor request was not
                // responsible for that metadata
                let respond_address = self.rt.replication_response_topic(&self.zenoh_prefix);
                self.hash_ring_util
                    .issue_replication_factor_request(
                        respond_address,
                        key.clone(),
                        &self.global_hash_rings[&Tier::Memory],
                        &self.local_hash_rings[&Tier::Memory],
                        &self.zenoh,
                        &self.zenoh_prefix,
                        &mut self.node_connections,
                    )
                    .await?;
                return Ok(());
            }
            Some(error) => bail!(
                "Unexpected error type `{:?}` in replication factor response.",
                error
            ),
        }

        // process pending key address requests
        let Self {
            pending_requests,
            node_sockets,
            ..
        } = self;
        if let Some(key_pending_requests) = pending_requests.get_mut(&key) {
            let mut threads = Vec::new();

            for &tier in ALL_TIERS {
                let threads_option = self
                    .hash_ring_util
                    .try_get_responsible_threads(
                        self.rt.replication_response_topic(&self.zenoh_prefix),
                        key.clone().into(),
                        &self.global_hash_rings,
                        &self.local_hash_rings,
                        &self.key_replication_map,
                        &[tier],
                        &self.zenoh,
                        &self.zenoh_prefix,
                        &mut self.node_connections,
                    )
                    .await
                    .context("failed to get responsible threads")?;

                if let Some(t) = threads_option {
                    if !t.is_empty() {
                        threads = t;
                        break;
                    }
                } else {
                    bail!("Missing replication factor for key {}.", key);
                }
            }

            for pending_key_req in key_pending_requests {
                let tcp_sockets = threads
                    .iter()
                    .filter_map(|t| node_sockets.get(t).map(|&s| (t.clone(), s)))
                    .collect();
                let key_res = AddressResponse {
                    addresses: vec![KeyAddress {
                        key: key.clone(),
                        nodes: threads.clone(),
                    }],
                    response_id: pending_key_req.request_id.clone(),
                    error: None,
                    tcp_sockets,
                };

                if let Some(reply_stream) = &mut pending_key_req.reply_stream {
                    send_tcp_message(&TcpMessage::AddressResponse(key_res), reply_stream)
                        .await
                        .context("failed to send reply via TCP")?;
                } else {
                    let serialized = serde_json::to_string(&key_res)
                        .context("failed to serialize KeyAddressResponse")?;
                    self.zenoh
                        .put(&pending_key_req.reply_path.clone(), serialized)
                        .await
                        .map_err(|e| eyre::eyre!(e))
                        .context("failed to send key address response")?;
                }
            }

            self.pending_requests.remove(&key);
        }
        Ok(())
    }

    fn init_replication(&mut self, key: ClientKey) {
        let entry = self.key_replication_map.entry(key).or_default();
        for &tier in ALL_TIERS {
            entry.global_replication.insert(
                tier,
                self.config_data.tier_metadata[&tier].default_replication,
            );
            entry
                .local_replication
                .insert(tier, self.config_data.default_local_replication);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice},
        messages::{
            replication_factor::{ReplicationFactor, ReplicationValue},
            response::{ResponseTuple, ResponseType},
            Response, Tier,
        },
        metadata::MetadataKey,
        nodes::routing::router_test_instance,
        store::LatticeValue,
        zenoh_test_instance, ClientKey, ALL_TIERS,
    };

    #[test]
    fn replication_response() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();

        let key: ClientKey = "key".into();

        let mut router = router_test_instance(zenoh.clone(), zenoh_prefix);
        let entry = router.key_replication_map.entry(key.clone()).or_default();

        entry.global_replication.insert(Tier::Memory, 1);
        entry.global_replication.insert(Tier::Disk, 1);
        entry.local_replication.insert(Tier::Memory, 1);
        entry.local_replication.insert(Tier::Disk, 1);

        let mut response = Response {
            ty: ResponseType::Put,
            tuples: Default::default(),
            response_id: Default::default(),
            error: Ok(()),
        };
        let mut tp = ResponseTuple {
            key: MetadataKey::Replication { key: key.clone() }.into(),
            lattice: None,
            error: None,
            invalidate: false,
        };

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

        let repfactor = serde_json::to_vec(&rf).expect("failed to serialize ReplicationFactor");

        tp.lattice = Some(LatticeValue::Lww(LastWriterWinsLattice::from_pair(
            Timestamp::now(),
            repfactor,
        )));
        response.tuples.push(tp);

        smol::block_on(router.replication_response_handler(response)).unwrap();

        assert_eq!(
            router.key_replication_map[&key].global_replication[&Tier::Memory],
            2,
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
