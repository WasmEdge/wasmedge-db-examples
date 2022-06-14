use crate::{
    messages::{request::RequestData, Request, Tier},
    nodes::kvs::{KvsNode, PendingGossip},
};
use eyre::{bail, Context};
use std::{collections::HashMap, time::Instant};

impl KvsNode {
    /// Handles incoming gossip messages.
    pub async fn gossip_handler(&mut self, serialized: &str) -> eyre::Result<()> {
        let work_start = Instant::now();

        let gossip: Request =
            serde_json::from_str(serialized).context("failed to decode key request")?;
        let mut gossip_map: HashMap<_, Vec<_>> = HashMap::new();

        let tuples = match gossip.request {
            RequestData::Put { tuples } => tuples,
            RequestData::Get { .. } => {
                bail!("received gossip request with request type Get")
            }
        };

        for tuple in tuples {
            // first check if the thread is responsible for the key
            let key = tuple.key.clone();
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
                .await?;

            if let Some(threads) = threads {
                if threads.contains(&self.wt) {
                    // this means this worker thread is one of the
                    // responsible threads
                    self.kvs.put(tuple.key, tuple.value)?;
                } else {
                    match &key {
                        crate::Key::Metadata(_) => {
                            // forward the gossip
                            for thread in threads {
                                gossip_map
                                    .entry(thread.gossip_topic(&self.zenoh_prefix))
                                    .or_default()
                                    .push(tuple.clone());
                            }
                        }
                        crate::Key::Client(key) => {
                            self.hash_ring_util
                                .issue_replication_factor_request(
                                    self.wt.replication_response_topic(&self.zenoh_prefix),
                                    key.clone(),
                                    &self.global_hash_rings[&Tier::Memory],
                                    &self.local_hash_rings[&Tier::Memory],
                                    &self.zenoh,
                                    &self.zenoh_prefix,
                                    &mut self.node_connections,
                                )
                                .await?;

                            self.pending_gossip.entry(key.into()).or_default().push(
                                PendingGossip {
                                    lattice_value: tuple.value,
                                },
                            );
                        }
                    }
                }
            } else {
                self.pending_gossip
                    .entry(key)
                    .or_default()
                    .push(PendingGossip {
                        lattice_value: tuple.value,
                    });
            }
        }

        // redirect gossip
        for (address, tuples) in gossip_map {
            let key_request = Request {
                request: RequestData::Put { tuples },
                response_address: Default::default(),
                request_id: Default::default(),
                address_cache_size: Default::default(),
            };
            let serialized =
                serde_json::to_string(&key_request).context("failed to serialize KeyRequest")?;
            self.zenoh
                .put(&address, serialized)
                .await
                .map_err(|e| eyre::eyre!(e))?;
        }

        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 4);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice, Lattice},
        messages::request::PutTuple,
        nodes::kvs::kvs_test_instance,
        store::LatticeValue,
        topics::ClientThread,
        zenoh_test_instance, ClientKey,
    };

    fn put_key_request(
        key: ClientKey,
        lattice_value: LatticeValue,
        node_id: String,
        zenoh_prefix: &str,
    ) -> String {
        let request = Request {
            request: RequestData::Put {
                tuples: vec![PutTuple {
                    key: key.into(),
                    value: lattice_value,
                }],
            },
            response_address: Some(
                ClientThread::new(node_id, 0)
                    .response_topic(zenoh_prefix)
                    .to_string(),
            ),
            request_id: Some("0".to_owned()),
            address_cache_size: Default::default(),
        };

        serde_json::to_string(&request).expect("failed to serialize KeyRequest")
    }

    #[test]
    fn simple_gossip_receive() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();

        let key: ClientKey = "key".into();
        let value = "value".as_bytes().to_owned();

        let put_request = put_key_request(
            key.clone(),
            LatticeValue::Lww(LastWriterWinsLattice::from_pair(
                Timestamp::now(),
                value.clone(),
            )),
            "simple_gossip_receive".to_string(),
            &zenoh_prefix,
        );

        let mut server = kvs_test_instance(zenoh, zenoh_prefix);
        server.key_replication_map.entry(key.clone()).or_default();

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.gossip_handler(&put_request)).unwrap();

        assert_eq!(server.pending_gossip.len(), 0);
        let lattice = server.kvs.get(&key.into()).unwrap().as_lww().unwrap();
        assert_eq!(lattice.reveal().value(), &value);
    }

    #[test]
    fn gossip_update() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();

        let key: ClientKey = "key".into();
        let mut value = "value1".as_bytes().to_owned();

        let mut server = kvs_test_instance(zenoh.clone(), zenoh_prefix.clone());
        server.key_replication_map.entry(key.clone()).or_default();
        server
            .kvs
            .put(
                key.clone().into(),
                LatticeValue::Lww(LastWriterWinsLattice::from_pair(
                    Timestamp::now(),
                    value.clone(),
                )),
            )
            .unwrap();

        value = "value2".as_bytes().to_owned();
        let put_request = put_key_request(
            key.clone(),
            LatticeValue::Lww(LastWriterWinsLattice::from_pair(
                Timestamp::now(),
                value.clone(),
            )),
            "gossip_update".to_string(),
            &zenoh_prefix,
        );

        assert_eq!(server.local_changeset.len(), 0);

        smol::block_on(server.gossip_handler(&put_request)).unwrap();

        assert_eq!(server.pending_gossip.len(), 0);
        let lattice = server.kvs.get(&key.into()).unwrap().as_lww().unwrap();
        assert_eq!(
            String::from_utf8(lattice.reveal().value().clone()).unwrap(),
            String::from_utf8(value).unwrap()
        );
    }

    // TODO: test pending gossip
    // TODO: test gossip forwarding
}
