use std::convert::TryInto;

use crate::{
    messages::{AddressRequest, AddressResponse, KeyAddress, TcpMessage},
    nodes::{routing::RoutingNode, send_tcp_message},
    AnnaError, ALL_TIERS,
};
use eyre::Context;
use smol::net::TcpStream;

impl RoutingNode {
    /// Handles incoming [`AddressRequest`] messages.
    pub async fn address_handler(
        &mut self,
        addr_request: AddressRequest,
        reply_stream: Option<TcpStream>,
    ) -> eyre::Result<()> {
        let mut num_servers = 0;
        for ring in self.global_hash_rings.values() {
            num_servers += ring.len();
        }

        let mut addresses = Vec::new();
        let mut tcp_sockets = Vec::new();
        let (respond, error) = if num_servers == 0 {
            for key in &addr_request.keys {
                let addr = KeyAddress {
                    key: key.clone(),
                    nodes: Vec::new(),
                };
                addresses.push(addr);
            }

            (true, Some(AnnaError::NoServers))
        } else {
            let mut respond = false;
            // if there are servers, attempt to return the correct threads
            for key in &addr_request.keys {
                let mut threads = Default::default();

                if !key.is_empty() {
                    // Only run this code is the key is a valid string.
                    // Otherwise, an empty response will be sent.
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
                            // this means we don't have the replication factor for
                            // the key
                            self.pending_requests.entry(key.clone()).or_default().push(
                                crate::nodes::routing::PendingRequest {
                                    request_id: addr_request.request_id,
                                    reply_path: addr_request
                                        .response_address
                                        .try_into()
                                        .context("invalid response address")?,
                                    reply_stream,
                                },
                            );
                            return Ok(());
                        }
                    }
                }

                let mut addr = KeyAddress {
                    key: key.clone(),
                    nodes: Vec::new(),
                };
                for thread in threads {
                    addr.nodes.push(thread.clone());
                    if let Some(&socket) = self.node_sockets.get(&thread) {
                        tcp_sockets.push((thread.clone(), socket));
                    }
                }
                respond = true;
                addresses.push(addr);
            }
            (respond, None)
        };

        if respond {
            let addr_response = AddressResponse {
                response_id: addr_request.request_id.clone(),
                addresses,
                error,
                tcp_sockets,
            };

            if let Some(mut reply_stream) = reply_stream {
                send_tcp_message(
                    &TcpMessage::AddressResponse(addr_response),
                    &mut reply_stream,
                )
                .await
                .context("failed to send reply via TCP")?;
            } else {
                let serialized_reply = serde_json::to_string(&addr_response)
                    .context("failed to serialize KeyAddressResponse")?;
                self.zenoh
                    .put(&addr_request.response_address, serialized_reply)
                    .await
                    .map_err(|e| eyre::eyre!(e))
                    .context("failed to send reply")?;
            }
        } else {
            log::error!("[address_handler] respond=false");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zenoh::prelude::{Receiver, ZFuture};

    use crate::{
        messages::{AddressRequest, AddressResponse, Tier},
        nodes::routing::router_test_instance,
        topics::KvsThread,
        zenoh_test_instance, ZenohValueAsString,
    };
    use std::time::Duration;

    #[test]
    fn address() {
        let zenoh = zenoh_test_instance();
        let zenoh_prefix = uuid::Uuid::new_v4().to_string();
        let mut subscriber = zenoh
            .subscribe(format!("{}/**", zenoh_prefix))
            .wait()
            .unwrap();

        let key = "key";

        let mut router = router_test_instance(zenoh.clone(), zenoh_prefix.clone());
        router.key_replication_map.entry(key.into()).or_default();

        assert_eq!(router.global_hash_rings[&Tier::Memory].len(), 3000);

        let req = AddressRequest {
            request_id: "1".into(),
            response_address: format!("{}/address_test_response_topic", &zenoh_prefix),
            keys: vec![key.into()],
        };

        smol::block_on(router.address_handler(req, None)).unwrap();

        let message = subscriber
            .receiver()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let resp: AddressResponse =
            serde_json::from_str(&message.value.as_string().unwrap()).unwrap();

        assert_eq!(resp.response_id, "1");
        assert_eq!(resp.error, None);

        for addr in resp.addresses {
            let key = addr.key;
            assert_eq!(key.as_str(), "key");
            for ip in addr.nodes {
                assert_eq!(ip, KvsThread::new("router_id".into(), 0));
            }
        }
    }
}
