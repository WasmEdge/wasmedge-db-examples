use std::borrow::Cow;

use super::RoutingNode;
use crate::{messages::TcpMessage, nodes::send_tcp_message};
use eyre::{bail, Context};
use smol::net::TcpStream;

mod address;
mod membership;
mod replication_change;
mod replication_response;
mod seed;

impl RoutingNode {
    /// Handles an incoming [`TcpMessage`].
    pub async fn handle_message(
        &mut self,
        message: TcpMessage,
        tcp_stream: Cow<'_, TcpStream>,
    ) -> eyre::Result<()> {
        match message {
            TcpMessage::Ping { payload } => {
                send_tcp_message(&TcpMessage::Pong { payload }, &mut tcp_stream.into_owned())
                    .await
                    .context("failed to send pong reply")?
            }
            TcpMessage::Notify(notify) => self
                .membership_handler(&notify)
                .await
                .context("membership handler failed")?,
            TcpMessage::AddressRequest(request) => self
                .address_handler(request, Some(tcp_stream.into_owned()))
                .await
                .context("address handler failed")?,
            TcpMessage::Response(response) => self
                .replication_response_handler(response)
                .await
                .context("replication response handler failed")?,
            TcpMessage::TcpJoin {
                kvs_thread,
                listen_socket,
            } => {
                if let Some(socket) = listen_socket {
                    self.node_sockets.insert(kvs_thread.clone(), socket);

                    // send socket addr to other KVS nodes if any
                    let message = TcpMessage::KvsSocket {
                        kvs_thread: kvs_thread.clone(),
                        socket,
                    };
                    for s in self.node_connections.values_mut() {
                        send_tcp_message(&message, s)
                            .await
                            .context("failed to forward TcpJoin message")?;
                    }
                }

                self.node_connections
                    .insert(kvs_thread.clone(), tcp_stream.into_owned());
            }
            other => bail!("received unsupported message over tcp: {:?}", other),
        }
        Ok(())
    }
}
