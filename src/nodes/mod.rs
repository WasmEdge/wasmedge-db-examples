//! Abstractions for the node types, including the main types [`KvsNode`], [`RoutingNode`], and [`ClientNode`].

pub use self::client::ClientNode;
use crate::{
    messages::{cluster_membership::ClusterInfo, TcpMessage},
    topics::RoutingThread,
    ZenohValueAsString,
};
use eyre::{bail, Context};
use futures::{AsyncReadExt, StreamExt};
use smol::{io::AsyncWriteExt, net::TcpStream};
use std::{convert::TryInto, time::Duration};

pub mod client;

/// Sends the given message on the given tcp stream.
///
/// TCP messages should only be sent using this method, to ensure that all
/// messages are sent in the same format.
pub async fn send_tcp_message(
    message: &TcpMessage,
    connection: &mut TcpStream,
) -> eyre::Result<()> {
    let serialized = serde_json::to_vec(&message).context("failed to serialize tcp message")?;
    let len = (serialized.len() as u64).to_le_bytes();
    connection
        .write_all(&len)
        .await
        .context("failed to send message length")?;
    connection
        .write_all(&serialized)
        .await
        .context("failed to send message")?;
    Ok(())
}

/// Receives a [`TcpMessage`] from the given stream.
///
/// This function requires that all messages are sent using [`send_tcp_message`],
/// otherwise parsing the messages will fail.
pub async fn receive_tcp_message(
    stream: &mut (impl futures::AsyncRead + Unpin),
) -> eyre::Result<Option<TcpMessage>> {
    const MAX_MSG_LEN: u64 = u32::MAX as u64;

    let mut len_raw = [0; 8];
    if let Err(err) = stream.read_exact(&mut len_raw).await {
        if err.kind() == std::io::ErrorKind::UnexpectedEof
            || err.kind() == std::io::ErrorKind::ConnectionReset
        {
            return Ok(None);
        } else {
            return Err(eyre::Error::new(err).wrap_err("failed to read message length"));
        }
    }
    let len = u64::from_le_bytes(len_raw);

    if len > MAX_MSG_LEN {
        bail!("Message is too long (length: {} bytes)", len);
    }

    let mut buf = vec![0; len.try_into().unwrap()];
    if let Err(err) = stream.read_exact(&mut buf).await {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            log::warn!("receive tcp message failed: {}", err);
            return Ok(None);
        } else {
            return Err(eyre::Error::new(err).wrap_err("failed to read message"));
        }
    }
    serde_json::from_slice(&buf)
        .with_context(|| {
            format!(
                "failed to deserialize message: `{}`",
                String::from_utf8_lossy(&buf)
            )
        })
        .map(Some)
}

/// Request cluster topology from a seed node.
pub async fn request_cluster_info(
    zenoh: &zenoh::Session,
    zenoh_prefix: &str,
) -> eyre::Result<ClusterInfo> {
    let mut i = 0;
    let membership = loop {
        let replies = zenoh
            .get(&RoutingThread::seed_topic(zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to query seed node")?;

        let mut replies = replies.collect::<Vec<_>>().await;
        match replies.as_mut_slice() {
            [] if i < 30 => {
                futures_timer::Delay::new(Duration::from_millis(100 * i)).await;
                i += 1; // retry
            }
            [] => {
                bail!("no replies received from seed node");
            }
            [reply] => {
                // add all the addresses that seed node sent
                break serde_json::from_str(&reply.sample.value.as_string()?)
                    .context("failed to deserialize ClusterMembership")?;
            }
            _ => bail!("multiple replies received from seed node"),
        };
    };
    Ok(membership)
}
