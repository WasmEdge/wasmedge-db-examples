//! Abstractions for the node types, including the main types [`KvsNode`], [`RoutingNode`], and [`ClientNode`].

pub use self::client::ClientNode;

pub mod client;

use eyre::{bail, Context};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp,
};

use crate::messages::TcpMessage;

/// Sends the given message on the given tcp stream.
///
/// TCP messages should only be sent using this method, to ensure that all
/// messages are sent in the same format.
pub async fn send_tcp_message(
    message: &TcpMessage,
    stream_tx: &mut tcp::OwnedWriteHalf,
) -> eyre::Result<()> {
    let serialized = serde_json::to_vec(&message).context("failed to serialize tcp message")?;
    let len = (serialized.len() as u64).to_le_bytes();
    stream_tx
        .write_all(&len)
        .await
        .context("failed to send message length")?;
    stream_tx
        .write_all(&serialized)
        .await
        .context("failed to send message")?;
    println!("[rc] sent tcp message: {:?}", message);
    Ok(())
}

/// Receives a [`TcpMessage`] from the given stream.
///
/// This function requires that all messages are sent using [`send_tcp_message`],
/// otherwise parsing the messages will fail.
pub async fn receive_tcp_message(
    stream_rx: &mut tcp::OwnedReadHalf,
) -> eyre::Result<Option<TcpMessage>> {
    const MAX_MSG_LEN: u64 = u32::MAX as u64;

    let mut len_raw = [0; 8];
    if let Err(err) = stream_rx.read_exact(&mut len_raw).await {
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
    if let Err(err) = stream_rx.read_exact(&mut buf).await {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            log::warn!("receive tcp message failed: {}", err);
            return Ok(None);
        } else {
            return Err(eyre::Error::new(err).wrap_err("failed to read message"));
        }
    }
    let res = serde_json::from_slice(&buf)
        .with_context(|| {
            format!(
                "failed to deserialize message: `{}`",
                String::from_utf8_lossy(&buf)
            )
        })
        .map(Some);
    println!("[rc] received tcp message: {:?}", res);
    res
}
