use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use anna_api::{
    lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice},
    ClientKey, LatticeValue,
};
use eyre::{Context, ContextCompat};
use rand::prelude::IteratorRandom;
use tokio::net::TcpStream;

use crate::{
    messages::{AddressRequest, TcpMessage},
    nodes::{receive_tcp_message, send_tcp_message},
    topics::{ClientThread, RoutingThread},
};

use super::client_request::ClientRequest;

pub struct ClientNode {
    client_thread: ClientThread,
    routing_threads: Vec<RoutingThread>,
    timeout: Duration,
    next_request_id: u32,
}

impl ClientNode {
    pub fn new(
        node_id: String,
        thread_id: u32,
        routing_threads: Vec<RoutingThread>,
        timeout: Duration,
    ) -> eyre::Result<Self> {
        let ct = ClientThread::new(node_id, thread_id);
        Ok(Self {
            client_thread: ct,
            routing_threads,
            timeout,
            next_request_id: 1,
        })
    }

    fn gen_request_id(&mut self) -> String {
        let id = format!(
            "{}:{}_{}",
            self.client_thread.node_id, self.client_thread.thread_id, self.next_request_id
        );
        self.next_request_id = (self.next_request_id + 1) % 10000;
        id
    }

    fn get_routing_thread(&self) -> Option<&RoutingThread> {
        let mut rng = rand::thread_rng();
        self.routing_threads.iter().choose(&mut rng)
    }

    pub async fn put_lww(&mut self, key: ClientKey, value: Vec<u8>) -> eyre::Result<()> {
        let request_id = self.gen_request_id();
        let request = AddressRequest {
            request_id,
            response_address: self.client_thread.address_response_topic().to_string(),
            keys: vec![key.clone()],
        };

        let routing_thread = self
            .get_routing_thread()
            .context("no routing threads")?
            .clone();

        let stream = TcpStream::connect("127.0.0.1:12340")
            .await
            .context("failed to connect to tcp stream")?;
        stream
            .set_nodelay(true)
            .context("failed to set nodelay for tcpstream")?;
        let (mut receiver, mut sender) = stream.into_split();
        send_tcp_message(&TcpMessage::AddressRequest(request), &mut sender).await?;
        let message = receive_tcp_message(&mut receiver).await?;
        let message = message.context("connection closed")?;
        let response = match message {
            TcpMessage::AddressResponse(response) => response,
            _ => panic!("expected address response"),
        };

        println!("[rc] address response: {:?}", response);
        assert!(response.error.is_none());

        let mut kvs_map = HashMap::new();
        for (kvs_thread, addr) in response.tcp_sockets {
            match kvs_map.entry(kvs_thread) {
                std::collections::hash_map::Entry::Occupied(_) => continue,
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let stream = TcpStream::connect(addr).await?;
                    stream
                        .set_nodelay(true)
                        .context("failed to set nodelay for tcpstream")?;
                    entry.insert(stream.into_split());
                }
            }
        }

        let mut key_addresses: HashMap<_, HashSet<_>> = HashMap::new();
        for key_addr in response.addresses {
            let key = key_addr.key;
            for node in key_addr.nodes {
                key_addresses.entry(key.clone()).or_default().insert(node);
            }
        }

        let lattice_val = LastWriterWinsLattice::from_pair(Timestamp::now(), value);
        let lattice = LatticeValue::Lww(lattice_val);

        let request_id = self.gen_request_id();
        let request = ClientRequest {
            key: key.clone(),
            put_value: Some(lattice),
            response_address: self.client_thread.response_topic().to_string(),
            request_id: request_id.clone(),
            address_cache_size: HashMap::new(),
            timestamp: Instant::now(),
        };

        let mut rng = rand::thread_rng();
        let kvs_thread = key_addresses
            .entry(key.clone())
            .or_default()
            .iter()
            .choose(&mut rng)
            .context("no kvs threads")?
            .clone();

        let (ref mut receiver, ref mut sender) = kvs_map
            .get_mut(&kvs_thread)
            .context("no connection to kvs thread")?;
        send_tcp_message(&TcpMessage::Request(request.into()), sender).await?;
        let message = receive_tcp_message(receiver).await?;
        let message = message.context("connection closed")?;
        let response = match message {
            TcpMessage::Response(response) => response,
            _ => panic!("expected response"),
        };

        println!("[rc] response: {:?}", response);

        todo!()
    }

    pub async fn get_lww(&mut self, key: ClientKey) -> eyre::Result<Vec<u8>> {
        todo!()
    }
}
