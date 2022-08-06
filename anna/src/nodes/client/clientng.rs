//! New generation of client node that expose a GET/PUT-based interface to users.

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

use anna_api::{
    lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice, Lattice},
    ClientKey, LatticeValue,
};
use eyre::{eyre, Context, ContextCompat};
use rand::prelude::IteratorRandom;
use tokio::net::{tcp, TcpStream};

use crate::{
    messages::{AddressRequest, AddressResponse, Response, TcpMessage},
    nodes::{receive_tcp_message, send_tcp_message},
    topics::{ClientThread, KvsThread, RoutingThread},
};

use super::client_request::ClientRequest;

/// Anna client.
pub struct ClientNode {
    client_thread: ClientThread,
    routing_ip: IpAddr,
    routing_port_base: u16,
    routing_threads: Vec<RoutingThread>,
    _timeout: Duration,
    next_request_id: u32,
    kvs_tcp_address_cache: HashMap<KvsThread, SocketAddr>,
    key_address_cache: HashMap<ClientKey, HashSet<KvsThread>>,
    // TODO: change to only write halves, the read halves should be constantly polled in some tasks
    _tcp_connections: HashMap<SocketAddr, (tcp::OwnedReadHalf, tcp::OwnedWriteHalf)>,
}

impl ClientNode {
    /// Create a new client node.
    pub fn new(
        node_id: String,
        thread_id: u32,
        routing_ip: IpAddr,
        routing_port_base: u16,
        routing_threads: Vec<RoutingThread>,
        timeout: Duration,
    ) -> eyre::Result<Self> {
        let client_thread = ClientThread::new(node_id, thread_id);
        Ok(Self {
            client_thread,
            routing_ip,
            routing_port_base,
            routing_threads,
            _timeout: timeout,
            next_request_id: 1,
            kvs_tcp_address_cache: Default::default(),
            key_address_cache: Default::default(),
            _tcp_connections: Default::default(),
        })
    }

    fn gen_request_id(&mut self) -> String {
        let id = format!(
            "{}:{}_{}",
            self.client_thread.node_id, self.client_thread.thread_id, self.next_request_id
        );
        log::trace!("Generated request ID: {}", id);
        self.next_request_id = (self.next_request_id + 1) % 10000;
        id
    }

    fn make_address_request(&mut self, key: ClientKey) -> AddressRequest {
        log::trace!("Making AddressRequest for key: {:?}", key);
        AddressRequest {
            request_id: self.gen_request_id(),
            response_address: self.client_thread.address_response_topic().to_string(),
            keys: vec![key],
        }
    }

    fn make_request(&mut self, key: ClientKey, value: Option<LatticeValue>) -> ClientRequest {
        log::trace!(
            "Making ClientRequest for key: {:?}, value: {:?}",
            key,
            value
        );
        ClientRequest {
            key,
            put_value: value,
            response_address: self.client_thread.response_topic().to_string(),
            request_id: self.gen_request_id(),
            address_cache_size: HashMap::new(),
            timestamp: Instant::now(),
        }
    }

    fn get_routing_thread(&self) -> Option<RoutingThread> {
        let mut rng = rand::thread_rng();
        let thread = self.routing_threads.iter().choose(&mut rng).cloned();
        log::trace!("Selected routing thread: {:?}", thread);
        thread
    }

    async fn get_tcp_connection(
        &mut self,
        addr: SocketAddr,
    ) -> eyre::Result<(tcp::OwnedReadHalf, tcp::OwnedWriteHalf)> {
        log::trace!("Connecting TCP to address: {:?}", addr);
        let stream = TcpStream::connect(addr)
            .await
            .context("failed to connect to tcp stream")?;
        stream
            .set_nodelay(true)
            .context("failed to set nodelay for tcpstream")?;
        // TODO: keep the connection
        Ok(stream.into_split())
    }

    async fn send_address_request(
        &mut self,
        request: AddressRequest,
    ) -> eyre::Result<AddressResponse> {
        let routing_thread = self.get_routing_thread().context("no routing threads")?;

        let (mut receiver, mut sender) = self
            .get_tcp_connection(SocketAddr::new(
                self.routing_ip,
                self.routing_port_base + routing_thread.thread_id as u16,
            ))
            .await?;
        send_tcp_message(&TcpMessage::AddressRequest(request), &mut sender).await?;
        // TODO: async receiving using oneshot channel
        let message = receive_tcp_message(&mut receiver).await?;
        let message = message.context("connection closed")?;

        match message {
            TcpMessage::AddressResponse(response) => Ok(response),
            _ => Err(eyre!("expected AddressResponse")),
        }
    }

    fn handle_address_response(&mut self, response: AddressResponse) -> eyre::Result<()> {
        response
            .tcp_sockets
            .into_iter()
            .for_each(|(kvs_thread, addr)| {
                self.kvs_tcp_address_cache.insert(kvs_thread, addr);
            });

        for key_addr in response.addresses {
            let key = key_addr.key;
            for node in key_addr.nodes {
                self.key_address_cache
                    .entry(key.clone())
                    .or_default()
                    .insert(node);
            }
        }

        Ok(())
    }

    /// Make and send an AddressRequest for the given key,
    /// and update the address cache with the response.
    async fn query_key_address(&mut self, key: &ClientKey) -> eyre::Result<()> {
        log::trace!("Querying address for key: {:?}", key);
        let request = self.make_address_request(key.clone());
        let response = self.send_address_request(request).await?;
        assert!(response.error.is_none()); // TODO: handle the error (cache invalidation, no server, etc.)
        self.handle_address_response(response)?;
        Ok(())
    }

    fn get_kvs_thread_from_cache(&self, key: &ClientKey) -> Option<KvsThread> {
        let mut rng = rand::thread_rng();
        let addr_set = self.key_address_cache.get(key);
        let thread = if let Some(addr_set) = addr_set {
            addr_set.iter().choose(&mut rng).cloned()
        } else {
            None
        };
        log::trace!("Selected KVS thread: {:?}, key: {:?}", thread, key);
        thread
    }

    fn get_key_tcp_address_from_cache(&self, key: &ClientKey) -> Option<SocketAddr> {
        // get a random kvs thread for the key
        let kvs_thread = self.get_kvs_thread_from_cache(key)?;
        // get the tcp address for the kvs thread
        let addr = self.kvs_tcp_address_cache.get(&kvs_thread).cloned();
        log::trace!("Selected KVS tcp address: {:?}, key: {:?}", addr, key);
        addr
    }

    async fn get_key_tcp_address(&mut self, key: &ClientKey) -> eyre::Result<Option<SocketAddr>> {
        let addr = match self.get_key_tcp_address_from_cache(key) {
            addr @ Some(_) => addr, // cache hit
            None => {
                // cache miss
                self.query_key_address(key).await?;
                self.get_key_tcp_address_from_cache(key)
            }
        };
        Ok(addr)
    }

    async fn send_request(&mut self, request: ClientRequest) -> eyre::Result<Response> {
        let addr = self
            .get_key_tcp_address(&request.key)
            .await?
            .context("fail to get tcp address of the kvs thread the key locates")?;
        let (mut receiver, mut sender) = self.get_tcp_connection(addr).await?;

        send_tcp_message(&TcpMessage::Request(request.into()), &mut sender).await?;
        // TODO: async receiving
        let message = receive_tcp_message(&mut receiver).await?;
        let message = message.context("connection closed")?;

        match message {
            TcpMessage::Response(response) => Ok(response),
            _ => Err(eyre!("expected Response")),
        }
    }

    /// Try to put a *last writer wins* value with the given key.
    pub async fn put_lww(&mut self, key: ClientKey, value: Vec<u8>) -> eyre::Result<()> {
        let lattice_val = LastWriterWinsLattice::from_pair(Timestamp::now(), value);
        let request = self.make_request(key.clone(), Some(LatticeValue::Lww(lattice_val)));
        let response = self.send_request(request).await?;
        assert!(response.error.is_ok());
        assert!(response.tuples.len() == 1);
        assert!(response.tuples[0].error.is_none());

        Ok(())
    }

    /// Try to get a *last writer wins* value with the given key.
    pub async fn get_lww(&mut self, key: ClientKey) -> eyre::Result<Vec<u8>> {
        let request = self.make_request(key.clone(), None);
        let response = self.send_request(request).await?;

        // TODO: handle cache invalidation and other special errors
        if response.error.is_err() {
            return Err(response.error.unwrap_err().into());
        }

        let response_tuple = response
            .tuples
            .get(0)
            .cloned()
            .ok_or_else(|| eyre!("response has no tuples"))?;
        if let Some(error) = response_tuple.error {
            Err(error.into())
        } else {
            Ok(response_tuple
                .lattice
                .context("expected lattice value")?
                .into_lww()?
                .into_revealed()
                .into_value())
        }
    }
}
