use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use anna_api::{
    lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice, Lattice},
    ClientKey, LatticeValue,
};
use eyre::{eyre, Context, ContextCompat};
use rand::prelude::IteratorRandom;
use tokio::net::TcpStream;

use crate::{
    messages::{AddressRequest, AddressResponse, Request, Response, TcpMessage},
    nodes::{receive_tcp_message, send_tcp_message},
    topics::{ClientThread, KvsThread, RoutingThread},
};

use super::client_request::ClientRequest;

pub struct ClientNode {
    client_thread: ClientThread,
    routing_threads: Vec<RoutingThread>,
    timeout: Duration,
    next_request_id: u32,
    kvs_tcp_address_cache: HashMap<KvsThread, SocketAddr>,
    key_address_cache: HashMap<ClientKey, HashSet<KvsThread>>,
}

impl ClientNode {
    pub fn new(
        node_id: String,
        thread_id: u32,
        routing_threads: Vec<RoutingThread>,
        timeout: Duration,
    ) -> eyre::Result<Self> {
        let client_thread = ClientThread::new(node_id, thread_id);
        Ok(Self {
            client_thread,
            routing_threads,
            timeout,
            next_request_id: 1,
            kvs_tcp_address_cache: Default::default(),
            key_address_cache: Default::default(),
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

    fn make_address_request(&mut self, key: ClientKey) -> AddressRequest {
        AddressRequest {
            request_id: self.gen_request_id(),
            response_address: self.client_thread.address_response_topic().to_string(),
            keys: vec![key],
        }
    }

    fn make_request(&mut self, key: ClientKey, value: Option<LatticeValue>) -> ClientRequest {
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
        self.routing_threads.iter().choose(&mut rng).cloned()
    }

    async fn send_address_request(
        &mut self,
        request: AddressRequest,
    ) -> eyre::Result<AddressResponse> {
        let routing_thread = self.get_routing_thread().context("no routing threads")?;

        // TODO: reuse connection
        // TODO: connect to selected routing thread
        let stream = TcpStream::connect("127.0.0.1:12340")
            .await
            .context("failed to connect to tcp stream")?;
        stream
            .set_nodelay(true)
            .context("failed to set nodelay for tcpstream")?;
        let (mut receiver, mut sender) = stream.into_split();

        send_tcp_message(&TcpMessage::AddressRequest(request), &mut sender).await?;
        // TODO: async receiving
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

    fn get_kvs_thread(&self, key: &ClientKey) -> Option<KvsThread> {
        let mut rng = rand::thread_rng();
        let addr_set = self.key_address_cache.get(key);
        if let Some(addr_set) = addr_set {
            addr_set.iter().choose(&mut rng).cloned()
        } else {
            None
        }
    }

    async fn send_request(
        &mut self,
        request: ClientRequest, // TODO: may be change to Request
    ) -> eyre::Result<Response> {
        let kvs_thread = self
            .get_kvs_thread(&request.key)
            .context("no kvs threads")?;
        let addr = self
            .kvs_tcp_address_cache
            .get(&kvs_thread)
            .context("no tcp address to kvs thread")?;

        // TODO: reuse connection
        let stream = TcpStream::connect(addr).await?;
        stream
            .set_nodelay(true)
            .context("failed to set nodelay for tcpstream")?;
        let (mut receiver, mut sender) = stream.into_split();

        send_tcp_message(&TcpMessage::Request(request.into()), &mut sender).await?;
        // TODO: async receiving
        let message = receive_tcp_message(&mut receiver).await?;
        let message = message.context("connection closed")?;

        match message {
            TcpMessage::Response(response) => Ok(response),
            _ => Err(eyre!("expected Response")),
        }
    }

    pub async fn put_lww(&mut self, key: ClientKey, value: Vec<u8>) -> eyre::Result<()> {
        // TODO: query key address lazily
        let request = self.make_address_request(key.clone());
        let response = self.send_address_request(request).await?;
        assert!(response.error.is_none());

        self.handle_address_response(response)?;

        let lattice_val = LastWriterWinsLattice::from_pair(Timestamp::now(), value);
        let request = self.make_request(key.clone(), Some(LatticeValue::Lww(lattice_val)));
        let response = self.send_request(request).await?;
        assert!(response.error.is_ok());
        assert!(response.tuples.len() == 1);
        assert!(response.tuples[0].error.is_none());

        Ok(())
    }

    pub async fn get_lww(&mut self, key: ClientKey) -> eyre::Result<Vec<u8>> {
        // TODO: query key address lazily
        let request = self.make_address_request(key.clone());
        let response = self.send_address_request(request).await?;
        assert!(response.error.is_none());

        self.handle_address_response(response)?;

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
