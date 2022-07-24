use std::time::Duration;

use anna_api::ClientKey;

use crate::topics::{ClientThread, RoutingThread};

pub struct ClientNode {
    client_thread: ClientThread,
    routing_threads: Vec<RoutingThread>,
    timeout: Duration,
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
        })
    }

    pub async fn put_lww(&mut self, key: ClientKey, value: Vec<u8>) -> eyre::Result<()> {
        todo!()
    }

    pub async fn get_lww(&mut self, key: ClientKey) -> eyre::Result<Vec<u8>> {
        todo!()
    }
}
