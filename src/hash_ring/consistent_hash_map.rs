use crate::Key;
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
};

pub struct ConsistentHashMap<T> {
    nodes: BTreeMap<u64, T>,
}

impl<T> ConsistentHashMap<T>
where
    T: Hash,
{
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn insert(&mut self, node: T) {
        self.nodes.insert(hash(&node), node);
    }

    pub fn remove(&mut self, node: &T) {
        self.nodes.remove(&hash(node));
    }

    /// Returns an iterator over the map's entries, starting at the hash of the given key.
    pub fn entries_starting_at(&self, key: &Key) -> impl Iterator<Item = &T> {
        let hash = hash(key);
        self.nodes
            .range(hash..)
            .chain(self.nodes.range(..hash))
            .map(|(_k, v)| v)
    }
}

impl<T> Default for ConsistentHashMap<T> {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
        }
    }
}

fn hash<T: Hash + ?Sized>(data: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

#[derive(PartialEq, Eq, Hash)]
pub struct VirtualNode {
    node_id: String,
    virtual_id: u32,
}

impl VirtualNode {
    pub fn new(node_id: String, virtual_id: u32) -> Self {
        Self {
            node_id,
            virtual_id,
        }
    }

    /// Gets the id of the node that this virtual node belongs to.
    pub fn node_id(&self) -> &str {
        self.node_id.as_str()
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct VirtualThread {
    thread_id: u32,
    virtual_id: u32,
}

impl VirtualThread {
    pub fn new(thread_id: u32, virtual_id: u32) -> Self {
        Self {
            thread_id,
            virtual_id,
        }
    }

    /// Get a reference to the virtual thread's thread id.
    pub fn thread_id(&self) -> &u32 {
        &self.thread_id
    }
}
