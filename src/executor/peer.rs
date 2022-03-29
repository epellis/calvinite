use std::{collections::HashMap, ops::Range};

use crate::common::{Record, VirtualNodeType};
use std::num::Wrapping;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq)]
pub struct Peer {
    pub id: Uuid,
}

#[derive(Debug, Clone)]
pub struct PeerManager {
    pub me: Peer,
    pub local_peers: Vec<Peer>, // Dev only!, also this includes yourself
}

impl PeerManager {
    pub fn get_ordered_peers(&self) -> Vec<Peer> {
        let mut sorted_peers = self.local_peers.clone();
        sorted_peers.sort_by_key(|p| p.id.as_u128());
        sorted_peers
    }

    pub fn peer_for_record(&self, record: &Record) -> Peer {
        let ordered_peers = self.get_ordered_peers();
        let virtual_node = record.virtual_node();
        let idx = virtual_node / ordered_peers.len() as VirtualNodeType;
        ordered_peers[idx as usize]
    }
}