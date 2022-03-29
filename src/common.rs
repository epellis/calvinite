use std::mem;

use serde::{Deserialize, Serialize};

pub type VirtualNodeType = u16;
pub const VIRTUAL_NODE_SIZE_BITS: usize = mem::size_of::<VirtualNodeType>();

// TODO: Maybe just make the virtual node
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: u64,
}

impl Record {
    fn virtual_node_byte_arry(&self) -> [u8; VIRTUAL_NODE_SIZE_BITS] {
        let bytes = bincode::serialize(self).unwrap();
        let digest: [u8; 16] = md5::compute(bytes).into();
        digest[..VIRTUAL_NODE_SIZE_BITS].try_into().unwrap()
        // VirtualNodeSize::from_be_bytes(digest);
    }

    pub fn virtual_node(&self) -> VirtualNodeType {
        let bytes = bincode::serialize(self).unwrap();
        let digest: [u8; 16] = md5::compute(bytes).into();
        VirtualNodeType::from_le_bytes(digest[..VIRTUAL_NODE_SIZE_BITS].try_into().unwrap())
    }

    pub fn fully_qualified_id_as_bytes(&self) -> Vec<u8> {
        let virtual_node = self.virtual_node_byte_arry();
        let bytes = bincode::serialize(self).unwrap();
        [virtual_node.to_vec(), bytes].concat()
    }
}