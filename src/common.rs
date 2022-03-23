use serde::{Deserialize, Serialize};

const VIRTUAL_NODE_SIZE_BITS: usize = 10;

// TODO: Maybe just make the virtual node 
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: u64,
}

impl Record {
    fn virtual_node(&self) -> [u8; VIRTUAL_NODE_SIZE_BITS] {
        let bytes = bincode::serialize(self).unwrap();
        let digest: [u8; 16] = md5::compute(bytes).into();
        digest[..VIRTUAL_NODE_SIZE_BITS].try_into().unwrap()
    }

    pub fn fully_qualified_id_as_bytes(&self) -> Vec<u8> {
        let virtual_node = self.virtual_node();
        let bytes = bincode::serialize(self).unwrap();
        [virtual_node.to_vec(), bytes].concat()
    }
}
