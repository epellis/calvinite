use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: u64,
}
