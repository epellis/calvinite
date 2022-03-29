use std::{collections::HashMap, ops::Range};

use crate::common::VirtualNodeSize;
use std::num::Wrapping;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PeerManager {
    pub local_peers: Vec<Peer>, // Dev only!, also this includes yourself
}

impl PeerManager {
    pub fn get_local_peer_ranges(&self) -> HashMap<Peer, Range<Wrapping<VirtualNodeSize>>> {
        let mut sorted_peers = self.local_peers.clone();
        sorted_peers.sort_by_key(|p| p.id.as_u128());

        let mut start = Wrapping::<VirtualNodeSize>::MIN;
        let step_size = Wrapping::<VirtualNodeSize>(
            VirtualNodeSize::MAX / self.local_peers.len() as VirtualNodeSize,
        );

        let mut peer_to_range = HashMap::default();

        let (last_peer, other_peers) = sorted_peers.split_last().unwrap();
        for peer in other_peers.iter().cloned() {
            peer_to_range.insert(
                peer,
                Range {
                    start: start,
                    end: start + step_size,
                },
            );
            start += step_size;
        }

        peer_to_range.insert(
            last_peer.clone(),
            Range {
                start: start,
                end: Wrapping::<VirtualNodeSize>::MIN,
            },
        );

        peer_to_range
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::Wrapping, ops::Range};

    use uuid::Uuid;

    use crate::common::VirtualNodeSize;

    use super::{Peer, PeerManager};

    #[test]
    fn get_local_peer_ranges_1_peer() {
        let pm = PeerManager {
            local_peers: vec![Peer {
                id: Uuid::from_u128(0),
            }],
        };

        let peer_to_range = pm.get_local_peer_ranges();
        assert_eq!(
            peer_to_range,
            [(
                Peer {
                    id: Uuid::from_u128(0)
                },
                Range {
                    start: Wrapping::<VirtualNodeSize>(0),
                    end: Wrapping::<VirtualNodeSize>(0)
                }
            )]
            .iter()
            .cloned()
            .collect()
        )
    }

    #[test]
    fn get_local_peer_ranges_2_peers() {
        let pm = PeerManager {
            local_peers: vec![
                Peer {
                    id: Uuid::from_u128(0),
                },
                Peer {
                    id: Uuid::from_u128(1),
                },
            ],
        };

        let peer_to_range = pm.get_local_peer_ranges();
        assert_eq!(
            peer_to_range,
            [
                (
                    Peer {
                        id: Uuid::from_u128(0)
                    },
                    Range {
                        start: Wrapping::<VirtualNodeSize>(0),
                        end: Wrapping::<VirtualNodeSize>(32767)
                    }
                ),
                (
                    Peer {
                        id: Uuid::from_u128(1)
                    },
                    Range {
                        start: Wrapping::<VirtualNodeSize>(32767),
                        end: Wrapping::<VirtualNodeSize>(0)
                    }
                )
            ]
            .iter()
            .cloned()
            .collect()
        )
    }

    #[test]
    fn get_local_peer_ranges_3_peers() {
        let pm = PeerManager {
            local_peers: vec![
                Peer {
                    id: Uuid::from_u128(0),
                },
                Peer {
                    id: Uuid::from_u128(1),
                },
                Peer {
                    id: Uuid::from_u128(2),
                },
            ],
        };

        let peer_to_range = pm.get_local_peer_ranges();
        assert_eq!(
            peer_to_range,
            [
                (
                    Peer {
                        id: Uuid::from_u128(0)
                    },
                    Range {
                        start: Wrapping::<VirtualNodeSize>(0),
                        end: Wrapping::<VirtualNodeSize>(21845)
                    }
                ),
                (
                    Peer {
                        id: Uuid::from_u128(1)
                    },
                    Range {
                        start: Wrapping::<VirtualNodeSize>(21845),
                        end: Wrapping::<VirtualNodeSize>(43690)
                    }
                ),
                (
                    Peer {
                        id: Uuid::from_u128(2)
                    },
                    Range {
                        start: Wrapping::<VirtualNodeSize>(43690),
                        end: Wrapping::<VirtualNodeSize>(0)
                    }
                )
            ]
            .iter()
            .cloned()
            .collect()
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq)]
pub struct Peer {
    pub id: Uuid,
}