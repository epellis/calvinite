use std::collections::{HashMap, HashSet};

use std::hash::Hash;

use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct LockManager<R> {
    ordered_pending_txns_for_record_lock: HashMap<R, Vec<Uuid>>,
    pending_record_locks_for_txn: HashMap<Uuid, HashSet<R>>,
    all_record_locks_for_txn: HashMap<Uuid, Vec<R>>,
}

impl<R: Hash + Eq + Clone> LockManager<R> {
    pub fn new() -> Self {
        Self {
            ordered_pending_txns_for_record_lock: HashMap::new(),
            pending_record_locks_for_txn: HashMap::new(),
            all_record_locks_for_txn: HashMap::new(),
        }
    }

    pub fn put_txn(&mut self, txn_uuid: Uuid, record_locks: Vec<R>) {
        // Store all held record locks for this txn so we know what to free when txn completes
        self.all_record_locks_for_txn
            .insert(txn_uuid, record_locks.clone());

        // Add this txn as pending to all impacted record locks
        for record_lock in record_locks.clone().into_iter() {
            let mut pending_txn_for_record_lock = self
                .ordered_pending_txns_for_record_lock
                .remove(&record_lock).unwrap_or_default();

            pending_txn_for_record_lock.push(txn_uuid);

            self.ordered_pending_txns_for_record_lock
                .insert(record_lock, pending_txn_for_record_lock);
        }

        // Store a list of impacted record locks that are already held and we'll need before we can start this txn
        let pending_record_locks_for_txn: Vec<R> = record_locks
            
            .into_iter()
            .filter(|record_lock| {
                let pending_txns_for_record_lock = self
                    .ordered_pending_txns_for_record_lock
                    .get(record_lock)
                    .unwrap();

                assert_eq!(pending_txns_for_record_lock.last().unwrap(), &txn_uuid);

                *pending_txns_for_record_lock.first().unwrap() != txn_uuid
            })
            .collect();

        self.pending_record_locks_for_txn.insert(
            txn_uuid,
            HashSet::from_iter(pending_record_locks_for_txn.iter().cloned()),
        );
    }

    pub fn pop_ready_txns(&mut self) -> Vec<Uuid> {
        let ready_txns: Vec<Uuid> = self
            .pending_record_locks_for_txn
            .iter()
            .filter(|(_, record_locks)| record_locks.is_empty())
            .map(|(uuid, _)| *uuid)
            .collect();

        for ready_txn_uuid in ready_txns.iter() {
            self.pending_record_locks_for_txn.remove(ready_txn_uuid);
        }

        ready_txns
    }

    pub fn complete_txn(&mut self, uuid: Uuid) {
        let record_locks_held_by_txn = self.all_record_locks_for_txn.remove(&uuid).unwrap();

        for record_lock in record_locks_held_by_txn.iter() {
            let mut pending_txns_for_record_lock = self
                .ordered_pending_txns_for_record_lock
                .remove(record_lock).unwrap_or_default();

            // Invariant: the first txn for this lock should always be this txn
            assert_eq!(pending_txns_for_record_lock[0], uuid);

            pending_txns_for_record_lock.remove(0);

            // If another txn is waiting for the lock, tell it the lock has been acquired
            if !pending_txns_for_record_lock.is_empty() {
                let next_pending_txn_uuid = pending_txns_for_record_lock[0];
                let mut pending_record_locks_for_next_pending_txn = self
                    .pending_record_locks_for_txn
                    .remove(&next_pending_txn_uuid)
                    .unwrap();

                pending_record_locks_for_next_pending_txn.remove(record_lock);
                self.pending_record_locks_for_txn.insert(
                    next_pending_txn_uuid,
                    pending_record_locks_for_next_pending_txn,
                );
            }

            self.ordered_pending_txns_for_record_lock
                .insert(record_lock.clone(), pending_txns_for_record_lock);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::lock_manager::LockManager;
    use uuid::Uuid;

    #[test]
    fn single_txn_proceeds() {
        let mut lm = LockManager::<u32>::new();

        let txn_uuid = Uuid::new_v4();

        lm.put_txn(txn_uuid, vec![0]);

        assert_eq!(lm.pop_ready_txns(), vec![txn_uuid]);
    }

    #[test]
    fn unrelated_txns_proceed() {
        let mut lm = LockManager::<u32>::new();

        let txn1_uuid = Uuid::new_v4();
        let txn2_uuid = Uuid::new_v4();

        lm.put_txn(txn1_uuid, vec![1]);
        lm.put_txn(txn2_uuid, vec![2]);

        assert_eq!(
            lm.pop_ready_txns().sort(),
            vec![txn1_uuid, txn2_uuid].sort()
        );
    }

    #[test]
    fn conflicting_txns_block_last() {
        let mut lm = LockManager::<u32>::new();

        let txn1_uuid = Uuid::new_v4();
        let txn2_uuid = Uuid::new_v4();

        lm.put_txn(txn1_uuid, vec![1]);
        lm.put_txn(txn2_uuid, vec![1]);

        assert_eq!(lm.pop_ready_txns(), vec![txn1_uuid]);

        lm.complete_txn(txn1_uuid);

        assert_eq!(lm.pop_ready_txns(), vec![txn2_uuid]);

        lm.complete_txn(txn2_uuid);

        assert_eq!(lm.pop_ready_txns(), vec![]);
    }
}
