use crate::calvinite_tonic::{RunStmtRequestWithUuid, RunStmtResponse};
use crate::common::Record;
use crate::stmt_analyzer::SqlStmt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync;

#[derive(Default)]
pub struct Scheduler {
    locks_by_record: Mutex<HashMap<Record, Arc<sync::Mutex<()>>>>,
}

impl Scheduler {
    pub async fn schedule_stmt(
        &self,
        stmt: RunStmtRequestWithUuid,
    ) -> anyhow::Result<RunStmtResponse> {
        let locks_for_txn = self.get_locks_for_txn(stmt)?;

        // Acquire all permits
        let mutex_guards = locks_for_txn.into_iter().map(|l| l.lock().await?);

        Ok(todo!())
    }

    fn get_locks_for_txn(
        &self,
        stmt: RunStmtRequestWithUuid,
    ) -> anyhow::Result<Vec<Arc<sync::Mutex<()>>>> {
        let sql_stmt = SqlStmt::from_string(stmt.query.clone())?;

        let locked_records = [
            &sql_stmt.inserted_records[..],
            &sql_stmt.updated_records[..],
            &sql_stmt.selected_records[..],
        ]
        .concat();

        let locks_for_records: Vec<Arc<sync::Mutex<()>>> = locked_records
            .clone()
            .into_iter()
            .map(|r| self.get_lock(r))
            .collect();

        Ok(locks_for_records)
    }

    fn get_lock(&self, record: Record) -> Arc<sync::Mutex<()>> {
        let mut locks_by_record = self.locks_by_record.lock().unwrap();

        if let Some(lock) = locks_by_record.get(&record) {
            lock.clone()
        } else {
            let lock = Arc::new(sync::Mutex::new(()));
            locks_by_record.insert(record, lock.clone());
            lock
        }
    }
}
