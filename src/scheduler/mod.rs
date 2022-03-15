use crate::calvinite_tonic::{RunStmtRequestWithUuid, RunStmtResponse};
use crate::common::Record;
use crate::stmt_analyzer::SqlStmt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::sync;
use uuid::Uuid;
use crate::executor::{Executor, ExecutorService};
use crate::scheduler::lock_manager::LockManager;
use crate::stmt_analyzer;

pub mod lock_manager;

#[derive(Debug)]
struct SchedulerData {
    lock_manager: LockManager<Record>,
    pending_txns: HashMap<Uuid, sync::oneshot::Sender<()>>,
}

impl Default for SchedulerData {
    fn default() -> Self {
        Self { lock_manager: LockManager::new(), pending_txns: HashMap::default() }
    }
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<SchedulerData>>,
    executor: Executor,
}

impl Scheduler {
    pub fn new(executor: Executor) -> Self {
        let inner = Arc::new(Mutex::new(SchedulerData::default()));
        Self { inner, executor }
    }

    // Submits a txn for execution. Txn will be run when it is safe. Returns result of txn.
    pub async fn submit_txn(&self, req: RunStmtRequestWithUuid) -> anyhow::Result<RunStmtResponse> {
        let txn_uuid = Uuid::parse_str(&req.uuid)?;
        let (sender, receiver) = sync::oneshot::channel();

        let sql_stmt = stmt_analyzer::SqlStmt::from_string(req.query.clone())?;
        let impacted_records = sql_stmt.inserted_records;

        dbg!(
            "Impacted Records of {:?} <-> {:?} are {:?}",
            txn_uuid,
            req.query.clone(),
            impacted_records.clone()
        );

        {
            let mut inner = self.inner.lock().unwrap();

            inner.pending_txns.insert(txn_uuid, sender);
            inner.lock_manager.put_txn(txn_uuid, impacted_records);

            let pending_txns = inner.lock_manager.pop_ready_txns();
            for pending_txn in pending_txns {
                let txn_notifier = inner.pending_txns.remove(&pending_txn).unwrap();
                txn_notifier.send(()).unwrap();
            }
        }

        let _ = receiver.await?;

        let res = self.executor.execute(req).await?;

        {
            let mut inner = self.inner.lock().unwrap();

            inner.lock_manager.complete_txn(txn_uuid);
            let pending_txns = inner.lock_manager.pop_ready_txns();
            for pending_txn in pending_txns {
                let txn_notifier = inner.pending_txns.remove(&pending_txn).unwrap();
                txn_notifier.send(()).unwrap();
            }
        }

        Ok(res)
    }
}