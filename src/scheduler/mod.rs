use crate::calvinite_tonic::{RunStmtRequestWithUuid, RunStmtResponse};
use crate::common::Record;
use crate::stmt_analyzer::SqlStmt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::sync;
use uuid::Uuid;
use crate::executor::Executor;
use crate::scheduler::lock_manager::LockManager;
use crate::stmt_analyzer;

pub mod lock_manager;

#[derive(thiserror::Error, Debug, Clone)]
pub enum SchedulerErr {}

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

#[cfg_attr(test, faux::create)]
#[derive(Debug, Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<SchedulerData>>,
    executor: Executor,
}

#[cfg_attr(test, faux::methods)]
impl Default for Scheduler {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SchedulerData::default())),
            executor: Executor::default(),
        }
    }
}

#[cfg_attr(test, faux::methods)]
impl Scheduler {
    pub fn new(executor: Executor) -> Self {
        let inner = Arc::new(Mutex::new(SchedulerData::default()));
        Self { inner, executor }
    }

    // Submits a txn for execution. Txn will be run when it is safe. Returns result of txn.
    pub async fn submit_txn(&self, req: RunStmtRequestWithUuid) -> Result<RunStmtResponse, SchedulerErr> {
        let txn_uuid = Uuid::parse_str(&req.uuid).unwrap();
        let (sender, receiver) = sync::oneshot::channel();

        let sql_stmt = stmt_analyzer::SqlStmt::from_string(req.query.clone()).unwrap();
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

        let _ = receiver.await.unwrap();

        let res = self.executor.execute(req).await.unwrap();

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

#[cfg(test)]
mod tests {
    use faux::when;
    use crate::calvinite_tonic::run_stmt_response::Result::Success;
    use crate::calvinite_tonic::{
        RunStmtRequest, RecordStorage, RunStmtRequestWithUuid, RunStmtResponse, RunStmtResults,
    };
    use crate::executor::Executor;
    use crate::scheduler::Scheduler;

    #[tokio::test]
    async fn scheduler_executes_single_stmt() {
        let mut executor = Executor::faux();

        let txn_uuid = uuid::Uuid::new_v4().to_string();

        let req = RunStmtRequestWithUuid {
            query: "".to_string(),
            uuid: txn_uuid.clone(),
        };

        when!(executor.execute).then_return(Ok(RunStmtResponse {
            result: Some(Success(RunStmtResults {
                uuid: txn_uuid.clone(),
                results: vec![],
            })),
        }));

        let scheduler = Scheduler::new(executor);

        let res = scheduler.submit_txn(req).await.unwrap();

        if let Some(Success(result)) = res.result {
            assert_eq!(result.results, vec![]);
        } else {
            panic!("Results were supposed to be successful")
        }
    }
}