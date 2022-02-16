use crate::common::Record;
use crate::lock_manager::*;
use crate::sequencer::calvinite::{RunStmtRequest, RunStmtRequestWithUuid};
use crate::stmt_analyzer;
use crate::stmt_analyzer::*;
use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct SchedulerService {
    sequenced_queries_channel: mpsc::Receiver<RunStmtRequestWithUuid>,
    scheduled_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
    pending_txns: HashMap<Uuid, RunStmtRequestWithUuid>,
    lock_manager: LockManager<Record>,
}

impl SchedulerService {
    pub fn new(
        sequenced_queries_channel: mpsc::Receiver<RunStmtRequestWithUuid>,
        scheduled_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
    ) -> Self {
        Self {
            sequenced_queries_channel,
            scheduled_queries_channel,
            pending_txns: HashMap::new(),
            lock_manager: LockManager::new(),
        }
    }

    pub async fn serve(&mut self) -> anyhow::Result<()> {
        while let Some(req) = self.sequenced_queries_channel.recv().await {
            let txn_uuid = Uuid::parse_str(&req.uuid)?;

            self.pending_txns.insert(txn_uuid.clone(), req.clone());

            let impacted_records = stmt_analyzer::get_impacted_records(&req.query)?;
            dbg!(
                "Impacted Records of {:?} <-> {:?} are {:?}",
                txn_uuid.clone(),
                req.query.clone(),
                impacted_records.clone()
            );
            self.lock_manager.put_txn(txn_uuid, impacted_records);

            self.schedule_ready_txns().await?;
        }
        Ok(())
    }

    async fn schedule_ready_txns(&mut self) -> anyhow::Result<()> {
        for ready_txn in self.lock_manager.pop_ready_txns().into_iter() {
            let txn_for_uuid = self.pending_txns.remove(&ready_txn).unwrap();

            self.scheduled_queries_channel.send(txn_for_uuid).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduler::SchedulerService;
    use crate::sequencer::calvinite::RunStmtRequestWithUuid;
    use sqlparser::ast::DataType::Uuid;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn serve_schedules_first_txn() {
        let (sequenced_queries_channel_tx, mut sequenced_queries_channel_rx) = mpsc::channel(32);
        let (scheduled_queries_channel_tx, mut scheduled_queries_channel_rx) = mpsc::channel(32);

        let mut ss =
            SchedulerService::new(sequenced_queries_channel_rx, scheduled_queries_channel_tx);

        tokio::spawn(async move {
            ss.serve().await.unwrap();
        });

        let stmt_uuid = uuid::Uuid::new_v4();

        let stmt = RunStmtRequestWithUuid {
            query: "INSERT INTO foo VALUES (1)".into(),
            uuid: stmt_uuid.to_string(),
        };

        sequenced_queries_channel_tx
            .send(stmt.clone())
            .await
            .unwrap();

        let scheduled_query = scheduled_queries_channel_rx.recv().await.unwrap();
        assert_eq!(stmt, scheduled_query);
    }

    // #[test]
    // fn get_impacted_records_for_insert() {
    //     let stmt = "INSERT INTO foo VALUES (1)";
    //
    //     assert_eq!(get_impacted_records(stmt).unwrap(), vec![Record { id: 1 }])
    // }
}
