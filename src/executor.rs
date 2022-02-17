use crate::common::Record;
use crate::sequencer::calvinite::{RecordStorage, RunStmtRequestWithUuid};
use crate::stmt_analyzer;
use prost::Message;
use tokio::sync::mpsc;

struct ExecutorService {
    scheduled_queries_channel: mpsc::Receiver<RunStmtRequestWithUuid>,
    completed_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
    storage: sled::Db,
}

impl ExecutorService {
    pub fn new(
        scheduled_queries_channel: mpsc::Receiver<RunStmtRequestWithUuid>,
        completed_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
    ) -> anyhow::Result<Self> {
        let tmp_dir = tempfile::tempdir()?;
        println!("Creating Sled DB at {}", tmp_dir.path().to_str().unwrap());

        Ok(Self {
            scheduled_queries_channel,
            completed_queries_channel,
            storage: sled::open(tmp_dir.path())?,
        })
    }

    pub async fn serve(&mut self) -> anyhow::Result<()> {
        while let Some(req) = self.scheduled_queries_channel.recv().await {
            // TODO: Spawn thread for this
            let (completed_req, result) = self.execute_request(req).await?;
            dbg!("Result of {:?}", result);
            self.completed_queries_channel.send(completed_req).await?;
        }
        Ok(())
    }

    async fn execute_request(
        &mut self,
        req: RunStmtRequestWithUuid,
    ) -> anyhow::Result<(RunStmtRequestWithUuid, Vec<Record>)> {
        let sql_stmt = stmt_analyzer::SqlStmt::from_raw_stmt(req.query.clone())?;

        for record in sql_stmt.write_records.iter() {
            let record_proto = RecordStorage { val: record.val };
            self.storage
                .insert(record.id.to_le_bytes(), record_proto.encode_to_vec())?;
        }

        Ok((req, Vec::new()))
    }
}
