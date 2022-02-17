use crate::sequencer::calvinite::RunStmtRequestWithUuid;
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
            let completed_req = self.execute_request(req).await?;
            self.completed_queries_channel.send(completed_req).await?;
        }
        Ok(())
    }

    // TODO: Return request and response
    async fn execute_request(
        &mut self,
        req: RunStmtRequestWithUuid,
    ) -> anyhow::Result<RunStmtRequestWithUuid> {
        Ok(req)
    }
}
