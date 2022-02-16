use crate::common::Record;
use crate::lock_manager::*;
use crate::sequencer::calvinite::RunStmtRequest;
use tokio::sync::mpsc;

pub struct SchedulerService {
    sequenced_queries_channel: mpsc::Receiver<RunStmtRequest>,
    lock_manager: LockManager<Record>,
}

impl SchedulerService {
    pub fn new(sequenced_queries_channel: mpsc::Receiver<RunStmtRequest>) -> Self {
        Self {
            sequenced_queries_channel,
            lock_manager: LockManager::new(),
        }
    }

    pub async fn serve(&mut self) -> anyhow::Result<()> {
        while let Some(req) = self.sequenced_queries_channel.recv().await {
            dbg!("Got: {}", req);
        }
        Ok(())
    }
}
