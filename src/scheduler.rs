use crate::sequencer::calvinite::RunStmtRequest;
use tokio::sync::mpsc;

pub struct SchedulerService {
    sequenced_queries_channel: mpsc::Receiver<RunStmtRequest>,
}
