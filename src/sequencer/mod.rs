use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcService;
use crate::calvinite_tonic::{RunStmtRequest, RunStmtRequestWithUuid, RunStmtResponse};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::sync;
use tokio::sync::broadcast::{Receiver, Sender};

use crate::scheduler::Scheduler;

use tonic::Response;
use uuid::Uuid;

#[derive(Debug)]
pub struct Sequencer {
    scheduler: Scheduler,
    global_req_log_rx: Receiver<RunStmtRequestWithUuid>,
    finished_txn_notifier: Arc<Mutex<HashMap<Uuid, sync::oneshot::Sender<RunStmtResponse>>>>,
}

impl Sequencer {
    pub async fn serve(&mut self) {
        loop {
            let req = self.global_req_log_rx.recv().await.unwrap();

            let uuid = Uuid::parse_str(&req.uuid).unwrap();

            let res = self.scheduler.submit_txn(req).await.unwrap();

            // If SequencerServer is local, notify that the txn is complete
            {
                let mut finished_txn_notifier = self.finished_txn_notifier.lock().unwrap();
                if let Some(tx) = finished_txn_notifier.remove(&uuid) {
                    tx.send(res).unwrap();
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct SequencerServer {
    global_req_log_tx: Sender<RunStmtRequestWithUuid>,
    finished_txn_notifier: Arc<Mutex<HashMap<Uuid, sync::oneshot::Sender<RunStmtResponse>>>>,
}

impl SequencerServer {
    pub fn build_default_sequencer(&self) -> Sequencer {
        self.build_sequencer(Scheduler::default())
    }

    pub fn build_sequencer(&self, scheduler: Scheduler) -> Sequencer {
        let global_req_log_rx = self.global_req_log_tx.subscribe();
        let finished_txn_notifier = self.finished_txn_notifier.clone();

        Sequencer {
            scheduler,
            global_req_log_rx,
            finished_txn_notifier,
        }
    }

    pub fn new(global_req_log_tx: Sender<RunStmtRequestWithUuid>) -> Self {
        Self {
            global_req_log_tx,
            finished_txn_notifier: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

impl Default for SequencerServer {
    fn default() -> Self {
        let (global_req_log_tx, _) = sync::broadcast::channel(1);
        Self::new(global_req_log_tx)
    }
}

#[tonic::async_trait]
impl SequencerGrpcService for SequencerServer {
    async fn run_stmt(
        &self,
        request: tonic::Request<RunStmtRequest>,
    ) -> Result<tonic::Response<RunStmtResponse>, tonic::Status> {
        let run_stmt_request = request.into_inner();

        let txn_uuid = Uuid::new_v4();

        let req = RunStmtRequestWithUuid {
            query: run_stmt_request.query.clone(),
            uuid: txn_uuid.to_string().clone(),
        };

        let (finished_txn_tx, finished_txn_rx) = sync::oneshot::channel();

        {
            let mut finished_txn_notifier = self.finished_txn_notifier.lock().unwrap();
            finished_txn_notifier.insert(txn_uuid, finished_txn_tx);
        }

        self.global_req_log_tx.send(req).unwrap();

        let res = finished_txn_rx.await.unwrap();

        Ok(Response::new(res))
    }
}

#[cfg(test)]
mod tests {
    use crate::calvinite_tonic::run_stmt_response::Result::Success;
    use crate::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
    use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
    use crate::calvinite_tonic::{RunStmtRequest, RunStmtResponse, RunStmtResults};
    use crate::scheduler::Scheduler;
    use crate::sequencer::{Sequencer, SequencerServer};
    use faux::when;
    use tokio::net::TcpListener;
    use tonic::transport::Server;
    use tonic::Request;

    #[tokio::test]
    async fn serve() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

        let mut scheduler = Scheduler::faux();

        when!(scheduler.submit_txn).then_return(Ok(RunStmtResponse {
            result: Some(Success(RunStmtResults {
                uuid: uuid::Uuid::new_v4().to_string(),
                results: vec![],
            })),
        }));

        let sequencer_server = SequencerServer::default();
        let mut sequencer = sequencer_server.build_sequencer(scheduler);

        tokio::spawn(async move {
            sequencer.serve().await;
        });

        tokio::spawn(async move {
            Server::builder()
                .add_service(SequencerGrpcServiceServer::new(sequencer_server))
                .serve_with_incoming(listener_stream)
                .await
                .unwrap();
        });

        let mut sequencer_client = SequencerGrpcServiceClient::connect(listener_http_address)
            .await
            .unwrap();

        let run_stmt_request = Request::new(RunStmtRequest {
            query: "SELECT * FROM foo WHERE id = 1;".into(),
        });

        let run_stmt_response = sequencer_client.run_stmt(run_stmt_request).await.unwrap();

        if let Some(Success(result)) = run_stmt_response.into_inner().result {
            assert_eq!(result.results, vec![]);
        } else {
            panic!("Results were supposed to be successful")
        }
    }
}
