use std::fmt::Debug;
use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcService;
use crate::calvinite_tonic::{RunStmtRequest, RunStmtRequestWithUuid, RunStmtResponse};

use crate::calvinite_tonic::run_stmt_response::Result::Success;
use std::sync::Arc;
use tokio::sync;
use tonic::Response;
use uuid::Uuid;
use crate::scheduler::Scheduler;

#[derive(Debug)]
pub struct Sequencer<S> {
    scheduler: S,
}

impl<S: Scheduler + Debug + Clone> Sequencer<S> {
    pub fn new(scheduler: S) -> Self {
        Self { scheduler }
    }
}

#[tonic::async_trait]
impl<S: Scheduler + Debug + Clone + Send + Sync + 'static> SequencerGrpcService for Sequencer<S> {
    async fn run_stmt(
        &self,
        request: tonic::Request<RunStmtRequest>,
    ) -> Result<tonic::Response<RunStmtResponse>, tonic::Status> {
        let run_stmt_request = request.into_inner();

        let txn_uuid = Uuid::new_v4().to_string();

        let completed_txn = self.scheduler
            .submit_txn(RunStmtRequestWithUuid {
                query: run_stmt_request.query.clone(),
                uuid: txn_uuid.clone(),
            })
            .await.unwrap();

        Ok(Response::new(completed_txn))
    }
}

#[cfg(test)]
mod tests {
    use crate::calvinite_tonic::run_stmt_response::Result::Success;
    use crate::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
    use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
    use crate::calvinite_tonic::{
        RunStmtRequest, RecordStorage, RunStmtRequestWithUuid, RunStmtResponse, RunStmtResults,
    };
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, mpsc};
    use tonic::transport::Server;
    use tonic::Request;
    use crate::scheduler::Scheduler;
    use crate::sequencer::Sequencer;

    #[derive(Clone, Debug, Default)]
    struct MockScheduler {}

    #[async_trait::async_trait]
    impl Scheduler for MockScheduler {
        async fn submit_txn(&self, req: RunStmtRequestWithUuid) -> anyhow::Result<RunStmtResponse> {
            let txn_uuid = req.uuid.clone();

            let stmt_response = RunStmtResponse {
                result: Some(Success(RunStmtResults {
                    uuid: txn_uuid,
                    results: vec![],
                })),
            };

            Ok(stmt_response)
        }
    }

    #[tokio::test]
    async fn serve() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

        let sequencer_service = Sequencer::new(MockScheduler::default());

        tokio::spawn(async move {
            Server::builder()
                .add_service(SequencerGrpcServiceServer::new(sequencer_service))
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