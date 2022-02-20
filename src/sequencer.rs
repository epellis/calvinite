use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcService;
use crate::calvinite_tonic::{RunStmtRequest, RunStmtRequestWithUuid, RunStmtResponse};

use crate::calvinite_tonic::run_stmt_response::Result::Success;
use std::sync::Arc;
use tokio::sync;
use tokio::sync::mpsc;
use tonic::Response;
use uuid::Uuid;

#[derive(Debug)]
pub struct SequencerService {
    sequenced_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
    completed_queries_channel: Arc<sync::broadcast::Sender<RunStmtResponse>>,
}

impl SequencerService {
    pub fn new(
        sequenced_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
        query_result_channel: Arc<sync::broadcast::Sender<RunStmtResponse>>,
    ) -> Self {
        Self {
            sequenced_queries_channel,
            completed_queries_channel: query_result_channel,
        }
    }
}

#[tonic::async_trait]
impl SequencerGrpcService for SequencerService {
    async fn run_stmt(
        &self,
        request: tonic::Request<RunStmtRequest>,
    ) -> Result<tonic::Response<RunStmtResponse>, tonic::Status> {
        let run_stmt_request = request.into_inner();

        let txn_uuid = Uuid::new_v4().to_string();

        self.sequenced_queries_channel
            .send(RunStmtRequestWithUuid {
                query: run_stmt_request.query.clone(),
                uuid: txn_uuid.clone(),
            })
            .await
            .unwrap();

        let mut completed_queries_rx = self.completed_queries_channel.subscribe();

        while let Ok(completed_query) = completed_queries_rx.recv().await {
            if let Some(Success(ref results)) = completed_query.result {
                if results.uuid == txn_uuid {
                    return Ok(Response::new(completed_query.clone()));
                }
            }
        }
        Err(tonic::Status::new(tonic::Code::Internal, "Unreachable"))
    }
}

#[cfg(test)]
mod tests {
    use crate::calvinite_tonic::run_stmt_response::Result::Success;
    use crate::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
    use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
    use crate::calvinite_tonic::{RunStmtRequest, RunStmtResponse, RunStmtResults};
    use crate::sequencer::SequencerService;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, mpsc};
    use tonic::transport::Server;
    use tonic::Request;

    #[tokio::test]
    async fn serve() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

        let (sequenced_queries_channel_tx, mut sequenced_queries_channel_rx) = mpsc::channel(32);
        let (query_result_channel_tx, _) = broadcast::channel(32);
        let arc_query_result_channel_tx = Arc::new(query_result_channel_tx);

        let sequencer_service = SequencerService::new(
            sequenced_queries_channel_tx,
            arc_query_result_channel_tx.clone(),
        );

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

        // 1: Client issues request
        let run_stmt_response_fut =
            tokio::spawn(async move { sequencer_client.run_stmt(run_stmt_request).await.unwrap() });

        // 2: Request is forwarded to scheduler
        let run_stmt_request_from_channel = sequenced_queries_channel_rx.recv().await.unwrap();
        assert_eq!(
            run_stmt_request_from_channel.query.clone(),
            "SELECT * FROM foo WHERE id = 1;"
        );

        // 3: Executor sends back results
        let mocked_run_stmt_response = RunStmtResponse {
            result: Some(Success(RunStmtResults {
                uuid: run_stmt_request_from_channel.uuid,
                results: Vec::new(),
            })),
        };

        arc_query_result_channel_tx.send(mocked_run_stmt_response.clone());

        let run_stmt_response = run_stmt_response_fut.await.unwrap();

        assert_eq!(run_stmt_response.into_inner(), mocked_run_stmt_response);
    }
}
