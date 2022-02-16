pub mod calvinite {
    tonic::include_proto!("calvinite"); // The string specified here must match the proto package name
}

use calvinite::sequencer_grpc_service_client::SequencerGrpcServiceClient;
use calvinite::sequencer_grpc_service_server::{SequencerGrpcService, SequencerGrpcServiceServer};
use calvinite::{RunStmtRequest, RunStmtResponse};
use tokio::sync::mpsc;
use tonic::Response;

#[derive(Debug)]
pub struct SequencerService {
    sequenced_queries_channel: mpsc::Sender<RunStmtRequest>,
}

impl SequencerService {
    pub fn new(sequenced_queries_channel: mpsc::Sender<RunStmtRequest>) -> Self {
        Self {
            sequenced_queries_channel,
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

        self.sequenced_queries_channel
            .send(run_stmt_request.clone())
            .await
            .unwrap();

        let response = RunStmtResponse {
            result: run_stmt_request.query,
        };

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use crate::sequencer::calvinite::sequencer_grpc_service_client::SequencerGrpcServiceClient;
    use crate::sequencer::calvinite::sequencer_grpc_service_server::SequencerGrpcServiceServer;
    use crate::sequencer::calvinite::RunStmtRequest;
    use crate::sequencer::SequencerService;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tonic::transport::Server;
    use tonic::Request;

    #[tokio::test]
    async fn serve() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

        let (tx, mut rx) = mpsc::channel(32);

        let sequencer_service = SequencerService::new(tx);

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
            query: "SELECT 1 + 1;".into(),
        });

        let run_stmt_response = sequencer_client.run_stmt(run_stmt_request).await.unwrap();
        let run_stmt_request_from_channel = rx.recv().await.unwrap();

        assert_eq!(run_stmt_response.into_inner().result, "SELECT 1 + 1;");
        assert_eq!(run_stmt_request_from_channel.query, "SELECT 1 + 1;");
    }
}
