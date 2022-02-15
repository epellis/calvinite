pub mod calvinite {
    tonic::include_proto!("calvinite"); // The string specified here must match the proto package name
}

use tonic::Response;
use calvinite::sequencer_grpc_service_server::{SequencerGrpcService, SequencerGrpcServiceServer};
use calvinite::sequencer_grpc_service_client::{SequencerGrpcServiceClient};
use calvinite::{RunStmtRequest, RunStmtResponse};

#[derive(Debug, Default)]
pub struct SequencerService {}


#[tonic::async_trait]
impl SequencerGrpcService for SequencerService {
    async fn run_stmt(
        &self,
        request: tonic::Request<RunStmtRequest>,
    ) -> Result<tonic::Response<RunStmtResponse>, tonic::Status> {
        let response = RunStmtResponse {
            result: request.into_inner().query
        };

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    // use tokio::net::TcpListener;
    // use tonic::transport::Server;
    // use crate::calvinite::sequencer_grpc_service_client::SequencerGrpcServiceClient;
    // use crate::sequencer::SequencerService;
    // use crate::SequencerGrpcServiceServer;

    use tokio::net::TcpListener;
    use tonic::Request;
    use tonic::transport::Server;
    use crate::sequencer::calvinite::RunStmtRequest;
    use crate::sequencer::calvinite::sequencer_grpc_service_client::SequencerGrpcServiceClient;
    use crate::sequencer::calvinite::sequencer_grpc_service_server::SequencerGrpcServiceServer;
    use crate::sequencer::SequencerService;

    #[tokio::test]
    async fn serve() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

        let sequencer_service = SequencerService::default();

        let sequencer_service_thread = tokio::spawn(async move {
            Server::builder()
                .add_service(SequencerGrpcServiceServer::new(sequencer_service))
                .serve_with_incoming(listener_stream)
                .await.unwrap();
        });

        let mut sequencer_client = SequencerGrpcServiceClient::connect(listener_http_address).await.unwrap();

        let run_stmt_request = Request::new(RunStmtRequest {
            query: "SELECT 1 + 1;".into()
        });

        let run_stmt_response = sequencer_client.run_stmt(run_stmt_request).await.unwrap();

        assert_eq!(run_stmt_response.into_inner().result, "SELECT 1 + 1;")
    }
}
