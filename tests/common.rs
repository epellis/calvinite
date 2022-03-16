extern crate core;

use calvinite::calvinite_tonic::run_stmt_response::Result::Success;
use calvinite::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
use calvinite::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
use calvinite::calvinite_tonic::{RecordStorage, RunStmtRequest};
use calvinite::sequencer::Sequencer;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Server};
use tonic::Request;

pub struct CalvinSingleInstance {
    client: SequencerGrpcServiceClient<Channel>,
}

impl CalvinSingleInstance {
    pub async fn new() -> Self {
        let sequencer = Sequencer::default();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

        let sequencer_thread = tokio::spawn(async move {
            Server::builder()
                .add_service(SequencerGrpcServiceServer::new(sequencer))
                .serve_with_incoming(listener_stream)
                .await
                .unwrap();
        });

        let client = SequencerGrpcServiceClient::connect(listener_http_address.clone())
            .await
            .unwrap();

        Self { client }
    }

    pub async fn assert_query(&mut self, query: &str, expected_results: Vec<RecordStorage>) {
        let req = Request::new(RunStmtRequest {
            query: query.to_string(),
        });
        let res = self.client.run_stmt(req).await.unwrap();

        if let Some(Success(result)) = res.into_inner().result {
            assert_eq!(result.results, expected_results);
        } else {
            panic!("Results were supposed to be successful")
        }
    }
}
