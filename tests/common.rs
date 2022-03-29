extern crate core;

use core::num;

use calvinite::calvinite_tonic::run_stmt_response::Result::Success;
use calvinite::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
use calvinite::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
use calvinite::calvinite_tonic::{RecordStorage, RunStmtRequest, RunStmtRequestWithUuid};
use calvinite::sequencer::SequencerServer;

use tokio::net::TcpListener;

use tokio::sync;
use tokio::sync::broadcast::Sender;
use tonic::transport::{Channel, Server};
use tonic::Request;

pub struct CalvinSingleInstance {
    client: SequencerGrpcServiceClient<Channel>,
}

impl CalvinSingleInstance {
    pub async fn default() -> Self {
        let (global_req_log_tx, _) = sync::broadcast::channel(1);
        Self::new_with_global_req_log(global_req_log_tx).await
    }

    pub async fn new_with_global_req_log(
        global_req_log_tx: Sender<RunStmtRequestWithUuid>,
    ) -> Self {
        let sequencer_server = SequencerServer::new(global_req_log_tx);
        let mut sequencer = sequencer_server.build_default_sequencer();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

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

pub struct CalvinMultipleInstances {
    pub instances: Vec<CalvinSingleInstance>,
}

impl CalvinMultipleInstances {
    pub async fn new(num_instances: usize) -> Self {
        let (global_req_log_tx, _) = sync::broadcast::channel(1);

        let mut instances = Vec::new();

        for _ in 0..num_instances {
            instances.push(
                CalvinSingleInstance::new_with_global_req_log(global_req_log_tx.clone()).await,
            );
        }

        Self { instances }
    }
}
