use calvinite::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
use calvinite::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
use calvinite::calvinite_tonic::{RecordStorage, RunStmtRequest};
use calvinite::executor::ExecutorService;
use calvinite::scheduler::SchedulerService;
use calvinite::sequencer::SequencerService;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tonic::transport::Server;
use tonic::Request;

mod common;

#[tokio::test]
async fn test_e2e() {
    let calvinite = common::CalvinInstance::new().await;

    let mut sequencer_client = calvinite.create_client().await;

    let run_stmt1_request = Request::new(RunStmtRequest {
        query: "INSERT INTO foo VALUES (1, 2)".into(),
    });

    let response_1 = sequencer_client.run_stmt(run_stmt1_request).await.unwrap();
    assert_eq!(response_1.into_inner().results, Vec::new());

    let run_stmt2_request = Request::new(RunStmtRequest {
        query: "SELECT * FROM foo WHERE id = 1".into(),
    });

    let response_2 = sequencer_client.run_stmt(run_stmt2_request).await.unwrap();
    assert_eq!(
        response_2.into_inner().results,
        vec![RecordStorage { val: 2 }]
    );
}
