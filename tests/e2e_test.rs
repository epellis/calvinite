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
async fn test_write_then_read() {
    let mut calvinite = common::CalvinSingleInstance::new().await;

    calvinite
        .assert_query("INSERT INTO foo VALUES (1, 2)", Vec::new())
        .await;
    calvinite
        .assert_query(
            "SELECT * FROM foo WHERE id = 1",
            vec![RecordStorage { val: 2 }],
        )
        .await;
}

#[tokio::test]
async fn test_write_then_read_then_read() {
    let mut calvinite = common::CalvinSingleInstance::new().await;

    calvinite
        .assert_query("INSERT INTO foo VALUES (1, 2)", Vec::new())
        .await;
    calvinite
        .assert_query(
            "SELECT * FROM foo WHERE id = 1",
            vec![RecordStorage { val: 2 }],
        )
        .await;
    calvinite
        .assert_query(
            "SELECT * FROM foo WHERE id = 1",
            vec![RecordStorage { val: 2 }],
        )
        .await;
}
