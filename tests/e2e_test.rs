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

#[tokio::test]
async fn test_e2e() {
    // Setup Channels
    let (sequenced_queries_channel_tx, mut sequenced_queries_channel_rx) = mpsc::channel(32);
    let (scheduled_queries_channel_tx, mut scheduled_queries_channel_rx) = mpsc::channel(32);
    let (completed_queries_channel_tx, mut completed_queries_channel_rx) = mpsc::channel(32);
    let (query_result_channel_tx, _) = broadcast::channel(32);
    let arc_query_result_channel_tx = Arc::new(query_result_channel_tx);

    // Setup Sequencer
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_address = listener.local_addr().unwrap();
    let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());

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

    // Setup Scheduler
    let mut ss = SchedulerService::new(
        sequenced_queries_channel_rx,
        scheduled_queries_channel_tx,
        completed_queries_channel_rx,
    );

    tokio::spawn(async move {
        ss.serve().await.unwrap();
    });

    // Setup Executor
    let mut es = ExecutorService::new(
        scheduled_queries_channel_rx,
        completed_queries_channel_tx,
        arc_query_result_channel_tx,
    )
    .unwrap();

    tokio::spawn(async move {
        es.serve().await.unwrap();
    });

    // Setup Client
    let mut sequencer_client = SequencerGrpcServiceClient::connect(listener_http_address)
        .await
        .unwrap();

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
