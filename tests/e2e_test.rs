// use calvinite::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
// use calvinite::executor::ExecutorService;
// use calvinite::scheduler::SchedulerService;
// use calvinite::sequencer::SequencerService;
// use tokio::net::TcpListener;
// use tokio::sync::mpsc;
// use tonic::transport::Server;
//
// #[tokio::test]
// async fn test_e2e() {
//     // Setup Channels
//     let (sequenced_queries_channel_tx, mut sequenced_queries_channel_rx) = mpsc::channel(32);
//     let (scheduled_queries_channel_tx, mut scheduled_queries_channel_rx) = mpsc::channel(32);
//     let (completed_queries_channel_tx, mut completed_queries_channel_rx) = mpsc::channel(32);
//     let (query_result_channel_tx, mut query_result_channel_rx) = mpsc::channel(32);
//
//     // Setup Sequencer
//     let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
//     let listener_address = listener.local_addr().unwrap();
//     let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
//
//     let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());
//
//     let sequencer_service = SequencerService::new(sequenced_queries_channel_tx);
//
//     tokio::spawn(async move {
//         Server::builder()
//             .add_service(SequencerGrpcServiceServer::new(sequencer_service))
//             .serve_with_incoming(listener_stream)
//             .await
//             .unwrap();
//     });
//
//     // Setup Scheduler
//     let mut ss = SchedulerService::new(
//         sequenced_queries_channel_rx,
//         scheduled_queries_channel_tx,
//         completed_queries_channel_rx,
//     );
//
//     tokio::spawn(async move {
//         ss.serve().await.unwrap();
//     });
//
//     // Setup Executor
//     let mut es = ExecutorService::new(
//         scheduled_queries_channel_rx,
//         completed_queries_channel_tx,
//         query_result_channel_tx,
//     )
//     .unwrap();
//
//     tokio::spawn(async move {
//         es.serve().await.unwrap();
//     });
//
//     // let stmt_uuid = uuid::Uuid::new_v4();
//
//     // let stmt = RunStmtRequestWithUuid {
//     //     query: "INSERT INTO foo VALUES (1, 2)".into(),
//     //     uuid: stmt_uuid.to_string(),
//     // };
//     //
//     // sequenced_queries_channel_tx
//     //     .send(stmt.clone())
//     //     .await
//     //     .unwrap();
//     //
//     // let scheduled_query = scheduled_queries_channel_rx.recv().await.unwrap();
//     // assert_eq!(stmt.clone(), scheduled_query);
//     //
//     // completed_queries_channel_tx
//     //     .send(stmt.clone())
//     //     .await
//     //     .unwrap();
// }
