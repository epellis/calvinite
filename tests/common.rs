// extern crate core;
//
// use calvinite::calvinite_tonic::run_stmt_response::Result::Success;
// use calvinite::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
// use calvinite::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
// use calvinite::calvinite_tonic::{RecordStorage, RunStmtRequest};
// use calvinite::executor::ExecutorService;
// use calvinite::scheduler::SchedulerService;
// use calvinite::sequencer::SequencerService;
// use std::sync::Arc;
// use tokio::net::TcpListener;
// use tokio::sync::{broadcast, mpsc};
// use tokio::task::JoinHandle;
// use tonic::transport::{Channel, Server};
// use tonic::Request;
//
// pub struct CalvinSingleInstance {
//     sequencer_thread: JoinHandle<()>,
//     scheduler_thread: JoinHandle<()>,
//     executor_thread: JoinHandle<()>,
//     client: SequencerGrpcServiceClient<Channel>,
// }
//
// impl CalvinSingleInstance {
//     pub async fn new() -> Self {
//         // Setup Channels
//         let (sequenced_queries_channel_tx, sequenced_queries_channel_rx) = mpsc::channel(32);
//         let (scheduled_queries_channel_tx, scheduled_queries_channel_rx) = mpsc::channel(32);
//         let (completed_queries_channel_tx, completed_queries_channel_rx) = mpsc::channel(32);
//         let (query_result_channel_tx, _) = broadcast::channel(32);
//         let arc_query_result_channel_tx = Arc::new(query_result_channel_tx);
//
//         // Setup Sequencer
//         let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
//         let listener_address = listener.local_addr().unwrap();
//         let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
//
//         let listener_http_address = format!("http://127.0.0.1:{}", listener_address.port());
//
//         let sequencer_service = SequencerService::new(
//             sequenced_queries_channel_tx,
//             arc_query_result_channel_tx.clone(),
//         );
//
//         let sequencer_thread = tokio::spawn(async move {
//             Server::builder()
//                 .add_service(SequencerGrpcServiceServer::new(sequencer_service))
//                 .serve_with_incoming(listener_stream)
//                 .await
//                 .unwrap();
//         });
//
//         // Setup Scheduler
//         let mut ss = SchedulerService::new(
//             sequenced_queries_channel_rx,
//             scheduled_queries_channel_tx,
//             completed_queries_channel_rx,
//         );
//
//         let scheduler_thread = tokio::spawn(async move {
//             ss.serve().await.unwrap();
//         });
//
//         // Setup Executor
//         let mut es = ExecutorService::new(
//             scheduled_queries_channel_rx,
//             completed_queries_channel_tx,
//             arc_query_result_channel_tx,
//         )
//         .unwrap();
//
//         let executor_thread = tokio::spawn(async move {
//             es.serve().await.unwrap();
//         });
//
//         let client = SequencerGrpcServiceClient::connect(listener_http_address.clone())
//             .await
//             .unwrap();
//
//         Self {
//             sequencer_thread,
//             scheduler_thread,
//             executor_thread,
//             client,
//         }
//     }
//
//     pub async fn assert_query(&mut self, query: &str, expected_results: Vec<RecordStorage>) {
//         let req = Request::new(RunStmtRequest {
//             query: query.to_string(),
//         });
//         let res = self.client.run_stmt(req).await.unwrap();
//
//         if let Some(Success(result)) = res.into_inner().result {
//             assert_eq!(result.results, expected_results);
//         } else {
//             panic!("Results were supposed to be successful")
//         }
//     }
// }