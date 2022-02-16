mod client;
mod common;
pub mod lock_manager;
mod scheduler;
mod sequencer;
mod stmt_analyzer;

// use std::net::SocketAddr;
// use tokio::net::TcpListener;
// use tokio_stream::wrappers::TcpListenerStream;
// use tonic::{Response, transport::Server};
// use tonic::transport::Error;
// use tonic::transport::server::{Router, Unimplemented};
//
// pub mod calvinite {
//     tonic::include_proto!("calvinite"); // The string specified here must match the proto package name
// }
//
// use calvinite::sequencer_grpc_service_server::{SequencerGrpcService, SequencerGrpcServiceServer};
// use calvinite::{RunStmtRequest, RunStmtResponse};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // // Test with `grpcurl -plaintext -import-path ./proto/calvinite -proto calvinite.proto -d '{"name": "Tonic"}' '[::]:50051' helloworld.Greeter/SayHello`
    // // let addr = "[::1]:0".parse()?;
    // let greeter = SequencerServiceServerImpl::default();
    //
    // let listener = TcpListener::bind("127.0.0.1:0").await?;
    // let listener_address = listener.local_addr()?;
    // let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
    //
    // println!("Listening on {}", listener_address);
    //
    // Server::builder()
    //     .add_service(SequencerGrpcServiceServer::new(greeter))
    //     .serve_with_incoming(listener_stream)
    //     .await?;
    //
    Ok(())
}
//
// #[derive(Debug)]
// pub struct SequencerService {
//     pub listener_address: SocketAddr,
//     server: Router<SequencerGrpcServiceServer<SequencerServiceServerImpl>, Unimplemented>,
// }
//
// impl SequencerService {
//     pub async fn start_and_serve() -> anyhow::Result<()> {
//         let listener = TcpListener::bind("127.0.0.1:0").await?;
//         let listener_address = listener.local_addr()?;
//         let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
//
//         let sequencer_service = SequencerServiceServerImpl::default();
//
//         let fut = Server::builder().add_service(SequencerGrpcServiceServer::new(sequencer_service)).serve_with_incoming(listener_stream);
//     }
// }
//
// #[derive(Debug, Default)]
// pub struct SequencerServiceServerImpl {}
//
//
// #[tonic::async_trait]
// impl SequencerGrpcService for SequencerServiceServerImpl {
//     async fn run_stmt(
//         &self,
//         request: tonic::Request<RunStmtRequest>,
//     ) -> Result<tonic::Response<RunStmtResponse>, tonic::Status> {
//         let response = RunStmtResponse {
//             result: request.into_inner().query
//         };
//
//         Ok(Response::new(response))
//     }
// }
