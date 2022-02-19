use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcService;
use crate::calvinite_tonic::{RunStmtRequest, RunStmtRequestWithUuid, RunStmtResponse};
use anyhow::anyhow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync;
use tokio::sync::{mpsc, oneshot};
use tonic::Response;
use uuid::Uuid;

/// Receives all completed transactions and forwards results to callbacks registered by the
/// sequencer.
#[derive(Debug)]
pub struct CompletedQueryNotifierService {
    query_result_channel: Mutex<mpsc::Receiver<RunStmtResponse>>,
    callbacks: Arc<Mutex<HashMap<Uuid, sync::oneshot::Sender<RunStmtResponse>>>>,
}

impl CompletedQueryNotifierService {
    pub fn new(query_result_channel: Mutex<mpsc::Receiver<RunStmtResponse>>) -> Self {
        Self {
            query_result_channel,
            callbacks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        // TODO: Show that this method is only called once instead of needing a mutex
        let mut query_channel = self.query_result_channel.lock().unwrap();

        while let Some(req) = query_channel.recv().await {
            let mut callbacks = self.callbacks.lock().unwrap();

            let txn_uuid = Uuid::parse_str(&req.uuid)?;

            let callback = callbacks
                .remove(&txn_uuid)
                .ok_or(anyhow!("Expected TXN callback"))?;

            callback.send(req).unwrap();
        }
        Ok(())
    }

    pub async fn register_callback(
        &self,
        uuid: Uuid,
        callback: sync::oneshot::Sender<RunStmtResponse>,
    ) {
        let mut callbacks = self.callbacks.lock().unwrap();

        callbacks.insert(uuid, callback);
    }
}

#[derive(Debug)]
pub struct SequencerService {
    sequenced_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
    completed_query_notification_service: Arc<CompletedQueryNotifierService>,
}

impl SequencerService {
    pub fn new(
        sequenced_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
        completed_query_notification_service: Arc<CompletedQueryNotifierService>,
    ) -> Self {
        Self {
            sequenced_queries_channel,
            completed_query_notification_service,
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

        let txn_uuid = Uuid::new_v4();

        let (completed_txn_tx, completed_txn_rx) = oneshot::channel();

        self.completed_query_notification_service
            .register_callback(txn_uuid.clone(), completed_txn_tx)
            .await;

        self.sequenced_queries_channel
            .send(RunStmtRequestWithUuid {
                query: run_stmt_request.query.clone(),
                uuid: Uuid::new_v4().to_string(),
            })
            .await
            .unwrap();

        let completed_txn = completed_txn_rx.await.unwrap();

        Ok(Response::new(completed_txn))
    }
}

#[cfg(test)]
mod tests {
    use crate::calvinite_tonic::sequencer_grpc_service_client::SequencerGrpcServiceClient;
    use crate::calvinite_tonic::sequencer_grpc_service_server::SequencerGrpcServiceServer;
    use crate::calvinite_tonic::RunStmtRequest;
    use crate::sequencer::{CompletedQueryNotifierService, SequencerService};
    use std::sync::{Arc, Mutex};
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

        let (sequenced_queries_channel_tx, mut sequenced_queries_channel_rx) = mpsc::channel(32);
        let (query_result_channel_tx, mut query_result_channel_rx) = mpsc::channel(32);

        let mut completion_notifier_service = Arc::new(CompletedQueryNotifierService::new(
            Mutex::new(query_result_channel_rx),
        ));

        let sequencer_service =
            SequencerService::new(sequenced_queries_channel_tx, completion_notifier_service);

        tokio::spawn(async move {
            completion_notifier_service.serve().await.unwrap();
        });

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
            query: "SELECT * FROM foo WHERE id = 1;".into(),
        });

        let run_stmt_response = sequencer_client.run_stmt(run_stmt_request).await.unwrap();
        let run_stmt_request_from_channel = sequenced_queries_channel_rx.recv().await.unwrap();

        // assert_eq!(run_stmt_response.into_inner().results, "SELECT 1 + 1;");
        // assert_eq!(run_stmt_request_from_channel.query, "SELECT 1 + 1;");
    }
}
