use calvinite::calvinite_tonic::RunStmtRequestWithUuid;
use calvinite::scheduler::SchedulerService;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_e2e() {
    // Setup Scheduler
    let (sequenced_queries_channel_tx, mut sequenced_queries_channel_rx) = mpsc::channel(32);
    let (scheduled_queries_channel_tx, mut scheduled_queries_channel_rx) = mpsc::channel(32);
    let (completed_queries_channel_tx, mut completed_queries_channel_rx) = mpsc::channel(32);

    let mut ss = SchedulerService::new(
        sequenced_queries_channel_rx,
        scheduled_queries_channel_tx,
        completed_queries_channel_rx,
    );

    tokio::spawn(async move {
        ss.serve().await.unwrap();
    });

    let stmt_uuid = uuid::Uuid::new_v4();

    let stmt = RunStmtRequestWithUuid {
        query: "INSERT INTO foo VALUES (1, 2)".into(),
        uuid: stmt_uuid.to_string(),
    };

    sequenced_queries_channel_tx
        .send(stmt.clone())
        .await
        .unwrap();

    let scheduled_query = scheduled_queries_channel_rx.recv().await.unwrap();
    assert_eq!(stmt.clone(), scheduled_query);

    completed_queries_channel_tx
        .send(stmt.clone())
        .await
        .unwrap();
}
