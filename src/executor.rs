use crate::common::Record;
use crate::sequencer::calvinite::{RecordStorage, RunStmtRequestWithUuid};
use crate::stmt_analyzer;
use crate::stmt_analyzer::SqlStmt;
use anyhow::anyhow;
use prost::Message;
use sqlparser::ast;
use std::collections::HashMap;
use tokio::sync::mpsc;

struct ExecutorService {
    scheduled_queries_channel: mpsc::Receiver<RunStmtRequestWithUuid>,
    completed_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
    query_result_channel: mpsc::Sender<Vec<RecordStorage>>,
    storage: sled::Db,
}

#[derive(Clone, Debug, Eq, Hash, PartialOrd, PartialEq)]
struct TouchedRecord {
    record: Record,
    is_dirty: bool,
}

impl ExecutorService {
    pub fn new(
        scheduled_queries_channel: mpsc::Receiver<RunStmtRequestWithUuid>,
        completed_queries_channel: mpsc::Sender<RunStmtRequestWithUuid>,
        query_result_channel: mpsc::Sender<Vec<RecordStorage>>,
    ) -> anyhow::Result<Self> {
        let tmp_dir = tempfile::tempdir()?;
        dbg!("Creating Sled DB at {}", tmp_dir.path().to_str().unwrap());

        Ok(Self {
            scheduled_queries_channel,
            completed_queries_channel,
            query_result_channel,
            storage: sled::open(tmp_dir.path())?,
        })
    }

    pub async fn serve(&mut self) -> anyhow::Result<()> {
        while let Some(req) = self.scheduled_queries_channel.recv().await {
            // TODO: Spawn thread for this
            let (completed_req, query_results) = self.execute_request(req).await?;
            self.completed_queries_channel.send(completed_req).await?;
            self.query_result_channel.send(query_results).await?;
        }
        Ok(())
    }

    async fn execute_request(
        &mut self,
        req: RunStmtRequestWithUuid,
    ) -> anyhow::Result<(RunStmtRequestWithUuid, Vec<RecordStorage>)> {
        let sql_stmt = stmt_analyzer::SqlStmt::from_string(req.query.clone())?;

        // Load read and write records into local memory
        let mut record_cache = HashMap::<TouchedRecord, RecordStorage>::new();

        for select_record in sql_stmt.selected_records.iter() {
            let record_bytes = self
                .storage
                .get(bincode::serialize(select_record)?)?
                .ok_or_else(|| anyhow!("no record exists for {}", select_record.id))?;

            let record_bytes_buf = bytes::Bytes::from(record_bytes.to_vec());

            record_cache.insert(
                TouchedRecord {
                    record: select_record.clone(),
                    is_dirty: false,
                },
                RecordStorage::decode(record_bytes_buf)?,
            );
        }

        for update_record in sql_stmt.updated_records.iter() {
            let record_bytes = self
                .storage
                .get(bincode::serialize(update_record)?)?
                .ok_or_else(|| anyhow!("no record exists for {}", update_record.id))?;

            let record_bytes_buf = bytes::Bytes::from(record_bytes.to_vec());

            record_cache.insert(
                TouchedRecord {
                    record: update_record.clone(),
                    is_dirty: false,
                },
                RecordStorage::decode(record_bytes_buf)?,
            );
        }

        dbg!("Record Cache Before Execution: {:?}", record_cache.clone());

        // Execute the query
        let results = Self::execute_stmt(&mut record_cache, sql_stmt.ast_stmts.first().unwrap())?;

        dbg!("Record Cache After Execution: {:?}", record_cache.clone());

        // Flush dirty records
        for (key, value) in record_cache.into_iter() {
            if key.is_dirty {
                self.storage
                    .insert(bincode::serialize(&key.record)?, value.encode_to_vec());
            }
        }

        Ok((req, results))
    }

    fn execute_stmt(
        record_cache: &mut HashMap<TouchedRecord, RecordStorage>,
        stmt: &ast::Statement,
    ) -> anyhow::Result<Vec<RecordStorage>> {
        match stmt {
            ast::Statement::Query(query) => Self::execute_query_stmt(record_cache, query),
            ast::Statement::Insert { source, .. } => {
                Self::execute_insert_stmt(record_cache, source)
            }
            ast::Statement::Update {
                selection: Some(selection),
                assignments,
                ..
            } => Self::execute_update_stmt(record_cache, selection, assignments),
            _ => Ok(Vec::new()),
        }
    }

    fn execute_query_stmt(
        record_cache: &mut HashMap<TouchedRecord, RecordStorage>,
        query: &ast::Query,
    ) -> anyhow::Result<Vec<RecordStorage>> {
        match &query.body {
            ast::SetExpr::Select(select) => match *select.clone() {
                ast::Select {
                    selection: Some(selection),
                    ..
                } => {
                    let record =
                        SqlStmt::find_id_in_expr(&selection).ok_or(anyhow!("Couldn't find ID"))?;
                    let record_value = record_cache
                        .get(&TouchedRecord {
                            record,
                            is_dirty: false,
                        })
                        .ok_or(anyhow!("expected record"))?;
                    Ok(vec![record_value.clone()])
                }
                _ => Ok(Vec::new()),
            },
            _ => Ok(Vec::new()),
        }
    }

    fn execute_insert_stmt(
        record_cache: &mut HashMap<TouchedRecord, RecordStorage>,
        source: &ast::Query,
    ) -> anyhow::Result<Vec<RecordStorage>> {
        match &source.body {
            ast::SetExpr::Values(ast::Values(values)) => {
                // TODO: Parse more than first insert
                let (key_expr, value_expr) = (&values[0][0], &values[0][1]);

                let key = SqlStmt::expr_to_num(key_expr).ok_or(anyhow!("failed to parse key"))?;
                let value =
                    SqlStmt::expr_to_num(value_expr).ok_or(anyhow!("failed to parse value"))?;

                record_cache.insert(
                    TouchedRecord {
                        record: Record { id: key },
                        is_dirty: true,
                    },
                    RecordStorage { val: value },
                );

                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn execute_update_stmt(
        record_cache: &mut HashMap<TouchedRecord, RecordStorage>,
        selection: &ast::Expr,
        assignments: &Vec<ast::Assignment>,
    ) -> anyhow::Result<Vec<RecordStorage>> {
        let record = SqlStmt::find_id_in_expr(selection).ok_or(anyhow!(""))?;

        let value = SqlStmt::expr_to_num(&assignments[0].value).ok_or(anyhow!(""))?;

        record_cache.insert(
            TouchedRecord {
                record,
                is_dirty: true,
            },
            RecordStorage { val: value },
        );

        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::ExecutorService;
    use crate::scheduler::SchedulerService;
    use crate::sequencer::calvinite::{RecordStorage, RunStmtRequestWithUuid};
    use sqlparser::ast::DataType::Uuid;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn executes_write_read() {
        let (scheduled_queries_channel_tx, mut scheduled_queries_channel_rx) = mpsc::channel(32);
        let (completed_queries_channel_tx, mut completed_queries_channel_rx) = mpsc::channel(32);
        let (query_result_channel_tx, mut query_result_channel_rx) = mpsc::channel(32);

        let mut es = ExecutorService::new(
            scheduled_queries_channel_rx,
            completed_queries_channel_tx,
            query_result_channel_tx,
        )
        .unwrap();

        tokio::spawn(async move {
            es.serve().await.unwrap();
        });

        let stmt1_uuid = uuid::Uuid::new_v4();
        let stmt1 = RunStmtRequestWithUuid {
            query: "INSERT INTO foo VALUES (1, 2)".into(),
            uuid: stmt1_uuid.to_string(),
        };

        scheduled_queries_channel_tx
            .send(stmt1.clone())
            .await
            .unwrap();

        let completed_query1 = completed_queries_channel_rx.recv().await.unwrap();
        assert_eq!(stmt1, completed_query1);

        let query_results1 = query_result_channel_rx.recv().await.unwrap();
        assert_eq!(query_results1, Vec::new());

        let stmt2_uuid = uuid::Uuid::new_v4();
        let stmt2 = RunStmtRequestWithUuid {
            query: "SELECT * FROM foo WHERE id = 1".into(),
            uuid: stmt2_uuid.to_string(),
        };

        scheduled_queries_channel_tx
            .send(stmt2.clone())
            .await
            .unwrap();

        let completed_query2 = completed_queries_channel_rx.recv().await.unwrap();
        assert_eq!(stmt2, completed_query2);

        let query_results2 = query_result_channel_rx.recv().await.unwrap();
        assert_eq!(query_results2, vec![RecordStorage { val: 2 }]);
    }
}
