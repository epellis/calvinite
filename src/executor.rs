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
    ) -> anyhow::Result<Self> {
        let tmp_dir = tempfile::tempdir()?;
        println!("Creating Sled DB at {}", tmp_dir.path().to_str().unwrap());

        Ok(Self {
            scheduled_queries_channel,
            completed_queries_channel,
            storage: sled::open(tmp_dir.path())?,
        })
    }

    pub async fn serve(&mut self) -> anyhow::Result<()> {
        while let Some(req) = self.scheduled_queries_channel.recv().await {
            // TODO: Spawn thread for this
            let (completed_req, result) = self.execute_request(req).await?;
            dbg!("Result of {:?}", result);
            self.completed_queries_channel.send(completed_req).await?;
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
                ..
            } => Self::execute_update_stmt(record_cache, selection),
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
    ) -> anyhow::Result<Vec<RecordStorage>> {
        Ok(Vec::new())
    }
}
