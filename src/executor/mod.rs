use crate::calvinite_tonic::run_stmt_response::Result::Success;
use crate::calvinite_tonic::{
    RecordStorage, RunStmtRequestWithUuid, RunStmtResponse, RunStmtResults,
};
use crate::common::Record;
use crate::stmt_analyzer;
use crate::stmt_analyzer::SqlStmt;
use anyhow::anyhow;
use prost::Message;
use sqlparser::ast;
use std::collections::HashMap;

pub mod peer;

#[derive(thiserror::Error, Debug, Clone)]
pub enum ExecutorErr {}

#[derive(Clone, Debug, Eq, Hash, PartialOrd, PartialEq)]
struct TouchedRecord {
    record: Record,
    is_dirty: bool,
}

#[cfg_attr(test, faux::create)]
#[derive(Clone, Debug)]
pub struct Executor {
    storage: sled::Db,
}

#[cfg_attr(test, faux::methods)]
impl Default for Executor {
    fn default() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        dbg!("Creating Sled DB at {}", tmp_dir.path().to_str().unwrap());
        Self {
            storage: sled::open(tmp_dir.path()).unwrap(),
        }
    }
}

#[cfg_attr(test, faux::methods)]
impl Executor {
    pub async fn execute(
        &self,
        req: RunStmtRequestWithUuid,
    ) -> Result<RunStmtResponse, ExecutorErr> {
        let txn_uuid = req.uuid.clone();

        let sql_stmt = stmt_analyzer::SqlStmt::from_string(req.query).unwrap();

        // Load read and write records into local memory
        let mut record_cache = HashMap::<TouchedRecord, RecordStorage>::new();

        for select_record in sql_stmt.selected_records.iter() {
            let record_bytes = self
                .storage
                .get(select_record.fully_qualified_id_as_bytes())
                .unwrap()
                .ok_or_else(|| anyhow!("no record exists for {}", select_record.id))
                .unwrap();

            let record_bytes_buf = bytes::Bytes::from(record_bytes.to_vec());

            record_cache.insert(
                TouchedRecord {
                    record: select_record.clone(),
                    is_dirty: false,
                },
                RecordStorage::decode(record_bytes_buf).unwrap(),
            );
        }

        for update_record in sql_stmt.updated_records.iter() {
            let record_bytes = self
                .storage
                .get(update_record.fully_qualified_id_as_bytes())
                .unwrap()
                .ok_or_else(|| anyhow!("no record exists for {}", update_record.id))
                .unwrap();

            let record_bytes_buf = bytes::Bytes::from(record_bytes.to_vec());

            record_cache.insert(
                TouchedRecord {
                    record: update_record.clone(),
                    is_dirty: false,
                },
                RecordStorage::decode(record_bytes_buf).unwrap(),
            );
        }

        dbg!("Record Cache Before Execution: {:?}", record_cache.clone());

        // Execute the query
        let results =
            Self::execute_stmt(&mut record_cache, sql_stmt.ast_stmts.first().unwrap()).unwrap();

        dbg!("Record Cache After Execution: {:?}", record_cache.clone());

        // Flush dirty records
        for (key, value) in record_cache.into_iter() {
            if key.is_dirty {
                self.storage.insert(
                    key.record.fully_qualified_id_as_bytes(),
                    value.encode_to_vec(),
                );
            }
        }

        Ok(RunStmtResponse {
            result: Some(Success(RunStmtResults {
                uuid: txn_uuid,
                results,
            })),
        })
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
    use crate::calvinite_tonic::{RecordStorage, RunStmtRequestWithUuid};

    use crate::calvinite_tonic::run_stmt_response::Result::Success;
    use crate::executor::Executor;

    #[tokio::test]
    async fn executes_write_read() {
        let ex = Executor::default();

        let stmt1_uuid = uuid::Uuid::new_v4();
        let stmt1 = RunStmtRequestWithUuid {
            query: "INSERT INTO foo VALUES (1, 2)".into(),
            uuid: stmt1_uuid.to_string(),
        };

        let query_results1 = ex.execute(stmt1).await.unwrap();
        if let Some(Success(result)) = query_results1.result {
            assert_eq!(result.results, Vec::new());
        } else {
            panic!("Should always be successful")
        }

        let stmt2_uuid = uuid::Uuid::new_v4();
        let stmt2 = RunStmtRequestWithUuid {
            query: "SELECT * FROM foo WHERE id = 1".into(),
            uuid: stmt2_uuid.to_string(),
        };

        let query_results2 = ex.execute(stmt2).await.unwrap();
        if let Some(Success(result)) = query_results2.result {
            assert_eq!(result.results, vec![RecordStorage { val: 2 }]);
        } else {
            panic!("Should always be successful")
        }
    }
}
