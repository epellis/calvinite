use crate::common::Record;
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn get_impacted_records(sql_stmt: &str) -> anyhow::Result<Vec<Record>> {
    let dialect = GenericDialect {};

    let sql_stmts = Parser::parse_sql(&dialect, sql_stmt)?;

    dbg!("Got Statements: {:?}", sql_stmts.clone());

    Ok(sql_stmts
        .into_iter()
        .flat_map(|stmt| get_impacted_records_from_stmt(stmt))
        .collect())
}

fn get_impacted_records_from_stmt(stmt: ast::Statement) -> Vec<Record> {
    match stmt {
        ast::Statement::Insert { source, .. } => match source.body {
            ast::SetExpr::Values(ast::Values(values)) => values
                .iter()
                .flat_map(|v| get_impacted_record_from_value_vec(v))
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(), // Statement::Analyze { .. } => {}
                         // Statement::Truncate { .. } => {}
                         // Statement::Msck { .. } => {}
                         // Statement::Query(_) => {}
                         // Statement::Insert { .. } => {}
                         // Statement::Directory { .. } => {}
                         // Statement::Copy { .. } => {}
                         // Statement::Update { .. } => {}
                         // Statement::Delete { .. } => {}
                         // Statement::CreateView { .. } => {}
                         // Statement::CreateTable { .. } => {}
                         // Statement::CreateVirtualTable { .. } => {}
                         // Statement::CreateIndex { .. } => {}
                         // Statement::AlterTable { .. } => {}
                         // Statement::Drop { .. } => {}
                         // Statement::SetVariable { .. } => {}
                         // Statement::ShowVariable { .. } => {}
                         // Statement::ShowCreate { .. } => {}
                         // Statement::ShowColumns { .. } => {}
                         // Statement::StartTransaction { .. } => {}
                         // Statement::SetTransaction { .. } => {}
                         // Statement::Comment { .. } => {}
                         // Statement::Commit { .. } => {}
                         // Statement::Rollback { .. } => {}
                         // Statement::CreateSchema { .. } => {}
                         // Statement::CreateDatabase { .. } => {}
                         // Statement::Assert { .. } => {}
                         // Statement::Grant { .. } => {}
                         // Statement::Revoke { .. } => {}
                         // Statement::Deallocate { .. } => {}
                         // Statement::Execute { .. } => {}
                         // Statement::Prepare { .. } => {}
                         // Statement::ExplainTable { .. } => {}
                         // Statement::Explain { .. } => {}
    }
}

fn get_impacted_record_from_value_vec(value: &Vec<ast::Expr>) -> Option<Record> {
    match value.first() {
        Some(ast::Expr::Value(ast::Value::Number(value, _))) => match value.parse() {
            Ok(number_value) => Some(Record { id: number_value }),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Record;
    use crate::stmt_analyzer::get_impacted_records;

    #[test]
    fn get_impacted_records_for_insert() {
        let stmt = "INSERT INTO foo VALUES (1)";

        assert_eq!(get_impacted_records(stmt).unwrap(), vec![Record { id: 1 }])
    }

    #[test]
    fn get_impacted_doulbrecords_for_insert_multiple_values() {
        let stmt = "INSERT INTO foo VALUES (1), (2), (3)";

        assert_eq!(
            get_impacted_records(stmt).unwrap(),
            vec![Record { id: 1 }, Record { id: 2 }, Record { id: 3 }]
        )
    }
}
