use crate::common::Record;
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Stores an analyzed SQL string made of many SQL Statements.
#[derive(Clone, Debug)]
pub struct SqlStmt {
    str_stmt: String,
    ast_stmts: Vec<ast::Statement>,
    pub read_records: Vec<Record>,
    pub write_records: Vec<Record>,
}

impl SqlStmt {
    pub fn from_string(str_stmt: String) -> anyhow::Result<Self> {
        let ast_stmts = Parser::parse_sql(&GenericDialect {}, &str_stmt)?;
        let write_records = ast_stmts
            .iter()
            .flat_map(Self::find_write_records)
            .collect();

        Ok(Self {
            str_stmt,
            ast_stmts,
            read_records: Vec::new(),
            write_records,
        })
    }

    fn find_write_records(stmt: &ast::Statement) -> Vec<Record> {
        match stmt {
            ast::Statement::Insert { source, .. } => match &source.body {
                ast::SetExpr::Values(ast::Values(values)) => values
                    .iter()
                    .flat_map(Self::get_impacted_record_from_value_vec)
                    .collect(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        }
    }

    fn get_impacted_record_from_value_vec(values: &Vec<ast::Expr>) -> Option<Record> {
        values
            .first()
            .and_then(|value| Self::expr_to_num(value).map(|key| Record { id: key }))
    }

    fn expr_to_num(expr: &ast::Expr) -> Option<u64> {
        match expr {
            ast::Expr::Value(ast::Value::Number(value, _)) => match value.parse() {
                Ok(number_value) => Some(number_value),
                _ => None,
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Record;
    use crate::stmt_analyzer::SqlStmt;

    #[test]
    fn get_impacted_records_for_insert() {
        let stmt = "INSERT INTO foo VALUES (1)".to_string();
        let analyzed_stmt = SqlStmt::from_string(stmt).unwrap();

        assert_eq!(analyzed_stmt.write_records, vec![Record { id: 1 }])
    }

    #[test]
    fn get_impacted_records_for_insert_multiple_values() {
        let stmt = "INSERT INTO foo VALUES (1), (2, 3), (4, 5, 6)".to_string();
        let analyzed_stmt = SqlStmt::from_string(stmt).unwrap();

        assert_eq!(
            analyzed_stmt.write_records,
            vec![Record { id: 1 }, Record { id: 2 }, Record { id: 4 }]
        )
    }
}
