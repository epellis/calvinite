use crate::common::Record;
use sqlparser::ast;
use sqlparser::ast::Expr;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Stores an analyzed SQL string made of many SQL Statements.
#[derive(Clone, Debug)]
pub struct SqlStmt {
    str_stmt: String,
    ast_stmts: Vec<ast::Statement>,
    pub read_records: Vec<Record>,
    pub inserted_records: Vec<Record>,
    pub updated_records: Vec<Record>,
}

impl SqlStmt {
    pub fn from_string(str_stmt: String) -> anyhow::Result<Self> {
        let ast_stmts = Parser::parse_sql(&GenericDialect {}, &str_stmt)?;
        let inserted_records = ast_stmts
            .iter()
            .flat_map(Self::find_inserted_records)
            .collect();

        let updated_records = ast_stmts
            .iter()
            .flat_map(Self::find_updated_records)
            .collect();

        Ok(Self {
            str_stmt,
            ast_stmts,
            read_records: Vec::new(),
            inserted_records,
            updated_records,
        })
    }

    fn find_updated_records(stmt: &ast::Statement) -> Vec<Record> {
        match stmt {
            ast::Statement::Update {
                selection: Some(selection),
                ..
            } => Vec::from_iter(Self::find_id_in_expr(selection).into_iter()),
            _ => Vec::new(),
        }
    }

    fn find_id_in_expr(expr: &ast::Expr) -> Option<Record> {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::Eq,
                right,
            } => match (*left.clone(), *right.clone()) {
                (
                    Expr::Identifier(ast::Ident { value, .. }),
                    Expr::Value(ast::Value::Number(number, _)),
                ) => {
                    if value == "id" {
                        if let Ok(num) = number.parse() {
                            Some(Record { id: num })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn find_inserted_records(stmt: &ast::Statement) -> Vec<Record> {
        match stmt {
            ast::Statement::Insert { source, .. } => match &source.body {
                ast::SetExpr::Values(ast::Values(values)) => values
                    .iter()
                    .flat_map(Self::first_num_from_value_vec)
                    .collect(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        }
    }

    fn first_num_from_value_vec(values: &Vec<ast::Expr>) -> Option<Record> {
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

        assert_eq!(analyzed_stmt.inserted_records, vec![Record { id: 1 }])
    }

    #[test]
    fn get_impacted_records_for_insert_multiple_values() {
        let stmt = "INSERT INTO foo VALUES (1), (2, 3), (4, 5, 6)".to_string();
        let analyzed_stmt = SqlStmt::from_string(stmt).unwrap();

        assert_eq!(
            analyzed_stmt.inserted_records,
            vec![Record { id: 1 }, Record { id: 2 }, Record { id: 4 }]
        )
    }

    #[test]
    fn get_impacted_records_for_update() {
        let stmt = "UPDATE foo SET id = 2 WHERE id = 1".to_string();
        let analyzed_stmt = SqlStmt::from_string(stmt).unwrap();

        assert_eq!(analyzed_stmt.updated_records, vec![Record { id: 1 }])
    }
}
