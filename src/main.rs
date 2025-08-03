use anyhow::Result;
use sqlparser::{ast::TableFactor, dialect::DuckDbDialect, parser::Parser};

pub mod config;
pub mod graph;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::parse_config("examples/simple".into());
    println!("{config:#?}");

    let sql = "SELECT a, b FROM c AS d";

    let dialect = DuckDbDialect;
    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    match ast[0] {
        sqlparser::ast::Statement::Query(ref query) => match query.body.as_ref() {
            sqlparser::ast::SetExpr::Select(select) => {
                println!("{:?}", select.from[0].relation);
                match &select.from[0].relation {
                    TableFactor::Table { name, alias, .. } => {
                        println!("Table name: {name}");
                        if let Some(alias) = alias {
                            println!("Alias: {}", alias.name);
                        } else {
                            println!("No alias provided");
                        }
                    }
                    _ => {
                        println!("Not a table factor");
                    }
                }
            }
            _ => {
                println!("Not a SELECT statement");
            }
        },
        _ => {
            println!("Not a query statement");
        }
    }

    // let db: DatabaseConnection = Database::connect("sqlite::memory:").await?;

    Ok(())
}
