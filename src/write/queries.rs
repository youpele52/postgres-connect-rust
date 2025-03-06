use super::super::read::queries::DatabaseQueriesRead;
use tokio_postgres::Error;

pub trait DatabaseQueriesWrite {
    async fn execute(
        &self,
        query: String,
        success_message: Option<&str>,
        error_message: Option<&str>,
    );
    async fn drop(&self, table_name: &str);
}

pub struct PostgresQueriesWrite;

impl DatabaseQueriesWrite for PostgresQueriesWrite {
    async fn execute(
        &self,
        query: String,
        success_message: Option<&str>,
        error_message: Option<&str>,
    ) {
        let read_queries = super::super::read::queries::PostgresQueriesRead;
        // read_queries.execute(query).await;

        match read_queries.execute(query).await {
            Ok(_) => println!(
                "{}",
                success_message.unwrap_or("✅  Query executed successfully")
            ),
            Err(e) => eprintln!(
                "{}\n\n❌  Error executing query: {}",
                error_message.unwrap_or("Error executing query"),
                e
            ),
        }
    }

    async fn drop(&self, table_name: &str) {
        let query = format!("DROP TABLE IF EXISTS {}", table_name).to_string();

        println!("Attempting to drop table: {}", table_name);
        let result = self
            .execute(
                query,
                Some(format!("✅ {} table dropped successfully", table_name).as_str()),
                Some("❌ Failed to drop table"),
            )
            .await;
        // let read_queries = read::queries::PostgresQueriesRead;
        // read_queries.new(query).await;
    }
}
