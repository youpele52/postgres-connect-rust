use super::db;
use tokio_postgres::Error;

pub trait DatabaseQueries {
    async fn new(&self, query: String) -> Result<Vec<tokio_postgres::row::Row>, Error>;
    async fn list_columns(&self, table_name: &str) -> Result<(), Error>;
    async fn list_tables(&self) -> Result<(), Error>;
    async fn table_row_count(&self, table_name: &str) -> Result<(), Error>;
}

pub struct PostgresQueries;

impl DatabaseQueries for PostgresQueries {
    async fn new(&self, query: String) -> Result<Vec<tokio_postgres::row::Row>, Error> {
        // Get database client
        let client = db::main().await?;

        // Execute the query without parameters
        let rows = client
            .query(&query, &[])
            .await
            .expect(" ❌ Failed to query database!!\n");

        // Collect all rows into a vector
        let mut result: Vec<tokio_postgres::row::Row> = Vec::new();
        for row in rows {
            result.push(row);
        }

        Ok(result)
    }

    /// List all columns in a table
    ///
    /// This function queries the database for all columns
    /// in a table and prints them to the console.
    async fn list_columns(&self, table_name: &str) -> Result<(), Error> {
        let query = format!(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}';",
        table_name
    )
        .to_string();
        let rows = self.new(query).await?;

        // Collect all rows into a vector
        let mut columns: Vec<(String, String)> = Vec::new();
        for row in rows {
            let column_name: String = row.get(0);
            let data_type: String = row.get(1);
            columns.push((column_name, data_type));
        }

        // Print table header
        println!("\n┌{:─<30}┬{:─<20}┐", "", "");
        println!("│ {:<28} │ {:<18} │", "column_name", "data_type");
        println!("├{:─<30}┼{:─<20}┤", "", "");

        // Print table rows
        for (col_name, data_type) in columns {
            println!("│ {:<28} │ {:<18} │", col_name, data_type);
        }

        // Print table footer
        println!("└{:─<30}┴{:─<20}┘", "", "");

        Ok(())
    }

    /// List all tables in the database
    ///
    /// This function queries the database for all tables
    /// in the public schema and prints them to the console.
    async fn list_tables(&self) -> Result<(), Error> {
        let query =
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
                .to_string();
        let rows = self.new(query).await?;

        // Collect all rows into a vector
        let mut tables: Vec<String> = Vec::new();
        for row in rows {
            let table_name: String = row.get(0);
            tables.push(table_name);
        }

        // Print table header
        println!("\n┌{:─<30}┐", "");
        println!("│ {:<28} │", "table_name");
        println!("├{:─<30}┤", "");
        // Print table rows
        for table_name in tables {
            println!("│ {:<28} │", table_name);
        }
        // Print table footer
        println!("└{:─<30}┘", "");

        Ok(())
    }

    /// Get the row count for a given table
    ///
    async fn table_row_count(&self, table_name: &str) -> Result<(), Error> {
        let query = format!("SELECT COUNT(*) FROM {} ", table_name).to_string();
        let rows = self.new(query).await?;

        // Get the count from the first row, first column
        let count: i64 = rows[0].get(0);

        // Print table header
        println!("\n┌{:─<30}┐", "");
        println!("│ {:<28} │", format!("Row count for {}", table_name));
        println!("├{:─<30}┤", "");
        // Print row count
        println!("│ {:<28} │", count);
        // Print table footer
        println!("└{:─<30}┘", "");

        Ok(())
    }
}
