use super::db;
use tokio_postgres::Error;

pub trait DatabaseQueriesRead {
    async fn execute(&self, query: String) -> Result<Vec<tokio_postgres::row::Row>, Error>;
    async fn list_columns(&self, table_name: &str) -> Result<(), Error>;
    async fn list_tables(&self, only_user_tables: Option<bool>) -> Result<Vec<String>, Error>;
    async fn table_row_count(&self, table_name: &str) -> Result<(), Error>;
    async fn check_postgis_support(&self) -> Result<bool, Error>;
}

#[derive(Clone)]
pub struct PostgresQueriesRead;

impl DatabaseQueriesRead for PostgresQueriesRead {
    async fn execute(&self, query: String) -> Result<Vec<tokio_postgres::row::Row>, Error> {
        // Get database client
        let (client, _) = db::new(None)
            .await
            .expect("❌ Failed to get database client");
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
        let rows = self.execute(query).await?;

        // Collect all rows into a vector
        let mut columns: Vec<(String, String)> = Vec::new();
        for row in rows {
            let column_name: String = row.get(0);
            let data_type: String = row.get(1);
            columns.push((column_name, data_type));
        }

        // Print table header

        println!("\n┌{:─<30}{:─<21}┐", "", "");
        println!(
            "│ {:<28}{:<21} │",
            format!("Columns in '{}' table", table_name),
            ""
        );
        println!("├{:─<30}┬{:─<20}┤", "", "");
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
    /// in the public schema and returns a vector of their names.
    /// If `only_user_tables` is `true`, it will only return user tables and
    /// not any system tables.
    ///
    /// Panics
    /// If the database query fails, this function will panic.
    async fn list_tables(&self, only_user_tables: Option<bool>) -> Result<Vec<String>, Error> {
        let only_user_tables = only_user_tables.unwrap_or(true); // Default to true
        let query = if only_user_tables {
            "
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name NOT IN (
                    'geometry_columns',
                    'spatial_ref_sys',
                    'raster_columns',
                    'raster_overviews'
                )
            "
            .to_string()
        } else {
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
                .to_string()
        };
        let rows = self.execute(query).await?;

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
        for table_name in &tables {
            println!("│ {:<28} │", table_name);
        }
        // Print table footer
        println!("└{:─<30}┘", "");

        Ok(tables)
    }

    /// Get the row count for a given table
    ///
    async fn table_row_count(&self, table_name: &str) -> Result<(), Error> {
        let query = format!("SELECT COUNT(*) FROM {} ", table_name).to_string();
        let rows = self.execute(query).await?;

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
    async fn check_postgis_support(&self) -> Result<bool, Error> {
        let query = "SELECT EXISTS (
            SELECT 1 
            FROM pg_extension 
            WHERE extname = 'postgis'
        )"
        .to_string();

        let rows = self.execute(query).await?;
        let postgis_exists: bool = rows[0].get(0);

        if postgis_exists {
            println!("PostGIS is supported in the current database");
        } else {
            println!("PostGIS is NOT supported in the current database");
        }

        Ok(postgis_exists)
    }
}
