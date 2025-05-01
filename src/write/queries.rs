use super::super::read::db;
use super::super::read::queries::DatabaseQueriesRead;
use super::super::read::Read;
use crate::write::utils::{
    convert_path, custom_unwrap_or, get_all_file_paths, process_and_upload_file, GeoJSONFile,
};
use chrono::Local;
use serde_json::{Deserializer, Value};
use std::error::Error as StdError;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::process::Command;
use std::time::Instant;
use sys_info;
use tokio_postgres::{Client, Error};

pub trait DatabaseQueriesWrite {
    async fn execute(
        &self,
        query: String,
        success_message: Option<&str>,
        error_message: Option<&str>,
    );

    async fn drop(&self, table_name: &str) -> Result<(), Box<dyn StdError>>;

    async fn drop_all_tables(&self) -> Result<(), Box<dyn std::error::Error>>;

    async fn fix_collation_version(&self, table_name: &str);

    async fn create_geo_table(&self, client: &Client, table_name: &str) -> Result<(), Error>;

    async fn insert_geojson(
        &self,
        geojson_path: &str,
        table_name: Option<&str>,
    ) -> Result<(), Box<dyn StdError>>;

    async fn backup_database(
        &self,
        output_dir: &str,
        no_of_jobs: Option<i32>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    async fn restore_database(
        &self,
        dump_file: &str,
        docker_container_name: Option<&str>,
        no_of_jobs: Option<i32>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct PostgresQueriesWrite;

impl DatabaseQueriesWrite for PostgresQueriesWrite {
    /// Execute a SQL query.
    ///
    /// This function will attempt to execute a SQL query
    /// in the database. If the query fails, an error message will be printed.
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

    /// Drop a table in the database.
    ///
    /// This function will attempt to drop a table
    /// in the database. If the table does not exist,
    /// the function will silently exit.
    async fn drop(&self, table_name: &str) -> Result<(), Box<dyn StdError>> {
        let query = format!("DROP TABLE IF EXISTS {} CASCADE", table_name).to_string();
        let read_queries = super::super::read::queries::PostgresQueriesRead;

        println!("🔄 Attempting to drop table: {}", table_name);
        match read_queries.execute(query).await {
            Ok(_) => {
                println!("✅ {} table dropped successfully", table_name);
                Ok(())
            }
            Err(e) => {
                eprintln!("❌ Failed to drop table: {}", e);
                Err(Box::new(e))
            }
        }
    }

    async fn drop_all_tables(&self) -> Result<(), Box<dyn std::error::Error>> {
        let read_queries = super::super::read::queries::PostgresQueriesRead;
        // let query = "DROP SCHEMA public CASCADE; CREATE SCHEMA public;";

        let tables = read_queries.list_tables(Some(true)).await?;

        if tables.is_empty() || tables.len() == 0 {
            println!("🤗 No tables to drop");
            return Ok(());
        }

        println!("Found {} tables to drop", tables.len());
        println!("🔄 Attempting to drop all tables");

        let drop_futures: Vec<_> = tables
            .into_iter()
            .map(|table_name| {
                // let read_queries = read_queries.clone(); // Clone for each closure
                let read_queries = super::super::read::queries::PostgresQueriesRead;

                let drop_query = format!("DROP TABLE {} CASCADE", table_name);

                println!("🔄 Scheduling drop for table: {}", table_name);
                async move {
                    match read_queries.execute(drop_query).await {
                        Ok(_) => println!("✅ Dropped table: {:?}", &table_name),
                        Err(e) => println!("❌ Failed to drop table {:?}: {}", &table_name, e),
                    }
                }
            })
            .collect();

        futures::future::join_all(drop_futures).await;
        println!("✅ All tables dropped successfully");

        Ok(())
    }

    /// Resolve version mismatch error
    ///WARNING:  database "postgres_db" has a collation version mismatch
    ///DETAIL:  The database was created using collation version 2.36, but the operating system provides version 2.31.
    ///HINT:  Rebuild all objects in this database that use the default collation and run ALTER DATABASE postgres_db REFRESH COLLATION VERSION, or build PostgreSQL with the right library version.
    async fn fix_collation_version(&self, table_name: &str) {
        let query = format!("ALTER DATABASE {} REFRESH COLLATION VERSION", table_name).to_string();
        self.execute(
            query,
            Some("✅ Collation version fixed successfully"),
            Some("❌ Failed to fix collation version"),
        )
        .await;
    }

    async fn create_geo_table(&self, client: &Client, table_name: &str) -> Result<(), Error> {
        println!("⏳ Attempting to create table: {}", table_name);
        client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            .await?;

        client
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(512) NOT NULL UNIQUE,
                properties JSONB NOT NULL,
                geometry GEOMETRY,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX {}_properties_idx ON {} USING GIN (properties);",
                table_name, table_name, table_name
            ))
            .await?;
        println!("✅ Table {} created successfully", table_name);
        Ok(())
    }

    async fn backup_database(
        &self,
        output_dir: &str,
        no_of_jobs: Option<i32>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();

        let db_config = Read::config_data().config;
        let no_of_jobs = no_of_jobs.unwrap_or(4);

        let output_file = format!(
            "{}/backup_{}_{}.dump",
            output_dir, db_config.db_name, timestamp
        );
        println!("🔄 Attempting to backup {} database", &db_config.db_name);
        println!("🕒 Backup timestamp: {}", timestamp);

        // Set password in environment variable
        std::env::set_var("PGPASSWORD", &db_config.password);

        let command = format!(
            "pg_dump \
            --host={} --port={} --username={} --dbname={} \
            --jobs={} \
            --format=custom \
            --no-privileges \
            --no-owner \
            --exclude-table='geometry_columns' \
            --exclude-table='spatial_ref_sys' \
            --exclude-table='raster_columns' \
            --exclude-table='raster_overviews' \
            --file={}",
            db_config.host,
            db_config.port,
            db_config.user,
            db_config.db_name,
            no_of_jobs,
            output_file
        );

        println!("💻 Executing command: {}", command);
        println!("⏳ Running pg_dump...");

        match tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .status()
            .await
        {
            Ok(status) if status.success() => {
                println!(
                    "✅ Database '{}' backed up to {}",
                    db_config.db_name, output_file
                );
                Ok(())
            }
            Ok(_) => {
                eprintln!("❌ Failed to backup database '{}'", db_config.db_name);
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Database backup failed",
                )))
            }
            Err(e) => {
                eprintln!(
                    "❌ Error backing up database '{}': {}",
                    db_config.db_name, e
                );
                Err(Box::new(e))
            }
        }
    }

    async fn restore_database(
        &self,
        dump_file: &str,
        docker_container_name: Option<&str>,
        no_of_jobs: Option<i32>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let db_config = Read::config_data().config;
        let no_of_jobs = no_of_jobs.unwrap_or(4);

        println!(
            "🔄 Attempting to restore database unto {}",
            &db_config.db_name
        );

        let _ = self.drop_all_tables().await?;

        // Detect system memory and set appropriate values
        let total_memory = sys_info::mem_info()
            .map(|info| info.total)
            .unwrap_or(16 * 1024 * 1024); // Default to 16GB if detection fails

        let work_mem = if total_memory >= 64 * 1024 * 1024 {
            // 64GB
            "512MB"
        } else if total_memory >= 32 * 1024 * 1024 {
            // 32GB
            "256MB"
        } else {
            // 16GB or less
            "128MB"
        };

        let maintenance_work_mem = if total_memory >= 64 * 1024 * 1024 {
            // 64GB
            "1GB"
        } else if total_memory >= 32 * 1024 * 1024 {
            // 32GB
            "512MB"
        } else {
            // 16GB or less
            "256MB"
        };

        // Set password in environment variable
        std::env::set_var("PGPASSWORD", &db_config.password);
        std::env::set_var("PGWORKMEM", work_mem);
        std::env::set_var("PGMAINTENANCE_WORK_MEM", maintenance_work_mem);

        println!("💾 Detected memory: {}KB", total_memory);
        println!("⚙️  Using WORK_MEM: {}", work_mem);
        println!("⚙️  Using MAINTENANCE_WORK_MEM: {}", maintenance_work_mem);
        println!("⚙️  Total number of jobs: {}", &no_of_jobs);
        // Step 1: Restore schema only
        println!("📊 Step 1: Restoring schema...");

        let mut schema_command: String = String::new();

        if let Some(container) = docker_container_name {
            println!("⚙️ Docker container specified: {}", container);
            schema_command = format!(
                "docker exec -i {} pg_restore \
                    --host={} --port={} --username={} --dbname={} \
                    --jobs={} \
                    --schema-only \
                    --clean \
                    --if-exists \
                    --no-acl \
                    --no-comments \
                    {}",
                container,
                db_config.host,
                db_config.port,
                db_config.user,
                db_config.db_name,
                no_of_jobs,
                dump_file
            );
        } else {
            println!("⚙️ No Docker container specified");
            schema_command = format!(
                "pg_restore \
                    --host={} --port={} --username={} --dbname={} \
                    --jobs={} \
                    --schema-only \
                    --clean \
                    --if-exists \
                    --no-acl \
                    --no-comments \
                    {}",
                db_config.host,
                db_config.port,
                db_config.user,
                db_config.db_name,
                no_of_jobs,
                dump_file
            );
        }

        match tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&schema_command)
            .status()
            .await
        {
            Ok(status) if status.success() => {
                println!("✅ Schema restored successfully");
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "❌ Schema restore failed",
                )));
            }
        }

        // Step 2: Restore data only
        println!("\n\n📊 Phase 2: Restoring data...");
        let data_command = format!(
            "pg_restore \
                --host={} --port={} --username={} --dbname={} \
                --jobs={} \
                --data-only \
                --disable-triggers \
                --no-acl \
                --no-comments \
                {}",
            db_config.host,
            db_config.port,
            db_config.user,
            db_config.db_name,
            no_of_jobs,
            dump_file
        );

        println!("⏳ Running pg_restore...");
        match tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&data_command)
            .status()
            .await
        {
            Ok(status) if status.success() => {
                let duration = start_time.elapsed();
                println!(
                    "✅ Database '{}' restored from {} in {:.2?}",
                    db_config.db_name, dump_file, duration
                );
                Ok(())
            }
            Ok(_) => {
                let duration = start_time.elapsed();
                eprintln!(
                    "❌ Failed to restore database '{}' after {:.2?}",
                    db_config.db_name, duration
                );
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Database restore failed",
                )))
            }
            Err(e) => {
                let duration = start_time.elapsed();
                eprintln!(
                    "❌ Error restoring database '{}' after {:.2?}: {}",
                    db_config.db_name, duration, e
                );
                Err(Box::new(e))
            }
        }
    }

    /// Uploads a GeoJSON file to the database.
    ///
    /// This function takes a GeoJSON file path and an optional table name. If no table name is
    /// provided, it will be extracted from the file name. The function creates a table if it doesn't
    /// exist and then uploads the GeoJSON data to the table using a COPY operation.
    ///
    /// # Parameters
    ///
    /// * `geojson_path`: The path to the GeoJSON file to upload.
    /// * `table_name`: An optional table name to use for the upload. If not provided, the table name
    ///   will be extracted from the file name.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error.
    ///
    /// # Errors
    ///
    /// * `Box<dyn StdError>`: If an error occurs during the upload process.
    ///
    /// # Examples
    ///
    /// ```
    /// let queries = PostgresQueriesWrite;
    /// let result = queries.insert_geojson("path/to/geojson.json", None);
    /// ```
    async fn insert_geojson(
        &self,
        geojson_path: &str,
        table_name: Option<&str>,
    ) -> Result<(), Box<dyn StdError>> {
        let (client, pool) = db::new(Some(true))
            .await
            .expect("❌ Failed to get database client or pool");

        let table_name = custom_unwrap_or(
            table_name,
            std::path::Path::new(geojson_path)
                .file_stem() // Option<&OsStr>
                .and_then(|s| s.to_str()) // Option<&str>
                .unwrap_or("unknown"),
            "table_name",
        );
        // Create table if it doesn't exist
        if let Err(e) = self.create_geo_table(&client, table_name).await {
            // Optionally, check for specific error code if not using IF NOT EXISTS
            eprintln!(
                "Warning: Could not create '{}' table (may already exist):\n{}",
                table_name, e
            );
            // You can proceed, unless the error is critical
        }
        process_and_upload_file(&client, geojson_path, table_name).await?;

        Ok(())
    }
}
