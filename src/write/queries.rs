use super::super::read::db;
use super::super::read::queries::DatabaseQueriesRead;
use super::super::read::Read;
use crate::write::utils::{convert_path, get_all_file_paths, GeoJSONFile};
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

    async fn split_geojson(
        &self,
        input_file: &str,
        output_dir: &str,
        chunk_size: usize,
    ) -> std::io::Result<()>;
    async fn fix_collation_version(&self, table_name: &str);
    async fn create_geo_table(&self, client: &Client, table_name: &str) -> Result<(), Error>;
    async fn insert_one_geojson(
        &self,
        client: &Client,
        geojson_path: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn StdError>>;
    async fn insert_geojson(
        &self,
        geojson_path: &str,
        table_name: &str,
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

    /// Split a large GeoJSON file into smaller chunks and write them to disk.
    ///
    /// This function will read a large GeoJSON file, split it into smaller chunks,
    /// and write them to disk. The output files will be named after the input
    /// file followed by a number, e.g. `input.geojson` becomes `input_0.geojson`,
    /// `input_1.geojson`, etc.
    ///
    /// # Parameters
    ///
    /// * `input_file`: The path to the input GeoJSON file to be split.
    /// * `output_dir`: The directory where the output files should be written.
    /// * `chunk_size`: The number of features to write to each output file. If
    ///   set to 0, the entire file will be written to a single output file.
    ///
    async fn split_geojson(
        &self,
        input_file: &str,
        output_dir: &str,
        chunk_size: usize,
    ) -> std::io::Result<()> {
        let file = File::open(input_file)
            .expect(format!("❌ Failed to open input file: {}", input_file).as_str());
        let reader = BufReader::new(file);
        let stream = Deserializer::from_reader(reader).into_iter::<Value>();
        let file_name = input_file
            .split('/')
            .last()
            .unwrap()
            .split('.')
            .next()
            .unwrap();
        let mut features = Vec::new();
        let mut file_count = 0;
        for feature in stream {
            if let Ok(f) = feature {
                features.push(f);
                if features.len() >= chunk_size {
                    // Write to new file
                    let output_file = format!(
                        "{}/split_geojson_count_{}-{}.geojson",
                        output_dir, file_count, file_name
                    );
                    let mut writer = BufWriter::new(File::create(&output_file)?);
                    let feature_collection =
                        serde_json::json!({"type": "FeatureCollection", "features": features});
                    writeln!(writer, "{}", serde_json::to_string(&feature_collection)?)?;
                    writer.flush()?;

                    // Reset for next chunk
                    features.clear();
                    file_count += 1;
                    println!("✅ Created {}", output_file);
                }
            }
        }

        // Write remaining features
        if !features.is_empty() {
            let output_file = format!(
                "{}/split_geojson_count_{}-{}.geojson",
                output_dir, file_count, file_name
            );
            let mut writer = BufWriter::new(File::create(&output_file)?);
            let feature_collection =
                serde_json::json!({"type": "FeatureCollection", "features": features});
            writeln!(writer, "{}", serde_json::to_string(&feature_collection)?)?;
            writer.flush()?;
            println!("✅ Created {}", output_file);
        }

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
        // Create table with JSONB column and index
        client
            .batch_execute(&format!(
                "CREATE TABLE {} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(512) NOT NULL UNIQUE,
                data JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX {}_data_idx ON {} USING GIN (data);",
                table_name, table_name, table_name
            ))
            .await
    }

    /// Insert GeoJSON file into the database
    ///
    /// This function will read a GeoJSON file and insert it into the database.
    /// The function will create the table if it does not exist.
    /// The function will also create an index on the JSONB column for efficient querying.
    ///
    ///  *Parameters*
    ///
    /// * `geojson_path`: The path to the GeoJSON file to be inserted.
    /// * `table_name`: The name of the table to insert the data into.
    ///
    /// *Errors*
    ///
    /// This function will return an error if the GeoJSON file does not exist,
    /// if the database connection fails, or if the insert query fails.
    async fn insert_geojson(
        &self,
        geojson_path: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn StdError>> {
        // Create connection pool for efficient database connections
        let pool = db::new_pool().await?;

        // Convert input path to Path object and verify it exists
        let converted_path = convert_path(geojson_path).unwrap();

        // Process files in parallel
        let paths = if converted_path.is_dir() {
            // If path is a directory, get all file paths recursively
            println!("🔄 Processing directory {}...", geojson_path);
            get_all_file_paths(converted_path).await?
        } else {
            // If path is a file, create single-element vector
            println!("🔄 Processing file {}...", geojson_path);
            vec![geojson_path.to_string()]
        };

        // Get database connection from pool
        let client = pool.get().await?;

        // Create table if it doesn't exist
        self.create_geo_table(&client, table_name).await?;

        // Process files concurrently using tokio::spawn
        let futures = paths.into_iter().map(|path| {
            // Clone pool and table_name for each task
            let pool = pool.clone();
            let table_name = table_name.to_string();

            // Spawn async task for each file
            tokio::spawn(async move {
                // Get database connection from pool
                let mut client = pool.get().await.expect("❌ Failed to get client");

                // Process GeoJSON file
                let geo_file = GeoJSONFile::process_geojson_file(&path)
                    .await
                    .expect("❌ Failed to process geojson file");

                // Start database transaction
                let transaction = client
                    .transaction()
                    .await
                    .expect("❌ Failed to start transaction");

                // Execute insert query
                transaction
                    .execute(
                        &format!("INSERT INTO {} (name, data) VALUES ($1, $2)", table_name),
                        &[&geo_file.file_name, &geo_file.json_data],
                    )
                    .await?;

                // Commit transaction
                transaction.commit().await
            })
        });

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(futures).await;

        // Handle any errors from tasks
        for result in results {
            result??;
        }

        // Check row count after insertion
        let read_queries = super::super::read::queries::PostgresQueriesRead;
        read_queries.table_row_count(table_name).await;

        // Return success
        Ok(())
    }
    async fn insert_one_geojson(
        &self,
        client: &Client,
        geojson_path: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let converted_path = convert_path(geojson_path).unwrap();

        // Connect to database
        let mut client = db::new().await?;
        println!("🔄 Processing geojson file '{}'...", geojson_path);

        // Process GeoJSON file
        let GeoJSONFile {
            file_name,
            json_data,
        } = GeoJSONFile::process_geojson_file(geojson_path).await?;

        println!(
            "🔄 Inserting geojson file '{}' into table '{}'...",
            geojson_path, table_name
        );
        // Start transaction
        let transaction = client
            .transaction()
            .await
            .expect("❌ Failed to start transaction");

        // In insert_one_geojson function
        match transaction
            .execute(
                &format!("INSERT INTO {} (name, data) VALUES ($1, $2)", table_name),
                &[&file_name, &json_data],
            )
            .await
        {
            Ok(_) => println!(
                "✅ Successfully inserted  geojson from {} into '{}' table",
                geojson_path, table_name
            ),
            Err(e) => {
                eprintln!("❌ Failed to insert GeoJSON: {}", e);
                return Err(Box::new(e));
            }
        }

        // Commit transaction
        match transaction.commit().await {
            Ok(_) => {
                println!("✅ Transaction committed successfully\n");
                Ok(())
            }
            Err(e) => {
                eprintln!("❌ Failed to commit transaction: {}", e);
                Err(Box::new(e))
            }
        }
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
}
