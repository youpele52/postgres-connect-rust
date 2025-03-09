use super::super::read::db;
use super::super::read::queries::DatabaseQueriesRead;
use super::super::read::Read;
use crate::write::utils::{convert_path, get_all_file_paths, GeoJSONFile};
use serde_json::{Deserializer, Value};
use std::error::Error as StdError;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::process::Command;
use tokio_postgres::{Client, Error};

pub trait DatabaseQueriesWrite {
    async fn execute(
        &self,
        query: String,
        success_message: Option<&str>,
        error_message: Option<&str>,
    );
    async fn drop(&self, table_name: &str) -> Result<(), Box<dyn StdError>>;
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

    async fn insert_geojson(
        &self,
        geojson_path: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn StdError>> {
        // Connect to database
        let mut client = db::new().await?;
        println!("🔄 Processing geojson file '{}'...", geojson_path);
        // Create table first
        self.create_geo_table(&client, table_name)
            .await
            .expect(format!("❌ Failed to create table: {}\n", table_name).as_str());
        let converted_path = convert_path(geojson_path).unwrap();
        if converted_path.is_dir() {
            println!("🔄 Processing directory {}...", geojson_path);
            let paths = get_all_file_paths(converted_path).await?;
            for path in paths {
                self.insert_one_geojson(&client, path.as_str(), table_name)
                    .await?;
            }
            let read_queries = super::super::read::queries::PostgresQueriesRead;
            read_queries.table_row_count(table_name).await;
            Ok(())
        } else {
            self.insert_one_geojson(&client, geojson_path, table_name)
                .await
        }
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
}
