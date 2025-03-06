use super::super::read::queries::DatabaseQueriesRead;
use super::super::read::Read;
use serde_json::{Deserializer, Value};
use std::error::Error as StdError;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::process::Command;
use tokio_postgres::Error;
pub trait DatabaseQueriesWrite {
    async fn execute(
        &self,
        query: String,
        success_message: Option<&str>,
        error_message: Option<&str>,
    );
    async fn drop(&self, table_name: &str);
    async fn split_geojson(
        &self,
        input_file: &str,
        output_dir: &str,
        chunk_size: usize,
    ) -> std::io::Result<()>;
    async fn upload_geojson(
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

    async fn upload_geojson(
        &self,
        geojson_path: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn StdError>> {
        let db_url = Read::config_data().db_url;

        if !std::path::Path::new(geojson_path).exists() {
            eprintln!("❌ GeoJSON file not found: {}", geojson_path);
            return Err("GeoJSON file not found".into());
        } else {
            println!("✅ GeoJSON file found");
        }
        // Ensure ogr2ogr is installed
        let check_ogr = Command::new("ogr2ogr").arg("--version").output();
        if check_ogr.is_err() {
            eprintln!("❌ (ogr2ogr) GDAL is not installed");
            return Err("GDAL is not installed. Please install GDAL first.".into());
        } else {
            println!("✅ GDAL is installed");
        }

        // Construct the ogr2ogr command
        let output = Command::new("ogr2ogr")
            .arg("-f")
            .arg("PostgreSQL")
            .arg(format!("PG:{}", db_url))
            .arg(geojson_path)
            .arg("-nln")
            .arg(table_name)
            .arg("-append")
            .arg("-progress")
            .arg("-lco")
            .arg("GEOMETRY_NAME=geom")
            .arg("-lco")
            .arg("FID=id")
            .arg("-lco")
            .arg("SPATIAL_INDEX=FALSE")
            .output()?;

        // Check for errors
        if !output.status.success() {
            eprintln!("❌ ogr2ogr command failed");
            eprintln!(
                "Command stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return Err(format!(
                "ogr2ogr failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        println!("✅ Successfully imported GeoJSON into {}", table_name);
        Ok(())
    }
}
