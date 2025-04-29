use anyhow::Result;
// use flate2::bufread::GzDecoder;
use async_compression::tokio::write::GzipDecoder;
use bytes::BytesMut;
use flate2::read::GzDecoder;
use futures::stream::FuturesUnordered;
use futures::SinkExt;
use geojson::{GeoJson, Geometry};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Value};
use std::error::Error as StdError;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::File as TokioFile;

use std::fmt::Display;
use tokio::io::AsyncRead;
use tokio_postgres::{Client, CopyInSink}; // Make sure this is imported

pub struct GeoJSONFile {
    pub file_name: String,
    pub json_data: Value,
}

impl GeoJSONFile {
    pub async fn process_geojson_file(path: &str) -> Result<Self, Box<dyn StdError>> {
        let data = tokio::fs::read_to_string(path).await?;

        // Parse JSON into Value
        let json_data: Value = serde_json::from_str(&data)?;

        let file_name = Path::new(path)
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                let err_msg = format!("❌ Unable to get file name from path: {}", path);
                Box::<dyn StdError>::from(err_msg)
            })?
            .split(".")
            .next()
            .ok_or_else(|| {
                let err_msg = format!("❌ Unable to get file name from path: {}", path);
                Box::<dyn StdError>::from(err_msg)
            })?
            .to_string();

        Ok(Self {
            file_name,
            json_data,
        })
    }
}

pub fn convert_path(path_str: &str) -> Result<&Path, Box<dyn StdError>> {
    let path: &Path = Path::new(path_str);
    if path.exists() {
        println!("Path exists!");
        Ok(path)
    } else {
        eprintln!("Path does not exist: {}", path_str);
        Err(format!("Path does not exist: {}", path_str).into())
    }
}

pub async fn get_all_file_paths(dir_path: &Path) -> Result<Vec<String>, Box<dyn StdError>> {
    let mut paths = Vec::new();
    let mut entries = fs::read_dir(dir_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            let sub_paths = Box::pin(get_all_file_paths(&path)).await?;
            paths.extend(sub_paths);
        } else if path.is_file() {
            paths.push(path.to_string_lossy().into_owned());
        }
    }

    Ok(paths)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureWithMeta {
    pub dataset_name: String,
    pub properties: Value,
    pub geometry_wkt: String,
}

/// Converts a GeoJSON geometry to its WKT representation.
///
/// This function takes a `Geometry` object from the `geojson` crate and converts it into a
/// Well-Known Text (WKT) representation. The conversion covers various geometry types such as
/// Point, MultiPoint, LineString, and others.
///
/// # Parameters
///
/// * `geom`: A reference to a `Geometry` object to be converted.
///
/// # Returns
///
/// A `Result` containing either the WKT representation as a `String` or an error if the conversion fails.
pub fn geometry_to_wkt(geom: &Geometry) -> Result<String> {
    match geom.value.clone() {
        geojson::Value::Point(c) => Ok(format!("POINT({} {})", c[0], c[1])),
        geojson::Value::MultiPoint(coords) => Ok(format!(
            "MULTIPOINT({})",
            coords
                .into_iter()
                .map(|p| format!("({} {})", p[0], p[1]))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        geojson::Value::LineString(coords) => Ok(format!(
            "LINESTRING({})",
            coords
                .into_iter()
                .map(|p| format!("{} {}", p[0], p[1]))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        geojson::Value::MultiLineString(lines) => Ok(format!(
            "MULTILINESTRING({})",
            lines
                .into_iter()
                .map(|line| format!(
                    "({})",
                    line.into_iter()
                        .map(|p| format!("{} {}", p[0], p[1]))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        geojson::Value::Polygon(polygons) => Ok(format!(
            "POLYGON({})",
            polygons
                .into_iter()
                .map(|ring| format!(
                    "({})",
                    ring.into_iter()
                        .map(|p| format!("{} {}", p[0], p[1]))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        geojson::Value::MultiPolygon(multipolygons) => Ok(format!(
            "MULTIPOLYGON({})",
            multipolygons
                .into_iter()
                .map(|poly| format!(
                    "({})",
                    poly.into_iter()
                        .map(|ring| format!(
                            "({})",
                            ring.into_iter()
                                .map(|p| format!("{} {}", p[0], p[1]))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        _ => anyhow::bail!("Unsupported geometry type"),
    }
}

/// Processes a GeoJSON file and uploads it to the database.
///
/// This function takes a database client, an input file path, and a table name. It opens the input
/// file, reads the GeoJSON data, and uploads it to the specified table using a COPY operation.
///
/// # Parameters
///
/// * `client`: A reference to a `Client` object from the `tokio_postgres` crate.
/// * `input_file`: The path to the GeoJSON file to process.
/// * `table_name`: The name of the table to upload the GeoJSON data to.
///
/// # Returns
///
/// A `Result` indicating success or an error.
///
/// # Errors
///
/// * `Box<dyn StdError>`: If an error occurs during the processing or upload process.
///
/// # Examples
///
/// ```
/// let client = Client::connect("host=localhost user=postgres password=postgres", "").await?;
/// let result = process_file(&client, "path/to/geojson.json", "my_table").await;
/// ```
pub async fn process_file(
    client: &Client,
    input_file: &str,
    table_name: &str,
) -> Result<(), Box<dyn StdError>> {
    eprintln!(
        "🔄 Attempting to process file: {}, table: {}",
        input_file, table_name
    );

    let file = File::open(input_file)
        .expect(format!("❌ Failed to open input file: {}", input_file).as_str());
    let reader = BufReader::new(file);
    let geojson: GeoJson = serde_json::from_reader(reader).expect("❌ Failed to parse GeoJSON");

    let features = match geojson {
        GeoJson::FeatureCollection(fc) => fc.features,
        _ => return Err("GeoJSON file does not contain a FeatureCollection".into()),
    };

    let mut file_count = 0;
    // Set up COPY operation
    let stmt = format!(
        "COPY {} (name, properties, geometry) FROM STDIN (FORMAT csv)",
        table_name
    );
    let mut sink = Box::pin(
        client
            .copy_in(&stmt)
            .await
            .expect("❌ Failed to start COPY operation"),
    );
    // For each row, build a CSV line and send it as a Vec<u8>:
    let name = "some_name";
    let properties = "{\"foo\": \"bar\"}";
    let geometry = "SRID=4326;POINT(1 2)";

    // ...
    let csv_line = format!(
        "{},{},{}\n",
        escape_csv_field(name),
        escape_csv_field(properties),
        escape_csv_field(geometry)
    );

    eprintln!("🔄 Processing features in {}", input_file);
    for (idx, feature) in features.into_iter().enumerate() {
        let name = match feature.id {
            Some(geojson::feature::Id::String(ref s)) => s.clone(),
            Some(geojson::feature::Id::Number(ref n)) => n.to_string(),
            None => format!("unknown_{}", idx),
        };
        let properties =
            serde_json::to_string(&feature.properties).expect("❌ Failed to serialize properties");
        let geometry = match feature.geometry {
            Some(ref geom) => geometry_to_wkt(geom).expect("❌ Failed to convert geometry to WKT"),
            None => "NULL".to_string(),
        };

        let csv_line = format!(
            "{},{},{}\n",
            escape_csv_field(&name),
            escape_csv_field(&properties),
            escape_csv_field(&geometry)
        );
        let bytes = BytesMut::from(csv_line.as_str());
        sink.send(bytes).await.expect("❌ Failed to send bytes");
    }

    eprintln!("⏳ Closing copy operation...");
    sink.close().await.expect("❌ Failed to close sink");
    eprintln!("✅ Copy operation completed successfully!!");
    Ok(())
}

/// Helper function to escape CSV fields
fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        // Escape quotes by doubling them and wrap in quotes
        let escaped = field.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        field.to_string()
    }
}
/// Returns the contained value of an `Option` if it exists, otherwise returns a default value.
///
/// This function takes an `Option` and a default value. If the `Option` contains a value,
/// that value is returned. Otherwise, the default value is returned. If the default
/// value is used, a warning message will be printed indicating the default is being used.
///
/// # Parameters
///
/// * `value`: The `Option` to unwrap.
/// * `default`: The default value to return if `value` is `None`.
/// * `value_name`: The name of the value being unwrapped, used in the warning message.
///
/// # Returns
///
/// The contained value if `Some`, otherwise `default`.
pub fn custom_unwrap_or<T: Display>(value: Option<T>, default: T, value_name: &str) -> T {
    match value {
        Some(v) => v,
        None => {
            eprintln!(
                "⚠️ No {} was given, using default value: {}",
                value_name, default
            );
            default
        }
    }
}
