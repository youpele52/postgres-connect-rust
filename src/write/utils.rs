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

// /// Stream a GeoJSON file for reading
// pub async fn open_geojson_file(
//     path: &Path,
// ) -> Result<Box<dyn AsyncRead + Unpin + Send>, Box<dyn StdError>> {
//     let file = File::open(path).await?;
//     let reader = BufReader::new(file);

//     Ok(Box::new(reader))
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureWithMeta {
    pub dataset_name: String,
    pub properties: Value,
    pub geometry_wkt: String,
}

/// GeoJSON parser
///
/// Converts GeoJSON geometry to WKT
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

pub async fn process_file(
    client: &Client,
    input_file: &str,
    table_name: &str,
) -> Result<(), Box<dyn StdError>> {
    println!(
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

    // let stream = Deserializer::from_reader(reader).into_iter::<geojson::Feature>();

    let file_name = input_file
        .split('/')
        .last()
        .unwrap()
        .split('.')
        .next()
        .unwrap();

    // let mut features: Vec<Value> = Vec::new();
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

    // Process each feature
    let dataset_name = "some_name";

    println!("🔄 Processing feature");
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

    // Convert to Bytes
    // let bytes = BytesMut::from(csv_line.as_str());
    // sink.send(bytes).await?;
    sink.close().await.expect("❌ Failed to close sink");
    Ok(())
}

// Helper function to escape CSV fields
fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        // Escape quotes by doubling them and wrap in quotes
        let escaped = field.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        field.to_string()
    }
}
