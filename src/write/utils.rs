use serde_json::Value;
use std::error::Error as StdError;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

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
