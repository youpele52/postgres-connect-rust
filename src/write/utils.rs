use serde_json::Value;
use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use tokio::fs;
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

