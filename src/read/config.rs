use serde::Deserialize;
use std::fs;
use toml;

#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
}

impl Config {
    pub fn new(config_filename: String) -> Config {
        // Read the TOML file
        let config_data = fs::read_to_string(config_filename).expect("Unable to read file");

        // Parse the data into the struct
        let config: toml::Value = toml::de::from_str(&config_data).expect("Unable to parse TOML");

        // Access the "person" section
        let read_config: Config = toml::de::from_str(&config["config"].to_string())
            .expect("\n\nUnable to deserialize config\n\n");

        return read_config;
    }
}
