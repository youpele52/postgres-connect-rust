use crate::read;
use tokio_postgres::{Error, NoTls};

pub async fn main() -> Result<tokio_postgres::Client, Error> {
    let config = read::Read::config_data().config;
    // Create connection string
    let connection_string: String = format!(
        "host={} port={} user={} password={} dbname={}",
        config.host, config.port, config.user, config.password, config.db_name
    );
    // Connect to the database
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("❌ Failed to connect to database!!");

    println!("✅ Connected to database: {}", connection_string);
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("❌ connection error: {}", e);
        }
    });

    return Ok(client);
}
