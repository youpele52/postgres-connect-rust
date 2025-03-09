use crate::read;
use deadpool_postgres::{Manager, Pool, PoolError};
use tokio_postgres::{Config, Error, NoTls};

pub async fn new() -> Result<tokio_postgres::Client, Error> {
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

pub async fn new_pool() -> Result<Pool, Box<dyn std::error::Error>> {
    let config = read::Read::config_data().config;
    let mut cfg = Config::new();
    cfg.host(&config.host);
    cfg.user(&config.user);
    cfg.password(&config.password);
    cfg.dbname(&config.db_name);

    let manager = Manager::new(cfg, NoTls);
    let pool = Pool::builder(manager)
        .max_size(16) // Adjust based on your needs
        .build()?;

    Ok(pool)
}
