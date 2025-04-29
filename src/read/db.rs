use crate::read;
use deadpool_postgres::{Manager, Pool, PoolError};
use tokio_postgres::{Config, Error, NoTls};

pub async fn new(
    pool: Option<bool>,
) -> Result<(tokio_postgres::Client, Option<Pool>), Box<dyn std::error::Error>> {
    let config = read::Read::config_data().config;
    let use_pool = pool.unwrap_or(false);
    println!("Using pool: {}", use_pool);

    let connection_string = format!(
        "host={} port={} user={} password={} dbname={}",
        config.host, config.port, config.user, config.password, config.db_name
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("❌ Failed to connect to database!!");

    println!("✅ Connected to database: {}", connection_string);
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("❌ connection error: {}", e);
        }
    });

    if use_pool {
        let mut cfg = Config::new();
        cfg.host(&config.host);
        cfg.user(&config.user);
        cfg.password(&config.password);
        cfg.dbname(&config.db_name);
        let manager = Manager::new(cfg, NoTls);
        let pool = Pool::builder(manager)
            .max_size(16)
            .build()
            .expect("❌ Failed to build pool");
        Ok((client, Some(pool)))
    } else {
        Ok((client, None))
    }
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
