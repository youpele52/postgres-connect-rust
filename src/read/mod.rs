pub mod args;
pub mod config;
pub mod db;
pub mod queries;

pub struct Read {
    pub config: config::Config,
    pub db_url: String,
}

impl Read {
    pub fn config_data() -> Read {
        let args_: args::Args = args::Args::new();
        let config: config::Config = config::Config::new(args_.config_filename);
        let db_url = format!(
            "postgresql://{}:{}@{}:{}/{}",
            config.user, config.password, config.host, config.port, config.db_name
        );
        return Read { config, db_url };
    }
}
