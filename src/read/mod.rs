pub mod args;
pub mod config;
pub mod db;
pub mod queries;

pub struct Read {
    pub config: config::Config,
}

impl Read {
    pub fn config_data() -> Read {
        let args_: args::Args = args::Args::new();
        let config_: config::Config = config::Config::new(args_.config_filename);
        return Read { config: config_ };
    }
}
