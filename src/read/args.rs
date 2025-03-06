use std::env;

pub struct Args {
    pub config_filename: String,
}

impl Args {
    pub fn new() -> Args {
        let args: Vec<String> = env::args().collect();
        
        // Use the provided config filename or default to "files/config.toml"
        if args.len() >= 2 {
            Args {
                config_filename: args[1].to_string(),
            }
        } else {
            println!("No config file specified, using default: files/config.toml");
            Args {
                config_filename: String::from("files/config.toml"),
            }
        }
    }
}
