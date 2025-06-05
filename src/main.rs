use std::env;

use anyhow::{Context, Result};
use clap::{arg, Command};
use dotenv::dotenv;

fn main() -> Result<()> {
    let matches = Command::new("syslog_publisher")
        .version("0.1")
        .about("Publish Syslog messages as Redis stream")
        .arg(arg!(--"redis-host" <VALUE>).default_value("localhost"))
        .arg(arg!(--"redis-port" <VALUE>).default_value("6379"))
        .get_matches();

    dotenv().ok();
    let redis_username = match env::var("REDIS_USERNAME") {
        Ok(val) => Some(val),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err).with_context(|| "can't read env variable REDIS_USERNAME"),
    };
    let redis_password = match env::var("REDIS_PASSWORD") {
        Ok(val) => Some(val),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err).with_context(|| "can't read env variable REDIS_PASSWORD"),
    };

    Ok(())
}
