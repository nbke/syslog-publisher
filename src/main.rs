use std::env;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::{arg, Command, value_parser};
use dotenv::dotenv;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

async fn syslog_worker(mut rx: mpsc::Receiver::<(Vec<u8>, SocketAddr)>) -> () {
    println!("started worker");
    while let Some((bytes, addr)) = rx.recv().await {
        println!("{:?} bytes received from {:?}", bytes.len(), addr);
    }
    unreachable!();
}

fn main() -> Result<()> {
    let matches = Command::new("syslog_publisher")
        .version("0.1")
        .about("Publish Syslog messages as Redis stream")
        .arg(arg!(-p --port <VALUE>).default_value("514").value_parser(value_parser!(u16)))
        .arg(arg!(--"redis-host" <VALUE>).default_value("localhost"))
        .arg(arg!(--"redis-port" <VALUE>).default_value("6379").value_parser(value_parser!(u16)))
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

    let rt  = Runtime::new()?;
    rt.block_on(async {
        let addr= format!("0.0.0.0:{}", matches.get_one::<u16>("port").expect("port is required"));
        let socket = UdpSocket::bind(&addr).await?;
        println!("Listening on: {}", socket.local_addr()?);

        let (tx, mut rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(1_000);
        tokio::spawn(syslog_worker(rx));

        let mut buf = [0; 1024];
        loop {
            println!("waiting for syslog");
            let (len, addr) = socket.recv_from(&mut buf).await?;
            tx.send((buf[..len].to_vec(), addr)).await?;
        }
    })
}
