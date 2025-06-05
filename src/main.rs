use std::env;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::{arg, Command, value_parser};
use dotenv::dotenv;
use redis::{AsyncCommands, streams::StreamMaxlen};
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use log::error;

async fn syslog_worker(mut rx: mpsc::Receiver::<(Vec<u8>, SocketAddr)>, mut redis_client: redis::aio::MultiplexedConnection, stream_key: String, maxlen: usize) -> () {
    while let Some((msg, addr)) = rx.recv().await {
        let items = vec![("msg".to_string(), msg), ("addr".to_string(), addr.to_string().into())];
        let result= redis_client.xadd_maxlen(&stream_key, StreamMaxlen::Approx(maxlen), "*", &items).await;
        match result {
            Ok(()) => (),
            Err(err) => error!("Could not send log message: {}", err),
        }
    }
    unreachable!();
}

fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init();

    let matches = Command::new("syslog_publisher")
        .version("0.1")
        .about("Publish Syslog messages as Redis stream")
        .arg(arg!(-p --port <VALUE>).default_value("514").value_parser(value_parser!(u16)).help("Port to listen for incoming syslog messages"))
        .arg(arg!(-k --key <VALUE>).required(true).help("Name of Redis stream"))
        .arg(arg!(--maxlen <VALUE>).default_value("25000").value_parser(value_parser!(usize)).help("Maximum length of Redis stream"))
        .arg(arg!(--"redis-host" <VALUE>).default_value("localhost").help("Hostname of Redis server"))
        .arg(arg!(--"redis-port" <VALUE>).default_value("6379").value_parser(value_parser!(u16)).help("Port of Redis server"))
        .get_matches();

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

    // Hostname and Port for Redis have a default value and are thus always present
    let redis_host = matches.get_one::<String>("redis-host").unwrap().clone();
    let redis_port = *matches.get_one::<u16>("redis-port").unwrap();
    let redis_conn_info = redis::ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp { 0: redis_host, 1: redis_port},
        redis: redis::RedisConnectionInfo { db: 0, username: redis_username, password: redis_password, protocol: redis::ProtocolVersion::RESP2 },
    };
    let maxlen = matches.get_one::<usize>("maxlen").unwrap();
    let stream_key = matches.get_one::<String>("key").unwrap().clone();

    let rt  = Runtime::new()?;
    rt.block_on(async {
        let addr= format!("0.0.0.0:{}", matches.get_one::<u16>("port").expect("port is required"));
        let socket = UdpSocket::bind(&addr).await?;
        println!("Listening on: {}", socket.local_addr()?);

        let client = redis::Client::open(redis_conn_info).unwrap();
        let conn = client.get_multiplexed_async_connection().await?;

        let (tx, rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(1_000);
        tokio::spawn(syslog_worker(rx, conn, stream_key, *maxlen));

        let mut buf = [0; 1024];
        loop {
            let (len, addr) = socket.recv_from(&mut buf).await?;
            tx.send((buf[..len].to_vec(), addr)).await?;
        }
    })
}
