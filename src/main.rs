use std::env;
use std::net::SocketAddr;
use std::collections::{HashMap, hash_map::Entry};

use anyhow::{Context, Result};
use chrono::Datelike;
use clap::{arg, Command, value_parser};
use dotenv::dotenv;
use redis::{AsyncCommands, streams::StreamMaxlen};
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use log::{error, warn};
use serde_json::json;

fn resolve_year((_month, _date, _hour, _min, _sec): syslog_loose::IncompleteDate) -> i32 {
    let now = chrono::Utc::now();
    now.year()
}

async fn syslog_worker<Tz: chrono::TimeZone>(mut rx: mpsc::Receiver::<(chrono::DateTime<Tz>, Vec<u8>, SocketAddr)>, mut redis_client: redis::aio::MultiplexedConnection, stream_key: String, maxlen: usize) -> () {
    while let Some((recv_ts, input, addr)) = rx.recv().await {
        let utf8_input = String::from_utf8_lossy(&input);
        let parsed_msg = syslog_loose::parse_message_with_year(&utf8_input, resolve_year, syslog_loose::Variant::Either);

        let protocol = match parsed_msg.protocol {
            syslog_loose::Protocol::RFC3164 => "RFC3164".to_string(),
            syslog_loose::Protocol::RFC5424(version) => format!("RFC5424_v{}", version),
        };
        let mut items = vec![
            ("recv_ts".to_string(), recv_ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
            ("addr".to_string(), addr.to_string().into()),
            ("protocol".to_string(), protocol),
        ];
        if let Some(facility) = parsed_msg.facility {
            items.push(("facility".to_string(), facility.as_str().to_string()));
        }
        if let Some(severity) = parsed_msg.severity {
            items.push(("severity".to_string(), severity.as_str().to_string()));
        }
        if let Some(ts) = parsed_msg.timestamp {
            items.push(("ts".to_string(), ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)));
        }
        if let Some(hostname) = parsed_msg.hostname {
            items.push(("hostname".to_string(), hostname.to_string()));
        }
        if let Some(appname) = parsed_msg.appname {
            items.push(("appname".to_string(), appname.to_string()));
        }
        if let Some(procid) = parsed_msg.procid {
            match procid {
                syslog_loose::ProcId::PID(pid) => items.push(("proc_pid".to_string(), format!("{}", pid))),
                syslog_loose::ProcId::Name(name) => items.push(("proc_name".to_string(), name.to_string())),
            }
        }
        if let Some(msg_id) = parsed_msg.msgid {
            items.push(("msgid".to_string(), msg_id.to_string()));
        }
        if !parsed_msg.structured_data.is_empty() {
            let mut json_elements = Vec::with_capacity(parsed_msg.structured_data.len());
            for data_entry in parsed_msg.structured_data {
                let mut json_params = HashMap::with_capacity(data_entry.params.len());
                for param in data_entry.params {
                    match json_params.entry(param.0) {
                        Entry::Occupied(_) => {
                            warn!("duplicate key in syslog structured data params: {}", param.0);
                            continue;
                        },
                        Entry::Vacant(vacant_entry) => vacant_entry.insert(param.1),
                    };
                }

                json_elements.push(json!({
                    "id": data_entry.id,
                    "params": json!(json_params),
                }));
            }

            items.push(("data".to_string(), json!(json_elements).to_string()));
        }
        items.push(("msg".to_string(), parsed_msg.msg.to_string()));

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

        let (tx, rx) = mpsc::channel::<(chrono::DateTime<chrono::Utc>, Vec<u8>, SocketAddr)>(1_000);
        tokio::spawn(syslog_worker(rx, conn, stream_key, *maxlen));

        let mut buf = [0; 1024];
        loop {
            let (len, addr) = socket.recv_from(&mut buf).await?;
            let recv_ts = chrono::Utc::now();
            tx.send((recv_ts, buf[..len].to_vec(), addr)).await?;
        }
    })
}
