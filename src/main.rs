mod parser;

use std::collections::{HashMap, hash_map::Entry};
use std::env;
use std::future::ready;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{
    Router,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
};
use chrono::{Datelike, Utc};
use clap::{ArgAction, Command, arg, value_parser};
use dotenv::dotenv;
use encoding_rs::{ISO_8859_15, UTF_8};
use log::{error, warn};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use redis::streams::StreamMaxlen;
use serde_json::json;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

fn resolve_year((_month, _date, _hour, _min, _sec): syslog_loose::IncompleteDate) -> i32 {
    let now = chrono::Utc::now();
    now.year()
}

async fn syslog_worker(
    mut rx: mpsc::Receiver<(chrono::DateTime<Utc>, Vec<u8>, SocketAddr)>,
    mut redis_client: impl redis::AsyncCommands,
    stream_key: String,
    maxlen: usize,
    include_raw: bool,
) {
    let histogram_timeshift = metrics::histogram!("log_messages_timeshift_millis");
    let counter_redis_errors = metrics::counter!("log_messages_redis_errors");

    while let Some((recv_ts, input, addr)) = rx.recv().await {
        let (utf8_input, encoding) = parser::decode_log_msg(&input);
        let parsed_msg = syslog_loose::parse_message_with_year(
            &utf8_input,
            resolve_year,
            syslog_loose::Variant::Either,
        );

        let protocol = match parsed_msg.protocol {
            syslog_loose::Protocol::RFC3164 => "RFC3164".to_string(),
            syslog_loose::Protocol::RFC5424(version) => format!("RFC5424_v{}", version),
        };
        let mut items = vec![
            (
                "recv_ts".to_string(),
                recv_ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            ),
            ("addr".to_string(), addr.to_string().into()),
            ("protocol".to_string(), protocol),
            ("encoding".to_string(), encoding.to_string()),
        ];
        if let Some(facility) = parsed_msg.facility {
            items.push(("facility".to_string(), facility.as_str().to_string()));
        }
        if let Some(severity) = parsed_msg.severity {
            items.push(("severity".to_string(), severity.as_str().to_string()));
        }
        if let Some(ts) = parsed_msg.timestamp {
            let ts_utc = ts.with_timezone(&chrono::Utc);
            items.push((
                "ts".to_string(),
                ts_utc.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            ));
            histogram_timeshift.record((recv_ts - ts_utc).as_seconds_f64() * 1_000.0);
        }
        if let Some(hostname) = parsed_msg.hostname {
            items.push(("hostname".to_string(), hostname.to_string()));
        }
        if let Some(appname) = parsed_msg.appname {
            items.push(("appname".to_string(), appname.to_string()));
        }
        if let Some(procid) = parsed_msg.procid {
            match procid {
                syslog_loose::ProcId::PID(pid) => {
                    items.push(("proc_pid".to_string(), format!("{}", pid)))
                }
                syslog_loose::ProcId::Name(name) => {
                    items.push(("proc_name".to_string(), name.to_string()))
                }
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
                            warn!(
                                "duplicate key in syslog structured data params: {}",
                                param.0
                            );
                            continue;
                        }
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
        if include_raw {
            // The string might not be valid UTF-8, but we do not use any functions from the std library
            // to process the string. FIXME convert to Vec<str, Vec<u8>>
            items.push(("raw".to_string(), unsafe {
                String::from_utf8_unchecked(input)
            }));
        }

        let result = redis_client
            .xadd_maxlen(&stream_key, StreamMaxlen::Approx(maxlen), "*", &items)
            .await;
        match result {
            Ok(()) => (),
            Err(err) => {
                counter_redis_errors.increment(1);
                error!("Could not send log message: {}", err);
            }
        }
    }
    unreachable!();
}

async fn handler_index() -> Html<&'static str> {
    Html("<h1>Metrics server for syslog publisher</h1>")
}

async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "404 error")
}

async fn start_metrics_server(metrics_addr: String, metrics_recorder: PrometheusHandle) {
    let app = Router::new()
        .route("/", get(handler_index))
        .route("/metrics", get(move || ready(metrics_recorder.render())))
        .fallback(handler_404);

    let listener = tokio::net::TcpListener::bind(metrics_addr).await.unwrap();
    println!("Serving metrics on: {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

fn setup_metrics_recorder() -> Result<PrometheusHandle> {
    const TIME_BUCKETS: &[f64] = &[
        0.0001, 0.001, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        100.0, 1000.0, 5_000.0, 10_000.0, 100_000.0, 500_000.0, 1_000_000.0,
        10_000_000.0, 100_000_000.0,
    ];

    const SIZE_BUCKETS: &[f64] = &[10.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 10_000.0];

    let builder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("log_messages_timeshift_millis".to_string()),
            TIME_BUCKETS,
        )?
        .set_buckets_for_metric(Matcher::Full("log_messages_size".to_string()), SIZE_BUCKETS)?;
    builder
        .install_recorder()
        .with_context(|| "can't install metrics recorder")
}

fn describe_metrics() {
    use metrics::Unit;
    metrics::describe_counter!(
        "log_messages_total",
        Unit::Count,
        "Total amount of received syslog messages"
    );
    metrics::describe_counter!(
        "log_messages_redis_errors",
        Unit::Count,
        "Amount of dropped messages due to Redis errors"
    );
    metrics::describe_histogram!(
        "log_messages_timeshift_millis",
        Unit::Milliseconds,
        "Offset between sending the syslog message and it being received by the syslog_publisher"
    );
    metrics::describe_histogram!(
        "log_messages_size",
        Unit::Bytes,
        "Size of the received syslog message"
    );
}

fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init();

    // The first invocation of the metrics macro registers registers them with the metrics recorder.
    // If however none of the metrics have been triggered yet, the metrics page will be empty.
    let metrics_recorder = setup_metrics_recorder()?;

    let matches = Command::new("syslog_publisher")
        .version("0.1")
        .about("Publish Syslog messages as Redis stream")
        .arg(
            arg!(-p --port <VALUE>)
                .default_value("514")
                .value_parser(value_parser!(u16))
                .help("Port to listen for incoming syslog messages"),
        )
        .arg(
            arg!(--"include-raw")
                .action(ArgAction::SetTrue)
                .help("Send the raw syslog message without decoding and parsing"),
        )
        .arg(
            arg!(-k --key <VALUE>)
                .required(true)
                .help("Name of Redis stream"),
        )
        .arg(
            arg!(--maxlen <VALUE>)
                .default_value("25000")
                .value_parser(value_parser!(usize))
                .help("Maximum length of Redis stream"),
        )
        .arg(
            arg!(--"redis-host" <VALUE>)
                .default_value("localhost")
                .help("Hostname of Redis server"),
        )
        .arg(
            arg!(--"redis-port" <VALUE>)
                .default_value("6379")
                .value_parser(value_parser!(u16))
                .help("Port of Redis server"),
        )
        .arg(
            arg!(--metrics <VALUE>)
                .default_value("127.0.0.1:3001")
                .help("Address of metrics server"),
        )
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
        addr: redis::ConnectionAddr::Tcp {
            0: redis_host,
            1: redis_port,
        },
        redis: redis::RedisConnectionInfo {
            db: 0,
            username: redis_username,
            password: redis_password,
            // connection manager can listen for updates and reconnect after a disconnect in RESP3 mode
            protocol: redis::ProtocolVersion::RESP3,
        },
    };
    let redis_config = redis::aio::ConnectionManagerConfig::new()
        .set_factor(2)
        .set_max_delay(1_000)
        .set_number_of_retries(25)
        .set_response_timeout(std::time::Duration::new(5, 0));

    let maxlen = matches.get_one::<usize>("maxlen").unwrap();
    let stream_key = matches.get_one::<String>("key").unwrap().clone();
    let metrics_addr = matches.get_one::<String>("metrics").unwrap().clone();
    let include_raw = matches.get_one::<bool>("include-raw").unwrap();

    let rt = Runtime::new()?;
    rt.block_on(async {
        let addr = format!(
            "0.0.0.0:{}",
            matches.get_one::<u16>("port").expect("port is required")
        );
        let socket = UdpSocket::bind(&addr).await?;
        println!("Listening for syslog on: {}", socket.local_addr()?);

        describe_metrics();
        tokio::spawn(start_metrics_server(metrics_addr, metrics_recorder));

        let redis_client = redis::Client::open(redis_conn_info)?;
        let redis_manager =
            redis::aio::ConnectionManager::new_with_config(redis_client, redis_config).await?;

        let (tx, rx) = mpsc::channel::<(chrono::DateTime<chrono::Utc>, Vec<u8>, SocketAddr)>(1_000);
        tokio::spawn(syslog_worker(
            rx,
            redis_manager,
            stream_key,
            *maxlen,
            *include_raw,
        ));

        let mut buf = [0; 65536];
        let counter_total_messages = metrics::counter!("log_messages_total");
        let histogram_messages_size = metrics::histogram!("log_messages_size");
        loop {
            let (len, addr) = socket.recv_from(&mut buf).await?;
            let recv_ts = Utc::now();
            counter_total_messages.increment(1);
            histogram_messages_size.record(len as f64);
            tx.send((recv_ts, buf[..len].to_vec(), addr)).await?;
        }
    })
}
