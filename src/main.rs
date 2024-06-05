use std::sync::Arc;

use anyhow::anyhow;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::resp::RespValue;
use crate::storage::Storage;

mod resp;
mod storage;

struct Settings {
    port: u16,
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = std::env::args().collect::<Vec<String>>();
    let args_hash = args
        .iter()
        .enumerate()
        .map(|(i, arg)| (arg.clone(), i))
        .collect::<std::collections::HashMap<String, usize>>();

    let port = args_hash
        .get("--port")
        .map(|i| args[i + 1].parse::<u16>().unwrap())
        .unwrap_or(6379);

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    let replicaof = args_hash.get("--replicaof").map(|i| args[i + 1].clone());

    let settings = Arc::new(Settings { port, replicaof });
    let storage = Arc::new(RwLock::new(Storage::new()));

    if let Some(replicaof) = &settings.replicaof {
        let (host, port) = replicaof.split_at(replicaof.find(' ').unwrap());
        let port = port.trim().parse::<u16>().unwrap();
        let mut stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        stream
            .write_all(
                &RespValue::Array(vec![RespValue::BulkString(Some(b"PING".to_vec()))]).to_bytes(),
            )
            .await?;

        stream
            .write_all(
                &RespValue::Array(vec![
                    RespValue::BulkString(Some(b"REPLCONF".to_vec())),
                    RespValue::BulkString(Some(b"listening-port".to_vec())),
                    RespValue::BulkString(Some(port.to_string().into_bytes())),
                ])
                .to_bytes(),
            )
            .await?;

        stream
            .write_all(
                &RespValue::Array(vec![
                    RespValue::BulkString(Some(b"REPLCONF".to_vec())),
                    RespValue::BulkString(Some(b"capa".to_vec())),
                    RespValue::BulkString(Some(b"psync2".to_vec())),
                ])
                .to_bytes(),
            )
            .await?;
    }

    loop {
        let (stream, _) = listener.accept().await?;

        let storage = storage.clone();
        let settings = settings.clone();
        tokio::spawn(async move {
            handle_connection(stream, storage, settings).await;
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    storage: Arc<RwLock<Storage>>,
    settings: Arc<Settings>,
) {
    println!("accepted new connection");

    tokio::spawn(async move {
        let mut resp_parser = resp::RespParser::new(stream);

        loop {
            let value = resp_parser.parse().await.unwrap();

            let result = match parse_command(value) {
                Ok((command, args)) => {
                    handle_command(command, args, storage.clone(), settings.clone()).await
                }
                Err(e) => RespValue::Error(e.to_string()),
            };

            resp_parser.write(result).await.unwrap();
        }
    });
}

fn parse_command(value: RespValue) -> Result<(String, Vec<RespValue>), anyhow::Error> {
    match value {
        RespValue::Array(a) => {
            let command = match a.first().unwrap().clone() {
                RespValue::BulkString(Some(s)) => String::from_utf8(s.clone())?.to_lowercase(),
                _ => return Err(anyhow!("Expected bulk string")),
            };
            let args = a.iter().skip(1).cloned().collect();

            Ok((command, args))
        }
        _ => Err(anyhow!("Expected array")),
    }
}

async fn handle_command(
    command: String,
    args: Vec<RespValue>,
    storage: Arc<RwLock<Storage>>,
    settings: Arc<Settings>,
) -> RespValue {
    match command.as_str() {
        "ping" => RespValue::SimpleString("PONG".to_string()),
        "echo" => args.first().unwrap().clone(),
        "set" => match args.as_slice() {
            [key, value] => {
                let mut storage = storage.write().await;
                storage.set(
                    String::from_utf8(key.to_bytes().clone()).unwrap(),
                    value.clone(),
                    None,
                );
                RespValue::SimpleString("OK".to_string())
            }
            [key, value, RespValue::BulkString(Some(argument)), RespValue::BulkString(Some(expiry))] => {
                match String::from_utf8(argument.clone())
                    .unwrap()
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "px" => {
                        let expiry = String::from_utf8(expiry.clone())
                            .unwrap()
                            .parse::<usize>()
                            .unwrap();
                        let mut storage = storage.write().await;
                        storage.set(
                            String::from_utf8(key.to_bytes().clone()).unwrap(),
                            value.clone(),
                            Some(expiry),
                        );
                        RespValue::SimpleString("OK".to_string())
                    }
                    _ => RespValue::Error("unknown argument".to_string()),
                }
            }
            _ => RespValue::Error("wrong number of arguments".to_string()),
        },
        "get" => {
            let mut storage = storage.write().await;
            match storage.get(&String::from_utf8(args[0].to_bytes().clone()).unwrap()) {
                Some(value) => value.clone(),
                None => RespValue::Null,
            }
        }
        "info" => match args.as_slice() {
            [] => RespValue::BulkString(Some(b"# Server\nversion:0.0.1\n".to_vec())),
            [RespValue::BulkString(Some(key))] => match String::from_utf8(key.clone())
                .unwrap()
                .to_ascii_lowercase()
                .as_str()
            {
                "replication" => {
                    let role = match settings.replicaof {
                        Some(_) => "slave",
                        None => "master",
                    };

                    RespValue::BulkString(Some(Vec::from(
                        format!("# Replication\nrole:{}\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n", role).as_bytes(),
                    )))
                }
                _ => RespValue::Error("unknown argument".to_string()),
            },
            _ => RespValue::Error("wrong number of arguments".to_string()),
        },
        "replconf" => RespValue::BulkString(Some(b"OK".to_vec())),
        _ => RespValue::Error("unknown command".to_string()),
    }
}
