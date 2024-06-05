use std::sync::Arc;

use anyhow::anyhow;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::resp::RespValue;
use crate::storage::Storage;

mod resp;
mod storage;

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

    let storage = Arc::new(RwLock::new(Storage::new()));

    loop {
        let (stream, _) = listener.accept().await?;

        let storage = storage.clone();
        tokio::spawn(async move {
            handle_connection(stream, storage).await;
        });
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, storage: Arc<RwLock<Storage>>) {
    println!("accepted new connection");

    tokio::spawn(async move {
        let mut resp_parser = resp::RespParser::new(stream);

        loop {
            let value = resp_parser.parse().await.unwrap();

            let result = match parse_command(value) {
                Ok((command, args)) => handle_command(command, args, storage.clone()).await,
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
                    RespValue::BulkString(Some(b"# Replication\nrole:master\n".to_vec()))
                }
                _ => RespValue::Error("unknown argument".to_string()),
            },
            _ => RespValue::Error("wrong number of arguments".to_string()),
        },
        _ => RespValue::Error("unknown command".to_string()),
    }
}
