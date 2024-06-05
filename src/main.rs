use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::resp::RespValue;

mod resp;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let storage = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (stream, _) = listener.accept().await?;

        let storage = storage.clone();
        tokio::spawn(async move {
            handle_connection(stream, storage).await;
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    storage: Arc<RwLock<HashMap<String, RespValue>>>,
) {
    println!("accepted new connection");

    tokio::spawn(async move {
        let mut resp_parser = resp::RespParser::new(stream);

        loop {
            let value = resp_parser.parse().await.unwrap();

            let result = match parse_command(value) {
                Ok((command, args)) => match command.as_str() {
                    "ping" => RespValue::SimpleString("PONG".to_string()),
                    "echo" => args.first().unwrap().clone(),
                    "set" => {
                        let mut storage = storage.write().await;
                        storage.insert(
                            String::from_utf8(args[0].to_bytes().clone()).unwrap(),
                            args[1].clone(),
                        );
                        RespValue::SimpleString("OK".to_string())
                    }
                    "get" => {
                        let storage = storage.read().await;
                        match storage.get(&String::from_utf8(args[0].to_bytes().clone()).unwrap()) {
                            Some(value) => value.clone(),
                            None => RespValue::Null,
                        }
                    }
                    _ => RespValue::Error("unknown command".to_string()),
                },
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
