mod resp;

use anyhow::anyhow;
use tokio::net::TcpListener;
use crate::resp::RespValue;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");

                tokio::spawn(async move {
                    let mut resp_parser = resp::RespParser::new(stream);

                    loop {
                        let value = resp_parser.parse().await.unwrap();

                        let result = match parse_command(value) {
                            Ok((command, args)) => {
                                match command.as_str() {
                                    "ping" => RespValue::SimpleString("PONG".to_string()),
                                    "echo" => args.first().unwrap().clone(),
                                    _ => RespValue::Error("unknown command".to_string())
                                }
                            }
                            Err(e) => RespValue::Error(e.to_string())
                        };

                        resp_parser.write(result).await.unwrap();
                    }
                });
            }
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
    }
}

fn parse_command(value: RespValue) -> Result<(String, Vec<RespValue>), anyhow::Error> {
    match value {
        RespValue::Array(a) => {
            let command = match a.first().unwrap().clone() {
                RespValue::BulkString(Some(s)) => String::from_utf8(s.clone())?.to_lowercase(),
                _ => return Err(anyhow!("Expected bulk string"))
            };
            let args = a.iter().skip(1).cloned().collect();

            Ok((command, args))
        }
        _ => Err(anyhow!("Expected array"))
    }
}