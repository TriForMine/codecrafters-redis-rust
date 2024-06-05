use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");

                tokio::spawn(async move {
                    let mut stream = stream;
                    let mut buffer = [0; 1024];

                    loop {
                        let bytes_read = stream.read(&mut buffer).await.unwrap();
                        if bytes_read == 0 {
                            println!("connection closed");
                            break;
                        }

                        stream.write(b"+PONG\r\n").await.unwrap();
                    }
                });
            }
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
    }
}
