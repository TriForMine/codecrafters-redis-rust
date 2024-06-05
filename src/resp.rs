use tokio::{net::TcpStream, io::{AsyncReadExt, AsyncWriteExt}};
use bytes::{Buf, BytesMut};
use anyhow::Result;

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(s) => format!("-{}\r\n", s).into_bytes(),
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespValue::BulkString(Some(s)) => {
                let mut resp = format!("${}\r\n", s.len()).into_bytes();
                resp.extend_from_slice(s);
                resp.extend_from_slice(b"\r\n");
                resp
            }
            RespValue::BulkString(None) => "$-1\r\n".bytes().collect(),
            RespValue::Array(a) => {
                let mut resp = format!("*{}\r\n", a.len()).into_bytes();
                for v in a {
                    resp.extend_from_slice(&v.to_bytes());
                }
                resp
            }
        }
    }
}

pub struct RespParser {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespParser {
    pub fn new(stream: TcpStream) -> Self {
        RespParser {
            stream,
            buffer: BytesMut::with_capacity(1024),
        }
    }

    pub async fn parse(&mut self) -> Result<RespValue> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;

        if bytes_read == 0 {
            return Err(anyhow::anyhow!("connection closed"));
        }

        if let Ok((resp, _)) = parse_single(self.buffer.split()) {
            Ok(resp)
        } else {
            Err(anyhow::anyhow!("incomplete response"))
        }
    }

    pub async fn write(&mut self, resp: RespValue) -> Result<()> {
        self.stream.write_all(&resp.to_bytes()).await?;
        Ok(())
    }
}


fn parse_single(buffer: BytesMut) -> Result<(RespValue, usize)> {
    println!("buffer: {:?}", String::from_utf8(buffer.to_vec()).unwrap());
    match buffer[0] {
        b'+' => parse_simple_string(&buffer[1..]),
        b'-' => parse_error(&buffer[1..]),
        b':' => parse_integer(&buffer[1..]),
        b'$' => parse_bulk_string(&buffer[1..]),
        b'*' => parse_array(&buffer[1..]),
        _ => Err(anyhow::anyhow!("invalid response")),
    }
}

fn parse_simple_string(buffer: &[u8]) -> Result<(RespValue, usize)> {
    println!("string: {:?}", buffer);
    if let Some((line, len)) = read_until_crlf(buffer) {
        let s = String::from_utf8(line[1..len - 2].to_vec())?;
        Ok((RespValue::SimpleString(s), len))
    } else {
        Err(anyhow::anyhow!("incomplete response"))
    }
}

fn parse_error(buffer: &[u8]) -> Result<(RespValue, usize)> {
    if let Some((line, len)) = read_until_crlf(buffer) {
        let s = String::from_utf8(line[1..len - 2].to_vec())?;
        Ok((RespValue::Error(s), len))
    } else {
        Err(anyhow::anyhow!("incomplete response"))
    }
}

fn parse_integer(buffer: &[u8]) -> Result<(RespValue, usize)> {
    if let Some((line, len)) = read_until_crlf(buffer) {
        let s = String::from_utf8(line[1..len - 2].to_vec())?;
        let i = s.parse()?;
        Ok((RespValue::Integer(i), len))
    } else {
        Err(anyhow::anyhow!("incomplete response"))
    }
}

fn parse_bulk_string(buffer: &[u8]) -> Result<(RespValue, usize)> {
    if let Some((line, len)) = read_until_crlf(buffer) {
        let s = String::from_utf8(line[0..len - 2].to_vec())?;
        let string_len = s.parse::<i64>()?;

        if string_len == -1 {
            return Ok((RespValue::BulkString(None), len));
        }

        let total_len = len as usize + string_len as usize;

        if buffer.len() < total_len {
            return Err(anyhow::anyhow!("incomplete response {:?}", buffer.len()));
        }

        let bulk_string = buffer[len..total_len].to_vec();

        Ok((RespValue::BulkString(Some(bulk_string)), total_len + 1))
    } else {
        Err(anyhow::anyhow!("incomplete response"))
    }
}

fn parse_array(buffer: &[u8]) -> Result<(RespValue, usize)> {
    if let Some((line, len)) = read_until_crlf(buffer) {
        let s = String::from_utf8(line[0..len - 2].to_vec())?;
        let array_len = s.parse::<i64>()?;

        if array_len == -1 {
            return Ok((RespValue::Array(vec![]), len as usize));
        }

        let mut total_len = len;
        let mut array = vec![];
        let mut buf = &buffer[len..];

        for _ in 0..array_len {
            let (resp, len) = parse_single(BytesMut::from(buf))?;

            array.push(resp);
            total_len += len + 2;
            buf = &buf[len + 2..];
        }

        Ok((RespValue::Array(array), total_len))
    } else {
        Err(anyhow::anyhow!("incomplete response"))
    }
}

fn read_until_crlf(buffer: &[u8]) -> Option<(BytesMut, usize)> {
    if let Some(pos) = buffer.windows(2).position(|b| b == b"\r\n") {
        let mut line = BytesMut::with_capacity(pos + 2);
        line.extend_from_slice(&buffer[..pos]);
        Some((line, pos + 2))
    } else {
        None
    }
}