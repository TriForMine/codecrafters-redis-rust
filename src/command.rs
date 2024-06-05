use crate::resp::RespValue;

pub enum Command {
    Ping,
}

impl Command {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Command::Ping => {
                RespValue::Array(vec![RespValue::BulkString(Some(b"PING".to_vec()))]).to_bytes()
            }
        }
    }
}
