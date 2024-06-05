use std::collections::HashMap;

use tokio::time::Instant;

use crate::resp::RespValue;

pub struct Data {
    pub value: RespValue,
    pub created: Instant,
    pub expiry: Option<usize>,
}

pub struct Storage {
    pub data: HashMap<String, Data>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: RespValue, expiry: Option<usize>) {
        let created = Instant::now();
        self.data.insert(
            key,
            Data {
                value,
                created,
                expiry,
            },
        );
    }

    pub fn get(&mut self, key: &str) -> Option<RespValue> {
        let data = self.data.get(key)?;

        if let Some(expiry) = data.expiry {
            if data.created.elapsed().as_millis() > expiry as u128 {
                self.data.remove(key);
                return None;
            }
        }

        Some(data.value.clone())
    }
}
