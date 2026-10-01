use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub id: String,
    pub payload: Vec<u8>,
    pub attributes: HashMap<String, String>,
    pub delivery_attempt: u32
}

impl Message {
    pub fn new(payload: Vec<u8>, attributes: HashMap<String, String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            payload,
            attributes,
            delivery_attempt: 0
        }
    }
}

impl From<Message> for crate::pubsub::Message {
    fn from(msg: Message) -> Self {
        Self {
            id: msg.id,
            payload: msg.payload,
            attributes: msg.attributes,
            delivery_attempt: msg.delivery_attempt
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterPolicy {
    pub dead_letter_queue: String,
    pub max_delivery_attempts: u32,
}
#[derive(Debug, Clone)]
pub struct PushConfig {
    pub push_endpoint: String,
    pub headers: HashMap<String, String>,
    pub timeout_secs: u64 // Default to 5s
}

