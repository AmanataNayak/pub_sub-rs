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

#[derive(Debug, Clone)]
pub enum DeliveryState {
    /// Message is waiting in queue to be fetched by a consumer.
    Ready,
    /// Message was fetched; contains the deadline(`Instant`) when visibility expires
    InFlight{
        deadline: Instant
    }
}