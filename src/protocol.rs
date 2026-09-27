use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::model::Message;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Command{
    CreateTopic {
        topic: String,
    },
    CreateSubscription {
        topic: String,
        subscription: String,
        ack_deadline_secs: u64,
    },
    Publish {
        topic: String,
        payload: Vec<u8>,
        attributes: HashMap<String, String>
    },
    Pull {
        topic: String,
        subscription: String
    },
    Ack {
        topic: String,
        subscription: String,
        message_id: String
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Message(Message),
    Error(String)
}

