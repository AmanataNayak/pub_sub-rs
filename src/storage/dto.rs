use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotHeader {
    pub magic: [u8; 4],      // b"PSSN"
    pub version: u32,        // 1
    pub checkpoint_lsn: u64, // LSN threshold for this snapshot
    pub timestamp_ms: u64,   // System time when snapshot was captured
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeadLetterPolicyState {
    pub dead_letter_queue: String,
    pub max_delivery_attempts: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PushConfigState {
    pub push_endpoint: String,
    pub headers: HashMap<String, String>,
    pub timeout_secs: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriptionState {
    pub name: String,
    pub ack_deadline_secs: u64,
    pub push_config: Option<PushConfigState>,
    pub batch_size: usize,
    pub max_outstanding_messages: Option<usize>,
    pub message_ttl_secs: Option<u64>,
    pub dropped_messages: u64,
    pub dead_letter_policy: Option<DeadLetterPolicyState>,
    pub ready_queue: VecDeque<String>,
    pub in_flight: HashMap<String, u64>, // msg_id -> deadline_epoch_ms
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicState {
    pub name: String,
    pub subscriptions: HashMap<String, SubscriptionState>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistentMessage {
    pub id: String,
    pub payload: Vec<u8>,
    pub attributes: HashMap<String, String>,
    pub delivery_attempt: u32,
    pub created_at_ms: u64, // Unix epoch milliseconds
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EngineSnapshot {
    pub header: SnapshotHeader,
    pub topics: HashMap<String, TopicState>,
    pub messages: HashMap<String, PersistentMessage>,
}