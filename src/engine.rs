/// Subscription Tracking

use std::collections::HashMap;
use std::time::{
    Duration,
    Instant,
};
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use crate::model::{DeliveryState, Message, DeadLetterPolicy, PullRequest, PushConfig};
use crate::errors::{PubSubError};
use crate::wal::{WalEntry, WalManager};
use crate::push::spawn_push_workers;


pub struct Subscription {
    pub name: String,
    pub ack_deadline: Duration,
    // Store message data alongside its current state
    messages: HashMap<String, (Message, DeliveryState)>,
    pub push_config: Option<PushConfig>,
    pub batch_size: usize,
    pub max_outstanding_messages: Option<usize>, // None
    pub dropped_messages: u64,
    pub dead_letter_policy: Option<DeadLetterPolicy>,
    pub notify: Arc<Notify>
}

impl Subscription {
    pub fn new(name: String, ack_deadline: Duration, batch_size: usize, max_outstanding_messages: Option<usize>, dead_letter_policy: Option<DeadLetterPolicy>) -> Self {

        Self {
            name,
            ack_deadline,
            messages: HashMap::new(),
            batch_size,
            dropped_messages: 0,
            max_outstanding_messages,
            dead_letter_policy,
            push_config: None,
            notify: Arc::new(Notify::new())
        }
    }

    pub fn new_push(name: String, ack_deadline: Duration, push_config: PushConfig, max_outstanding_messages: Option<usize>, dead_letter_policy: Option<DeadLetterPolicy>) -> Self {
        Self {
            name,
            ack_deadline,
            messages: HashMap::new(),
            batch_size: 1,
            dropped_messages: 0,
            max_outstanding_messages,
            dead_letter_policy,
            push_config: Some(push_config),
            notify: Arc::new(Notify::new())
        }
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Expose the notify handlers so stream handers can wait on it
    pub fn notify_handler(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    /// Called when a topic fan-outs a new message to this subscription.
    pub fn push(&mut self, msg: Message) -> bool {
        match self.max_outstanding_messages {
            Some(cap) if self.len() >= cap => {
                self.dropped_messages += 1;
                false
            },
            _ => {
                self.messages.insert(msg.id.clone(), (msg, DeliveryState::Ready));
                self.notify.notify_waiters();
                true
            }
        }
    }

    /// Pulls message up to request_batch_size
    /// Falls back to self.batch_size if request_batch_size is None or 0
    pub fn pull_batch(&mut self, request_batch_size: Option<usize>, topic_name: &str) -> PullRequest {
        let max_items = match request_batch_size {
            Some(n) if n > 0 => n,
            _ => self.batch_size,
        };

        let mut batch = Vec::with_capacity(max_items);
        let now = Instant::now();
        let mut keys_to_remove = Vec::new();
        let mut poison_messages = Vec::new();

        for (id, (msg, state)) in self.messages.iter_mut() {


            let is_available = match state {
                DeliveryState::Ready => true,
                DeliveryState::InFlight { deadline} => now >= *deadline
            };


            if is_available {
                if let Some(ref dlp) = self.dead_letter_policy {
                    if msg.delivery_attempt >= dlp.max_delivery_attempts {
                        // Mark key for removal from HashMap
                        keys_to_remove.push(id.clone());

                        // Enrich metadata attributes
                        msg.attributes.insert(
                            "x-dead-letter-source-subscription".to_string(),
                            self.name.clone()
                        );

                        msg.attributes.insert(
                            "x-dead-letter-delivery-attempts".to_string(),
                            msg.delivery_attempt.to_string()
                        );

                        msg.attributes.insert(
                            "x-dead-letter-source-topic".to_string(),
                            topic_name.to_string()
                        );

                        msg.attributes.insert(
                            "x-dead-letter-original-message-id".to_string(),
                            msg.id.clone()
                        );
                        poison_messages.push(msg.clone());
                        continue;
                    }
                }
                if batch.len() < max_items {
                    msg.delivery_attempt += 1;
                    *state = DeliveryState::InFlight {
                        deadline: now + self.ack_deadline, //language= update the deadline
                    };
                    batch.push(msg.clone());
                }
            }
        }

        for key in keys_to_remove {
            self.messages.remove(&key);
        }
        PullRequest {
            batch,
            poison_messages
        }
    }

    /// Remove the message in case of acknowledgment
    pub fn ack(&mut self, message_id: &str) -> bool{
        self.messages.remove(message_id).is_some()
    }

    pub fn contains_key(&self, message_id: &str) -> bool {
        self.messages.contains_key(message_id)
    }
}

pub struct Topic {
    pub name: String,
    // Maps subscription name to its Subscription state
    subscription: HashMap<String, Subscription>
}

impl Topic {
    pub fn new(name: String) -> Self {
        Self {
            name,
            subscription: HashMap::new()
        }
    }

    /// Register a new subscription under this topic
    pub fn create_subscription(&mut self, name: &str, ack_deadline: Duration, batch_size: usize, max_outstanding_messages: Option<usize>, dead_letter_policy: Option<DeadLetterPolicy>,) -> bool {
        if self.subscription.contains_key(name) {
            return false; // Already exists
        }
        let sub = Subscription::new(
            name.to_string(),
            ack_deadline,
            batch_size,
            max_outstanding_messages,
            dead_letter_policy
        );
        self.subscription.insert(name.to_string(), sub);
        true
    }

    pub fn create_push_subscription(&mut self, name: &str, ack_deadline: Duration, push_config: PushConfig, max_outstanding_messages: Option<usize>, dead_letter_policy: Option<DeadLetterPolicy>) -> bool {
        if self.subscription.contains_key(name) {
            return false; // Already exits
        }

        let sub = Subscription::new_push(
            name.to_string(),
            ack_deadline,
            push_config,
            max_outstanding_messages,
            dead_letter_policy
        );

        self.subscription.insert(name.to_string(), sub);

        true
    }

    // Fan-out publish: pushes a clone of the message to every subscription.
    pub fn publish(&mut self, msg: Message) -> String {
        for sub in self.subscription.values_mut() {
            sub.push(msg.clone());
        }
        msg.id
    }

    /// Helper to get a mutable reference to a specific subscription
    pub fn get_subscription_mut(&mut self, name: &str) -> Option<&mut Subscription> {
        self.subscription.get_mut(name)
    }

    pub fn get_subscription(&self, name: &str) -> Option<&Subscription> {
        self.subscription.get(name)
    }
}

#[derive(Clone)]
pub struct Engine {
    // Arc allows multiple thread to own a reference.
    // RwLock allows multiple concurrent readers or one exclusive writer
    // topics: Arc<RwLock<HashMap<String, Topic>>>,
    // Each topic is wrapped in its own Arc<RwLoc<...>>
    topics: Arc<RwLock<HashMap<String, Arc<RwLock<Topic>>>>>,
    wal: Option<Arc<WalManager>>
}

impl Engine {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            wal: None,
        }
    }

    pub fn with_wal(wal: Arc<WalManager>) -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            wal: Some(wal)
        }
    }

    // private helper method on engine that fetches teh Arc<RwLock<Topic>> and immediately
    // drops the outer map lock
    fn get_topic(&self, topic_name: &str) -> Result<Arc<RwLock<Topic>>, PubSubError> {
        let topics = self.topics.read().unwrap();

        // .cloned() clones the Arc pointer (cheap counter increment), NOT the Topic itself
        topics
            .get(topic_name)
            .cloned()
            .ok_or_else(|| PubSubError::TopicNotFound(topic_name.to_string()))
    } // Outer `topics` read lock is dropped right here!

    /// Creates a new topic.
    pub fn create_topic(&self, name: &str) -> Result<(), PubSubError> {
        let mut topics = self.topics.write().unwrap();
        if topics.contains_key(name) {
            return Err(PubSubError::TopicAlreadyExist(name.to_string()));
        }
        if let Some(wal) = &self.wal {
            let entry = WalEntry::CreateTopic {
                topic: name.to_string()
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }
        let topic = Arc::new(RwLock::new(Topic::new(name.to_string())));
        topics.insert(name.to_string(), topic);
        Ok(())
    }

    /// Creates a subscription under a topic.
    pub fn create_subscription(&self, topic_name: &str, sub_name: &str, ack_deadline: Duration, batch_size: Option<usize>, max_outstanding_messages: Option<usize>, dead_letter_queue: Option<String>, max_delivery_attempts: Option<u32>) -> Result<(), PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        // Build DLQ Policy ONLY if a dead_letter_queue was explicitly provided
        let dead_letter_policy = if let Some(dlq_name) = dead_letter_queue {
            if self.get_topic(&dlq_name).is_err() {
                return Err(PubSubError::TopicNotFound(dlq_name))
            }

            Some(DeadLetterPolicy {
                dead_letter_queue: dlq_name,
                max_delivery_attempts: max_delivery_attempts.unwrap_or(5)
            })

        } else {
            None
        };
        let mut topic = topic_arc.write().unwrap();

        if topic.subscription.contains_key(sub_name) {
            return Err(PubSubError::SubscriptionAlreadyExists(sub_name.to_string()));
        }

        let batch_size = batch_size.unwrap_or(1);
        if let Some(wal) = &self.wal {
            let entry = WalEntry::CreateSubscription {
                topic: topic_name.to_string(),
                subscription: sub_name.to_string(),
                ack_deadline_sec: ack_deadline.as_secs(),
                batch_size,
                max_outstanding_messages,
                dead_letter_queue: dead_letter_policy.as_ref().map(|p| p.dead_letter_queue.clone()),
                max_delivery_attempts,
                push_endpoint: None,
                timeout_secs: None,
                headers: None
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }

        topic.create_subscription(
            sub_name,
            ack_deadline,
            batch_size,
            max_outstanding_messages,
            dead_letter_policy,
        );


        Ok(())
    }

    pub fn create_push_subscription(&self, topic_name: &str, sub_name: &str, push_endpoint: &str, headers: HashMap<String, String>, timeout_secs: Option<u64>, ack_deadline: Duration, max_outstanding_messages: Option<usize>, dead_letter_queue: Option<String>, max_delivery_attempts: Option<u32>) -> Result<(), PubSubError> {
        // Validate DLQ
        let dead_letter_policy = if let Some(dlq_name) = dead_letter_queue {
            if self.get_topic(&dlq_name).is_err() {
                return Err(PubSubError::TopicNotFound(dlq_name))
            }

            Some(DeadLetterPolicy {
                dead_letter_queue: dlq_name,
                max_delivery_attempts: max_delivery_attempts.unwrap_or(5)
            })

        } else {
            None
        };

        let push_config = PushConfig {
            push_endpoint: push_endpoint.to_string(),
            headers,
            timeout_secs: timeout_secs.unwrap_or(5),
        };

        if let Some(wal) = &self.wal {
            let entry = WalEntry::CreateSubscription {
                topic: topic_name.to_string(),
                subscription: sub_name.to_string(),
                ack_deadline_sec: ack_deadline.as_secs(),
                batch_size: 1,
                max_outstanding_messages,
                dead_letter_queue: dead_letter_policy.as_ref().map(|p| p.dead_letter_queue.clone()),
                max_delivery_attempts,
                push_endpoint: Some(push_config.push_endpoint.clone()),
                timeout_secs: Some(push_config.timeout_secs),
                headers: Some(push_config.headers.clone()),
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }

        let notify = {
            let mut topic_arc = self.get_topic(topic_name)?;
            let mut topic = topic_arc.write().unwrap();

            if topic.subscription.contains_key(sub_name) {
                return Err(PubSubError::SubscriptionAlreadyExists(sub_name.to_string()));
            }

            topic.create_push_subscription(
                sub_name,
                ack_deadline,
                push_config.clone(),
                max_outstanding_messages,
                dead_letter_policy,
            );
            topic.get_subscription(sub_name).unwrap().notify_handler()
        };

        // Spawn the background push worker
        spawn_push_workers(
            topic_name.to_string(),
            sub_name.to_string(),
            push_config.clone(),
            notify,
            self.clone()
        );

        Ok(())
    }

    /// Fan-Out publish to all subscription under a topic.
    pub fn publish(&self, topic_name: &str, msg: Message) -> Result<String, PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        let mut topic = topic_arc.write().unwrap();

        if let Some(wal) = &self.wal {
            let entry = WalEntry::Publish {
                topic: topic_name.to_string(),
                message: msg.clone()
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }

        let msg_id = topic.publish(msg);
        Ok(msg_id)
    }

    /// Pulls a message from a specific subscription.
    pub fn pull_batch(&self, topic_name: &str, sub_name: &str, batch_size_override: Option<usize>) -> Result<Vec<Message>, PubSubError> {
        let (batch, poison_messages, dlq_policy) = {
            let topic_arc = self.get_topic(topic_name)?;
            let mut topic = topic_arc.write().unwrap();

            let sub = topic
                .get_subscription_mut(sub_name)
                .ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;

            let res = sub.pull_batch(batch_size_override, topic_name);

            (res.batch, res.poison_messages, sub.dead_letter_policy.clone())
        }; // Write lock on `topic` is explicitly dropped right here!

        // Publish to DLQ outside the topic
        if let Some(dlq_policy) = dlq_policy {
            for msg in poison_messages {
                let _ = self.publish(&dlq_policy.dead_letter_queue, msg);
            }
        }
        Ok(batch)
    }

    /// Acknowledges a message on a subscription
    pub fn ack(&self, topic_name: &str, sub_name: &str, message_id: &str) -> Result<(), PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        let mut topic = topic_arc.write().unwrap();

        let sub = topic.get_subscription_mut(sub_name).ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;

        if !sub.contains_key(message_id) { // (Or check sub.messages.contains_key)
            return Err(PubSubError::MessageNotFound(message_id.to_string()));
        }

        // 2. WRITE TO WAL
        if let Some(wal) = &self.wal {
            let entry = WalEntry::Ack {
                topic: topic_name.to_string(),
                subscription: sub_name.to_string(),
                message_id: message_id.to_string(),
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }

        // 3. MUTATE IN-MEMORY STATE
        sub.ack(message_id);
        Ok(())

    }

    pub fn get_subscription_notify(&self, topic_name: &str, sub_name: &str) -> Result<Arc<Notify>, PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        let topic = topic_arc.read().unwrap();

        let sub = topic.get_subscription(sub_name)
            .ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;

        // Returns a clone of the Arc<Notify> pointer
        Ok(sub.notify_handler())
    }
}

#[cfg(test)]
mod subscription_tests {
    use super::*;
    use std::collections::HashMap;
    use std::thread::sleep;

    fn dummy_msg(payload_str: &str) -> Message {
        Message::new(payload_str.as_bytes().to_vec(), HashMap::new())
    }

    #[test]
    fn test_empty_subscription_pull() {
        let mut sub = Subscription::new("sub-empty".to_string(), Duration::from_secs(10), 5, None, None);

        let batch = sub.pull_batch(None, "");
        assert!(batch.batch.is_empty());

        let batch_zero = sub.pull_batch(Some(0), "");
        assert!(batch_zero.batch.is_empty());
    }

    #[test]
    fn test_batch_size_boundaries() {
        let mut sub = Subscription::new("sub-bounds".to_string(), Duration::from_secs(10), 2, None, None);

        for i in 1..10 {
            sub.push(dummy_msg(&format!("msg-{i}")));
        }

        // batch_size = 0 should return self.size element
        let batch_zero = sub.pull_batch(Some(0), "");
        assert_eq!(batch_zero.batch.len(), 2);

        // batch_size = None should return self.size element
        let batch_none = sub.pull_batch(None, "");
        assert_eq!(batch_none.batch.len(), 2);

        // batch_size larger than remaining items
        let batch_size = sub.pull_batch(Some(100), "");
        assert_eq!(batch_size.batch.len(), 5);

        // Edge case 4: Queue is now empty
        assert!(sub.pull_batch(None, "").batch.is_empty());
    }

    #[test]
    fn test_double_ack_and_invalid_ack() {
        let mut sub = Subscription::new("sub-ack".to_string(), Duration::from_secs(10), 1, None, None);
        let msg = dummy_msg("hello");
        let msg_id = msg.id.clone();
        sub.push(msg);

        // case 1: ACK non-existent ID
        assert!(!sub.ack("non-existent-uuid"));

        // Pull the message to move it to InFlight
        let pulled = sub.pull_batch(None, "");
        assert_eq!(pulled.batch.len(), 1);

        // case 2: First ACK succeeds
        assert!(sub.ack(&msg_id));

        // case 3: Second ACK on same ID fails (already removed)
        assert!(!sub.ack(&msg_id));
    }

    #[test]
    fn test_visibility_expiration_and_attempt_counter() {
        // Fast 30ms visibility deadline
        let mut sub = Subscription::new("sub-ttl".to_string(), Duration::from_millis(30), 10, None, None);

        let msg1 = dummy_msg("msg-1");
        let msg1_id = msg1.id.clone();
        sub.push(msg1);

        // 1st Pull: attempt counter = 1
        let batch1 = sub.pull_batch(None, "");
        assert_eq!(batch1.batch[0].delivery_attempt, 1);

        // Immediate pull returns empty (msg1 is InFlight)
        assert!(sub.pull_batch(None, "").batch.is_empty());

        // Wait for deadline to expire (40ms > 30ms)
        sleep(Duration::from_millis(40));

        // 2nd Pull: msg1 re-claimed, attempt counter = 2
        let batch2 = sub.pull_batch(None, "").batch;
        assert_eq!(batch2.len(), 1);
        assert_eq!(batch2[0].id, msg1_id);
        assert_eq!(batch2[0].delivery_attempt, 2);
    }

    #[test]
    fn test_subscription_capacity_limit_and_ack_drain() {
        // Create a subscription capped at max 2 outstanding messages
        let mut sub = Subscription::new(
            "sub-cap".to_string(),
            Duration::from_secs(10),
            10, // batch_size
            Some(2),  // max_outstanding_messages
            None
        );

        assert_eq!(sub.dropped_messages, 0);

        // Push 1 and 2: Should succeed
        assert!(sub.push(dummy_msg("msg-1")));
        assert!(sub.push(dummy_msg("msg-2")));
        assert_eq!(sub.len(), 2);

        // Push 3: Should fail (full) and increment dropped_messages counter
        assert!(!sub.push(dummy_msg("msg-3")));
        assert_eq!(sub.len(), 2);
        assert_eq!(sub.dropped_messages, 1);

        // Pull 1 message and ACK it to free up capacity
        let batch = sub.pull_batch(Some(1), "").batch;
        assert_eq!(batch.len(), 1);
        let msg_id = batch[0].id.clone();
        assert!(sub.ack(&msg_id));
        assert_eq!(sub.len(), 1);

        // Push 4: Space is freed, should succeed now
        assert!(sub.push(dummy_msg("msg-4")));
        assert_eq!(sub.len(), 2);
        assert_eq!(sub.dropped_messages, 1); // dropped counter stays at 1
    }
}


#[cfg(test)]
mod topic_tests {
    use super::*;
    use std::collections::HashMap;

    fn dummy_msg(payload_str: &str) -> Message {
        Message::new(payload_str.as_bytes().to_vec(), HashMap::new())
    }


    #[test]
    fn test_publish_with_zero_subscriber() {
        let mut topic = Topic::new("topic-no-subs".to_string());

        let msg_id = topic.publish(dummy_msg("orphaned message"));
        assert!(!msg_id.is_empty());
    }

    #[test]
    fn test_fanout_isolation_and_independent_acks() {
        let mut topic = Topic::new("orders".to_string());

        topic.create_subscription("sub-billing", Duration::from_secs(10), 5, None, None);
        topic.create_subscription("sub-analytics", Duration::from_secs(10), 5, None, None);

        let msg = dummy_msg("Order #99");
        let msg_id = msg.id.clone();
        topic.publish(msg);

        // Both subscriptions must receive their own copy
        let billing_msgs = {
            let sub = topic.get_subscription_mut("sub-billing").unwrap();
            sub.pull_batch(None, "orders").batch
        };
        assert_eq!(billing_msgs.len(), 1);

        let analytics_msgs = {
            let sub = topic.get_subscription_mut("sub-analytics").unwrap();
            sub.pull_batch(None, "orders").batch
        };
        assert_eq!(analytics_msgs.len(), 1);

        // Acking on sub-billing remove it from sub-billing ONLY
        {
            let billing_sub = topic.get_subscription_mut("sub-billing").unwrap();
            assert!(billing_sub.ack(&msg_id));
            assert!(!billing_sub.contains_key(&msg_id));
        }
        {
            let sub = topic.get_subscription_mut("sub-analytics").unwrap();
            assert!(sub.contains_key(&msg_id));
        }
    }

    #[test]
    fn test_topic_fanout_with_partial_capacity() {
        let mut topic = Topic::new("orders".to_string());

        // Sub A: Unbounded (None)
        topic.create_subscription("sub-unbounded", Duration::from_secs(10), 10, None, None);
        // Sub B: Capped at 1 message
        topic.create_subscription("sub-capped", Duration::from_secs(10), 10, Some(1), None);

        let msg1 = dummy_msg("Order #1");
        let msg2 = dummy_msg("Order #2");

        // Publish 2 messages
        topic.publish(msg1);
        topic.publish(msg2);

        // Sub A gets both messages
        let sub_a = {
            let s = topic.get_subscription_mut("sub-unbounded").unwrap();
            (s.len(), s.dropped_messages)
        };
        assert_eq!(sub_a.0, 2);
        assert_eq!(sub_a.1, 0);

        // Sub B gets only 1 message, second is dropped
        let sub_b = {
            let s = topic.get_subscription_mut("sub-capped").unwrap();
            (s.len(), s.dropped_messages)
        };
        assert_eq!(sub_b.0, 1);
        assert_eq!(sub_b.1, 1);
    }
}

#[cfg(test)]
mod engine_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;


    fn dummy_msg(payload_str: &str) -> Message {
        Message::new(payload_str.as_bytes().to_vec(), HashMap::new())
    }

    #[test]
    fn test_engine_collision_and_not_found_errors() {
        let engine = Engine::new();

        // Topic creation & duplicate checks
        assert!(engine.create_topic("payments").is_ok());
        assert_eq!(
            engine.create_topic("payments").unwrap_err(),
            PubSubError::TopicAlreadyExist("payments".to_string())
        );

        // Subscription under non-existent topic
        assert_eq!(
            engine.create_subscription("missing-topic", "sub-1", Duration::from_secs(5), Some(5), None, None, None).unwrap_err(),
            PubSubError::TopicNotFound("missing-topic".to_string())
        );

        // Duplicate subscription under existing topic
        assert!(engine.create_subscription("payments", "sub-1", Duration::from_secs(5), Some(5), None, None, None).is_ok());
        assert_eq!(
            engine.create_subscription("payments", "sub-1", Duration::from_secs(5), Some(5), None, None, None).unwrap_err(),
            PubSubError::SubscriptionAlreadyExists("sub-1".to_string())
        );

        // Pull/ACK operations on non-existent topic/sub
        assert_eq!(
            engine.pull_batch("ghost-topic", "sub-1", None).unwrap_err(),
            PubSubError::TopicNotFound("ghost-topic".to_string())
        );
        assert_eq!(
            engine.pull_batch("payments", "ghost-sub", None).unwrap_err(),
            PubSubError::SubscriptionNotFound("ghost-sub".to_string())
        );
    }

    #[test]
    fn test_concurrent_multithreaded_publish_and_pull() {
        let engine = Arc::new(Engine::new());
        engine.create_topic("events").unwrap();
        engine.create_subscription("events", "sub-workers", Duration::from_secs(10), Some(10), None, None, None).unwrap();

        let mut handles = vec![];

        // Spawn 5 Producer Threads (each publishing 20 messages)
        for t_id in 0..5 {
            let engine_clone = Arc::clone(&engine);
            let handle = thread::spawn(move || {
                for i in 0..20 {
                    let payload = format!("producer-{t_id}-msg-{i}").into_bytes();
                    let msg = Message::new(payload, HashMap::new());
                    engine_clone.publish("events", msg).unwrap();
                }
            });
            handles.push(handle);
        }

        // Spawn 5 Consumer Threads (each pulling and ACKing messages)
        for _ in 0..5 {
            let engine_clone = Arc::clone(&engine);
            let handle = thread::spawn(move || {
                for _ in 0..30 {
                    if let Ok(msgs) = engine_clone.pull_batch("events", "sub-workers", Some(5)) {
                        for m in msgs {
                            if engine_clone.ack("events", "sub-workers", &m.id).is_ok() {}
                        }
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Final pull to clear any remaining queued messages
        let remaining = engine.pull_batch("events", "sub-workers", Some(100)).unwrap();
        for m in remaining {
            let _ = engine.ack("events", "sub-workers", &m.id);
        }
    }

    #[test]
    fn test_dead_letter_topic_routing_and_attribute_enrichment() {
        let engine = Engine::new();

        // 1. Create main topic and dead-letter topic
        engine.create_topic("orders").unwrap();
        engine.create_topic("orders-dlt").unwrap();

        // 2. Create DLT subscription to read evicted poison messages
        engine
            .create_subscription("orders-dlt", "dlt-sub", Duration::from_secs(10), None, None, None, None)
            .unwrap();

        // 3. Create main subscription with max 2 delivery attempts and DLT configured
        let ack_deadline = Duration::from_millis(50);
        engine
            .create_subscription(
                "orders",
                "sub-payments",
                ack_deadline,
                Some(10),
                None,
                Some("orders-dlt".to_string()), // dead_letter_topic
                Some(2),                      // max_delivery_attempts
            )
            .unwrap();

        // 4. Publish message
        let msg = dummy_msg("Poison Order #99");
        let original_msg_id = msg.id.clone();
        engine.publish("orders", msg).unwrap();

        // Attempt 1: Pull message (delivery_attempt = 1)
        let batch1 = engine.pull_batch("orders", "sub-payments", Some(1)).unwrap();
        assert_eq!(batch1.len(), 1);
        assert_eq!(batch1[0].delivery_attempt, 1);

        // Wait for visibility timeout to expire
        thread::sleep(Duration::from_millis(60));

        // Attempt 2: Pull message again (delivery_attempt = 2)
        let batch2 = engine.pull_batch("orders", "sub-payments", Some(1)).unwrap();
        assert_eq!(batch2.len(), 1);
        assert_eq!(batch2[0].delivery_attempt, 2);

        // Wait for visibility timeout to expire again
        thread::sleep(Duration::from_millis(60));

        // Attempt 3: Threshold hit! Pulling now should evict poison msg to 'orders-dlt' and return empty
        let batch3 = engine.pull_batch("orders", "sub-payments", Some(1)).unwrap();
        assert!(batch3.is_empty(), "Poison message should be evicted, not returned to consumer");

        // 5. Verify the poison message arrived in 'orders-dlt' via 'dlt-sub'
        let dlt_batch = engine.pull_batch("orders-dlt", "dlt-sub", Some(1)).unwrap();
        assert_eq!(dlt_batch.len(), 1);

        let dlt_msg = &dlt_batch[0];
        assert_eq!(dlt_msg.id, original_msg_id);

        // Check injected DLT metadata attributes
        assert_eq!(
            dlt_msg.attributes.get("x-dead-letter-source-subscription").unwrap(),
            "sub-payments"
        );
        assert_eq!(
            dlt_msg.attributes.get("x-dead-letter-source-topic").unwrap(),
            "orders"
        );
        assert_eq!(
            dlt_msg.attributes.get("x-dead-letter-delivery-attempts").unwrap(),
            "2"
        );
    }

    #[tokio::test]
    async fn test_push_webhook_delivery_and_headers() {
        // 1. Bind local mock HTTP server to an ephemeral port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{}", addr);

        // Channel to pass captured raw HTTP request from server task to test assertion
        let (tx, mut rx) = mpsc::channel::<String>(1);

        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 2048];
                let n = socket.read(&mut buf).await.unwrap();
                let request_text = String::from_utf8_lossy(&buf[..n]).to_string();

                // Pass captured HTTP request back to main test runner
                tx.send(request_text).await.unwrap();

                // Return HTTP 200 OK response
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        // 2. Initialize Engine & Topic
        let engine = Engine::new();
        engine.create_topic("orders").unwrap();

        // 3. Configure Custom Headers & Create Push Subscription
        let mut custom_headers = HashMap::new();
        custom_headers.insert("Authorization".to_string(), "Bearer test-token-123".to_string());

        engine
            .create_push_subscription(
                "orders",
                "sub-push-test",
                &endpoint,
                custom_headers,
                Some(5),                 // timeout_secs
                Duration::from_secs(10), // ack_deadline
                None,                    // max_outstanding_messages
                None,                    // dead_letter_queue
                Some(5),                 // max_delivery_attempts
            )
            .unwrap();

        // 4. Publish Message to Topic
        let msg_payload = b"Push Delivery Payload".to_vec();
        engine
            .publish(
                "orders",
                Message {
                    id: "msg-push-001".to_string(),
                    payload: msg_payload,
                    attributes: HashMap::new(),
                    delivery_attempt: 0,
                },
            )
            .unwrap();

        // 5. Await HTTP Request at Mock Server
        let req_raw = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("Webhook endpoint did not receive HTTP POST within timeout")
            .expect("Channel closed prematurely");

        let req_lower = req_raw.to_lowercase();

        // 6. Assert HTTP Method, Headers, and Payload
        assert!(req_raw.starts_with("POST"));
        assert!(req_lower.contains("authorization: bearer test-token-123"));
        assert!(req_lower.contains("x-pubsub-message-id: msg-push-001"));
        assert!(req_lower.contains("x-pubsub-subscription: sub-push-test"));
        assert!(req_raw.contains("Push Delivery Payload"));

        // 7. Verify Message Was Auto-ACKed
        tokio::time::sleep(Duration::from_millis(100)).await;
        let pending = engine
            .pull_batch("orders", "sub-push-test", Some(1))
            .unwrap();
        assert!(
            pending.is_empty(),
            "Message should be auto-ACKed after receiving HTTP 200 OK"
        );
    }
}