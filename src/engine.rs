/// Subscription Tracking

use std::collections::HashMap;
use std::time::{
    Duration,
    Instant,
};
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;
use crate::model::{DeliveryState, Message};
use crate::errors::{PubSubError};
use crate::wal::{WalEntry, WalManager};

pub struct Subscription {
    pub name: String,
    pub ack_deadline: Duration,
    // Store message data alongside its current state
    messages: HashMap<String, (Message, DeliveryState)>,
    batch_size: usize,
    pub notify: Arc<Notify>
}

impl Subscription {
    pub fn new(name: String, ack_deadline: Duration, batch_size: Option<usize>) -> Self {
        let effective_batch_size = batch_size.unwrap_or(1).max(1);

        Self {
            name,
            ack_deadline,
            messages: HashMap::new(),
            batch_size: effective_batch_size,
            notify: Arc::new(Notify::new())
        }
    }

    /// Expose the notify handlers so stream handers can wait on it
    pub fn notify_handler(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    /// Called when a topic fan-outs a new message to this subscription.
    pub fn push(&mut self, msg: Message) {
        self.messages.insert(msg.id.clone(), (msg, DeliveryState::Ready));
        self.notify.notify_waiters();
    }

    /// Pulls the next available message (either Ready or expired InFlight)
    // pub fn pull(&mut self) -> Option<Message>{
    //     let now = Instant::now();
    //
    //     // 1. Look for message that is ready or has timed out
    //     for (msg, state) in self.messages.values_mut() {
    //         let is_available = match state {
    //             DeliveryState::Ready => true,
    //             DeliveryState::InFlight { deadline } => now >= *deadline,
    //         };
    //
    //         if is_available {
    //             // 2. Increment delivery attempts
    //             msg.delivery_attempt += 1;
    //
    //             // 3. Mark an Inflight with a fresh deadline
    //             let new_deadline = now + self.ack_deadline;
    //             *state = DeliveryState::InFlight {
    //                 deadline: new_deadline
    //             };
    //
    //             // 4. Return the updated message to caller
    //             return Some(msg.clone());
    //         }
    //     }
    //     // No message available for pickup right now
    //     None
    // }

    /// Pulls message up to request_batch_size
    /// Falls back to self.batch_size if request_batch_size is None or 0
    pub fn pull_batch(&mut self, request_batch_size: Option<usize>) -> Vec<Message> {
        let max_items = match request_batch_size {
            Some(n) if n > 0 => n,
            _ => self.batch_size,
        };

        let mut batch = Vec::with_capacity(max_items);
        let now = Instant::now();

        for (msg, state) in self.messages.values_mut() {
            if batch.len() >= max_items {
                break;
            }

            let is_available = match state {
                DeliveryState::Ready => true,
                DeliveryState::InFlight { deadline} => now >= *deadline
            };

            if is_available {
                msg.delivery_attempt += 1;
                *state = DeliveryState::InFlight {
                    deadline: now + self.ack_deadline, //language= update the deadline
                };
                batch.push(msg.clone());
            }
        }
        batch
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
    pub fn create_subscription(&mut self, name: &str, ack_deadline: Duration, batch_size: Option<usize>) -> bool {
        if self.subscription.contains_key(name) {
            return false; // Already exists
        }
        let sub = Subscription::new(
            name.to_string(),
            ack_deadline,
            batch_size
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
    pub fn create_subscription(&self, topic_name: &str, sub_name: &str, ack_deadline: Duration, batch_size: Option<usize>) -> Result<(), PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        let mut topic = topic_arc.write().unwrap();

        if topic.subscription.contains_key(sub_name) {
            return Err(PubSubError::SubscriptionAlreadyExists(sub_name.to_string()));
        }
        if let Some(wal) = &self.wal {
            let entry = WalEntry::CreateSubscription {
                topic: topic_name.to_string(),
                subscription: sub_name.to_string(),
                ack_deadline_sec: ack_deadline.as_secs(),
                batch_size: batch_size.unwrap_or(1)
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }

        topic.create_subscription(sub_name, ack_deadline, batch_size);
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
        let topic_arc = self.get_topic(topic_name)?;
        let mut topic = topic_arc.write().unwrap();

        let sub = topic
            .get_subscription_mut(sub_name)
            .ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;

        Ok(sub.pull_batch(batch_size_override))
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
    use std::fmt::format;
    use std::thread::sleep;

    fn dummy_msg(payload_str: &str) -> Message {
        Message::new(payload_str.as_bytes().to_vec(), HashMap::new())
    }

    #[test]
    fn test_empty_subscription_pull() {
        let mut sub = Subscription::new("sub-empty".to_string(), Duration::from_secs(10), Some(5));

        let batch = sub.pull_batch(None);
        assert!(batch.is_empty());

        let batch_zero = sub.pull_batch(Some(0));
        assert!(batch_zero.is_empty());
    }

    #[test]
    fn test_batch_size_boundaries() {
        let mut sub = Subscription::new("sub-bounds".to_string(), Duration::from_secs(10), Some(2));

        for i in 1..10 {
            sub.push(dummy_msg(&format!("msg-{i}")));
        }

        // batch_size = 0 should return self.size element
        let batch_zero = sub.pull_batch(Some(0));
        assert_eq!(batch_zero.len(), 2);

        // batch_size = None should return self.size element
        let batch_none = sub.pull_batch(None);
        assert_eq!(batch_none.len(), 2);

        // batch_size larger than remaining items
        let batch_size = sub.pull_batch(Some(100));
        assert_eq!(batch_size.len(), 5);

        // Edge case 4: Queue is now empty
        assert!(sub.pull_batch(None).is_empty());
    }

    #[test]
    fn test_double_ack_and_invalid_ack() {
        let mut sub = Subscription::new("sub-ack".to_string(), Duration::from_secs(10), Some(1));
        let msg = dummy_msg("hello");
        let msg_id = msg.id.clone();
        sub.push(msg);

        // case 1: ACK non-existent ID
        assert!(!sub.ack("non-existent-uuid"));

        // Pull the message to move it to InFlight
        let pulled = sub.pull_batch(None);
        assert_eq!(pulled.len(), 1);

        // case 2: First ACK succeeds
        assert!(sub.ack(&msg_id));

        // case 3: Second ACK on same ID fails (already removed)
        assert!(!sub.ack(&msg_id));
    }

    #[test]
    fn test_visibility_expiration_and_attempt_counter() {
        // Fast 30ms visibility deadline
        let mut sub = Subscription::new("sub-ttl".to_string(), Duration::from_millis(30), Some(10));

        let msg1 = dummy_msg("msg-1");
        let msg1_id = msg1.id.clone();
        sub.push(msg1);

        // 1st Pull: attempt counter = 1
        let batch1 = sub.pull_batch(None);
        assert_eq!(batch1[0].delivery_attempt, 1);

        // Immediate pull returns empty (msg1 is InFlight)
        assert!(sub.pull_batch(None).is_empty());

        // Wait for deadline to expire (40ms > 30ms)
        sleep(Duration::from_millis(40));

        // 2nd Pull: msg1 re-claimed, attempt counter = 2
        let batch2 = sub.pull_batch(None);
        assert_eq!(batch2.len(), 1);
        assert_eq!(batch2[0].id, msg1_id);
        assert_eq!(batch2[0].delivery_attempt, 2);
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

        topic.create_subscription("sub-billing", Duration::from_secs(10), Some(5));
        topic.create_subscription("sub-analytics", Duration::from_secs(10), Some(5));

        let msg = dummy_msg("Order #99");
        let msg_id = msg.id.clone();
        topic.publish(msg);

        // Both subscriptions must receive their own copy
        let billing_msgs = {
            let sub = topic.get_subscription_mut("sub-billing").unwrap();
            sub.pull_batch(None)
        };
        assert_eq!(billing_msgs.len(), 1);

        let analytics_msgs = {
            let sub = topic.get_subscription_mut("sub-analytics").unwrap();
            sub.pull_batch(None)
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
}

#[cfg(test)]
mod engine_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;

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
            engine.create_subscription("missing-topic", "sub-1", Duration::from_secs(5), Some(5)).unwrap_err(),
            PubSubError::TopicNotFound("missing-topic".to_string())
        );

        // Duplicate subscription under existing topic
        assert!(engine.create_subscription("payments", "sub-1", Duration::from_secs(5), Some(5)).is_ok());
        assert_eq!(
            engine.create_subscription("payments", "sub-1", Duration::from_secs(5), Some(5)).unwrap_err(),
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
        engine.create_subscription("events", "sub-workers", Duration::from_secs(10), Some(10)).unwrap();

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
                let mut total_acked = 0;
                for _ in 0..30 {
                    if let Ok(msgs) = engine_clone.pull_batch("events", "sub-workers", Some(5)) {
                        for m in msgs {
                            if engine_clone.ack("events", "sub-workers", &m.id).is_ok() {
                                total_acked += 1;
                            }
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
}