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
    pub notify: Arc<Notify>
}

impl Subscription {
    pub fn new(name: String, ack_deadline: Duration) -> Self {
        Self {
            name,
            ack_deadline,
            messages: HashMap::new(),
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
    pub fn pull(&mut self) -> Option<Message>{
        let now = Instant::now();

        // 1. Look for message that is ready or has timed out
        for (msg, state) in self.messages.values_mut() {
            let is_available = match state {
                DeliveryState::Ready => true,
                DeliveryState::InFlight { deadline } => now >= *deadline,
            };

            if is_available {
                // 2. Increment delivery attempts
                msg.delivery_attempt += 1;

                // 3. Mark an Inflight with a fresh deadline
                let new_deadline = now + self.ack_deadline;
                *state = DeliveryState::InFlight {
                    deadline: new_deadline
                };

                // 4. Return the updated message to caller
                return Some(msg.clone());
            }
        }
        // No message available for pickup right now
        None
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
    pub fn create_subscription(&mut self, name: &str, ack_deadline: Duration) -> bool {
        if self.subscription.contains_key(name) {
            return false; // Already exists
        }
        let sub = Subscription::new(
            name.to_string(),
            ack_deadline
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
    pub fn create_subscription(&self, topic_name: &str, sub_name: &str, ack_deadline: Duration) -> Result<(), PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        let mut topic = topic_arc.write().unwrap();

        if topic.subscription.contains_key(sub_name) {
            return Err(PubSubError::SubscriptionAlreadyExists(sub_name.to_string()));
        }
        if let Some(wal) = &self.wal {
            let entry = WalEntry::CreateSubscription {
                topic: topic_name.to_string(),
                subscription: sub_name.to_string(),
                ack_deadline_sec: ack_deadline.as_secs()
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }

        topic.create_subscription(sub_name, ack_deadline);
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
    pub fn pull(&self, topic_name: &str, sub_name: &str) -> Result<Option<Message>, PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        let mut topic = topic_arc.write().unwrap();

        let sub = topic.get_subscription_mut(sub_name).ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;
        Ok(sub.pull())
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
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::thread::sleep;

    fn dummy_message() -> Message {
        Message::new(b"hello world".to_vec(), HashMap::new())
    }

    #[test]
    fn test_topic_and_subscription_collision_errors(){
        let engine = Engine::new();

        assert!(engine.create_topic("orders").is_ok());

        let err = engine.create_topic("orders").unwrap_err();
        assert_eq!(err, PubSubError::TopicAlreadyExist("orders".to_string()));

        let err = engine
            .create_subscription("missing-topic", "sub-1", Duration::from_secs(10))
            .unwrap_err();
        assert_eq!(err, PubSubError::TopicNotFound("missing-topic".to_string()));

        assert!(engine.create_subscription("orders", "sub-1", Duration::from_secs(10)).is_ok());

        let err = engine
            .create_subscription("orders", "sub-1", Duration::from_secs(5))
            .unwrap_err();
        assert_eq!(err, PubSubError::SubscriptionAlreadyExists("sub-1".to_string()));
    }

    #[test]
    fn test_publish_pull_ack_error_handling() {
        let engine = Engine::new();
        engine.create_topic("orders").unwrap();
        engine
            .create_subscription("orders", "sub-1", Duration::from_secs(10))
            .unwrap();

        let msg = dummy_message();
        let msg_id = msg.id.clone();

        let err = engine.publish("invalid-topic", msg.clone()).unwrap_err();
        assert_eq!(err, PubSubError::TopicNotFound("invalid-topic".to_string()));

        assert!(engine.publish("orders", msg).is_ok());

        let err = engine.pull("orders", "invalid-sub").unwrap_err();
        assert_eq!(
            err,
            PubSubError::SubscriptionNotFound("invalid-sub".to_string())
        );

        let pulled = engine.pull("orders", "sub-1").unwrap().unwrap();
        assert_eq!(pulled.id, msg_id);

        let err = engine.ack("orders", "sub-1", "fake-msg-id").unwrap_err();
        assert_eq!(err, PubSubError::MessageNotFound("fake-msg-id".to_string()));

        assert!(engine.ack("orders", "sub-1", &msg_id).is_ok());

        let err = engine.ack("orders", "sub-1", &msg_id).unwrap_err();
        assert_eq!(err, PubSubError::MessageNotFound(msg_id));
    }

    #[test]
    fn test_visibility_timeout_and_redelivery() {
        let mut sub = Subscription::new("sub-fast".to_string(), Duration::from_millis(50));
        let msg = dummy_message();
        let msg_id = msg.id.clone();
        sub.push(msg);

        // First pull: should succeed with delivery_attempt = 1
        let first_pull = sub.pull().expect("first pull should success");
        assert_eq!(first_pull.id, msg_id);
        assert_eq!(first_pull.delivery_attempt, 1);

        // Immediate second pull: should return None because it's InFlight
        assert!(sub.pull().is_none());

        // Wait past the visibility timeout
        sleep(Duration::from_millis(60));

        // Third pull: deadline expired, message reclaimed with delivery_attempt = 2
        let reclaimed = sub.pull().expect("Should re-deliver expired message");
        assert_eq!(reclaimed.id, msg_id);
        assert_eq!(reclaimed.delivery_attempt, 2);
    }
}