use std::collections::HashMap;
use std::time::{
    Duration,
    Instant,
};
use std::sync::{Arc};
use tokio::sync::Notify;
use crate::model::{DeliveryState, Message, DeadLetterPolicy, PullRequest, PushConfig};

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

