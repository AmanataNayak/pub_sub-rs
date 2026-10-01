use std::cmp::max;
use std::collections::{HashMap, VecDeque};
use std::time::{
    Duration,
    Instant,
};
use std::sync::{Arc};
use tokio::sync::Notify;
use crate::model::{Message, DeadLetterPolicy, PushConfig};

pub struct Subscription {
    pub name: String,
    pub ack_deadline: Duration,
    // O(1) Queue of message IDs ready for delivery
    ready_queue: VecDeque<String>,
    // Storage for active messages
    messages: HashMap<String, Message>,
    // Map tracking in-flight message deadlines (msg_id -> expiration_time)
    in_flight: HashMap<String, Instant>,
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
            ready_queue: VecDeque::new(),
            in_flight: HashMap::new(),
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
            ready_queue: VecDeque::new(),
            in_flight: HashMap::new(),
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
                let msg_id = msg.id.clone();
                self.messages.insert(msg_id.clone(), msg);
                self.ready_queue.push_back(msg_id);
                self.notify.notify_waiters();
                true
            }
        }
    }

    /// Pulls message up to request_batch_size
    /// Falls back to self.batch_size if request_batch_size is None or 0
    pub fn pull_batch(&mut self, request_batch_size: Option<usize>) -> Vec<Message> {
        let max_items = match request_batch_size {
            Some(n) if n > 0 => n,
            _ => self.batch_size
        };

        let expiration = Instant::now() + self.ack_deadline;

        let mut batch = Vec::with_capacity(max_items);
        while  batch.len() < max_items{
            let msg_id = match self.ready_queue.pop_front() {
                Some(id) => id,
                None => break // queue is empty
            };

            if let Some(mut msg) = self.messages.get_mut(&msg_id) {
                // mark message as in-flight with its visibility deadline
                msg.delivery_attempt += 1;
                self.in_flight.insert(msg_id, expiration);

                batch.push(msg.clone())
            }
        }

        batch
    }

    /// Remove the message in case of acknowledgment
    pub fn ack_batch(&mut self, message_ids: &[String]) -> usize{
        let mut success_count = 0;
        for message_id in message_ids {
            self.in_flight.remove(message_id);
            if self.messages.remove(message_id).is_some() {
                success_count += 1;
            }
        }
        success_count
    }

    pub fn contains_key(&self, message_id: &str) -> bool {
        self.messages.contains_key(message_id)
    }

    /// Checks for expired in-flight messages.
    /// Requeues unexpired messages to `ready_queue` or purges and returns DLQ-bound messages.
    pub fn requeue_expired(&mut self) -> Vec<(String, Message)> {
        let now = Instant::now();
        let mut dead_letter_messages = Vec::new();

        // 1. Collect expired IDs
        let expired_ids: Vec<String> = self
            .in_flight
            .iter()
            .filter(|(_, deadline)| now > **deadline)
            .map(|(id, _)| id.clone())
            .collect();

        for msg_id in expired_ids {
            self.in_flight.remove(&msg_id);

            // 2. Increment delivery_attempt and check if it exceeds DLQ policy limit
            let should_dlq = if let Some(msg) = self.messages.get_mut(&msg_id) {
                match &self.dead_letter_policy {
                    Some(policy) => msg.delivery_attempt >= policy.max_delivery_attempts,
                    None => false,
                }
            } else {
                false
            };

            // 3. Borrow on `self.messages` has ended, so now we can safely take ownership or requeue
            if should_dlq {
                if let Some(mut removed_msg) = self.messages.remove(&msg_id) {
                    let dlq_topic = self
                        .dead_letter_policy
                        .as_ref()
                        .map(|p| p.dead_letter_queue.clone())
                        .unwrap_or_default();

                    // Enrich metadata attributes
                    removed_msg.attributes.insert(
                        "x-dead-letter-source-subscription".to_string(),
                        self.name.clone()
                    );

                    removed_msg.attributes.insert(
                        "x-dead-letter-delivery-attempts".to_string(),
                        removed_msg.delivery_attempt.to_string()
                    );

                    removed_msg.attributes.insert(
                        "x-dead-letter-original-message-id".to_string(),
                        removed_msg.id.clone()
                    );

                    dead_letter_messages.push((dlq_topic, removed_msg));
                }
            } else {
                // Requeue back to ready_queue for another attempt
                self.ready_queue.push_back(msg_id);
            }
        }

        if !self.ready_queue.is_empty() {
            self.notify.notify_waiters();
        }

        dead_letter_messages
    }
}

#[cfg(test)]
mod subscription_tests {
    use super::*;
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;

    fn dummy_msg(payload_str: &str) -> Message {
        Message {
            id: uuid::Uuid::new_v4().to_string(),
            payload: payload_str.as_bytes().to_vec().into(),
            attributes: HashMap::new(),
            delivery_attempt: 0,
        }
    }

    fn create_msg(id: &str) -> Message {
        Message {
            id: id.to_string(),
            payload: b"test payload".to_vec().into(),
            attributes: HashMap::new(),
            delivery_attempt: 0,
        }
    }

    #[test]
    fn test_empty_subscription_pull() {
        let mut sub = Subscription::new("sub-empty".to_string(), Duration::from_secs(10), 5, None, None);

        let batch = sub.pull_batch(None);
        assert!(batch.is_empty());

        let batch_zero = sub.pull_batch(Some(0));
        assert!(batch_zero.is_empty());
    }

    #[test]
    fn test_batch_size_boundaries() {
        let mut sub = Subscription::new("sub-bounds".to_string(), Duration::from_secs(10), 2, None, None);

        for i in 1..10 {
            sub.push(dummy_msg(&format!("msg-{i}")));
        }

        // batch_size = 0 falls back to self.batch_size (2)
        let batch_zero = sub.pull_batch(Some(0));
        assert_eq!(batch_zero.len(), 2);

        // batch_size = None falls back to self.batch_size (2)
        let batch_none = sub.pull_batch(None);
        assert_eq!(batch_none.len(), 2);

        // batch_size larger than remaining items (returns remaining 5)
        let batch_size = sub.pull_batch(Some(100));
        assert_eq!(batch_size.len(), 5);

        // Edge case 4: Queue is now empty
        assert!(sub.pull_batch(None).is_empty());
    }

    #[test]
    fn test_double_ack_and_invalid_ack() {
        let mut sub = Subscription::new("sub-ack".to_string(), Duration::from_secs(10), 1, None, None);
        let msg = dummy_msg("hello");
        let msg_id = msg.id.clone();
        sub.push(msg);

        // case 1: ACK non-existent ID
        assert_eq!(sub.ack_batch(&["non-existent-uuid".to_string()]), 0);

        // Pull the message to move it to InFlight
        let pulled = sub.pull_batch(None);
        assert_eq!(pulled.len(), 1);

        // case 2: First ACK succeeds
        let arr = [msg_id.to_string()];
        assert_eq!(sub.ack_batch(&arr), 1);

        let arr = [msg_id.to_string()];
        // case 3: Second ACK on same ID fails (already removed)
        assert_eq!(sub.ack_batch(&arr), 0);
    }

    #[test]
    fn test_visibility_expiration_and_attempt_counter() {
        // Fast 30ms visibility deadline
        let mut sub = Subscription::new("sub-ttl".to_string(), Duration::from_millis(30), 10, None, None);

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

        // Requeue timed-out messages back to ready_queue
        sub.requeue_expired();

        // 2nd Pull: msg1 re-claimed, attempt counter = 2
        let batch2 = sub.pull_batch(None);
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
            None,
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
        let batch = sub.pull_batch(Some(1));
        assert_eq!(batch.len(), 1);
        let msg_id = batch[0].id.clone();
        assert_eq!(sub.ack_batch(&[msg_id]), 1);
        assert_eq!(sub.len(), 1);

        // Push 4: Space is freed, should succeed now
        assert!(sub.push(dummy_msg("msg-4")));
        assert_eq!(sub.len(), 2);
        assert_eq!(sub.dropped_messages, 1); // dropped counter stays at 1
    }

    #[test]
    fn test_fifo_ready_queue_pull() {
        let mut sub = Subscription::new(
            "sub-fifo".to_string(),
            Duration::from_secs(10),
            100,
            None,
            None,
        );

        sub.push(create_msg("m1"));
        sub.push(create_msg("m2"));
        sub.push(create_msg("m3"));

        // 1. First pull gets m1 and m2 in strict FIFO order
        let batch_1 = sub.pull_batch(Some(2));
        assert_eq!(batch_1.len(), 2);
        assert_eq!(batch_1[0].id, "m1");
        assert_eq!(batch_1[1].id, "m2");

        // 2. Next pull gets remaining m3
        let batch_2 = sub.pull_batch(Some(2));
        assert_eq!(batch_2.len(), 1);
        assert_eq!(batch_2[0].id, "m3");

        // 3. Subsequent pull gets nothing
        let batch_empty = sub.pull_batch(Some(2));
        assert!(batch_empty.is_empty());
    }

    #[test]
    fn test_ack_removes_from_inflight_and_storage() {
        let mut sub = Subscription::new(
            "sub-ack".to_string(),
            Duration::from_secs(10),
            100,
            None,
            None,
        );

        sub.push(create_msg("m1"));
        let pulled = sub.pull_batch(Some(1));
        assert_eq!(pulled.len(), 1);

        // ACK message
        let ack_success = sub.ack_batch(&["m1".to_string()]);
        assert_eq!(ack_success, 1);

        // Pulling again should yield nothing
        let pulled_again = sub.pull_batch(Some(1));
        assert!(pulled_again.is_empty());
    }

    #[test]
    fn test_skips_stale_or_missing_ids_in_ready_queue() {
        let mut sub = Subscription::new(
            "sub-stale".to_string(),
            Duration::from_millis(10),
            100,
            None,
            None,
        );

        sub.push(create_msg("m1"));
        sub.push(create_msg("m2"));

        // Simulate out-of-band deletion or ACK before pull (stale ID in ready_queue)
        sub.pull_batch(Some(2));
        sub.ack_batch(&["m1".to_string()]);
        sleep(Duration::from_millis(10));
        sub.requeue_expired();
        // Pull should skip missing 'm1' silently and return 'm2'
        let batch = sub.pull_batch(Some(2));
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].id, "m2");
    }

    #[test]
    fn test_visibility_timeout_requeues_to_ready_queue() {
        let mut sub = Subscription::new(
            "sub-timeout".to_string(),
            Duration::from_millis(20), // 20ms visibility deadline
            100,
            None,
            None,
        );

        sub.push(create_msg("m1"));
        let batch = sub.pull_batch(Some(1));
        assert_eq!(batch.len(), 1);

        // Wait for deadline to pass
        sleep(Duration::from_millis(30));

        // Scan and requeue expired in-flight messages
        sub.requeue_expired();

        // Message should be back in ready_queue and pullable
        let requeued_batch = sub.pull_batch(Some(1));
        assert_eq!(requeued_batch.len(), 1);
        assert_eq!(requeued_batch[0].id, "m1");
    }
}