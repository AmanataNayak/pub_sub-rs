use std::collections::{HashMap, VecDeque};
use std::time::{
    Duration,
    Instant,
};
use std::sync::{Arc};
use tokio::sync::Notify;
use crate::model::{Message, DeadLetterPolicy, PushConfig, PullResponse};

pub struct Subscription {
    pub name: String,
    pub ack_deadline: Duration,
    // O(1) Queue of message IDs ready for delivery
    pub ready_queue: VecDeque<String>,
    // Storage for active messages
    pub messages: HashMap<String, Message>,
    // Map tracking in-flight message deadlines (msg_id -> expiration_time)
    pub in_flight: HashMap<String, Instant>,
    pub push_config: Option<PushConfig>,
    pub batch_size: usize,
    pub max_outstanding_messages: Option<usize>, // None
    pub message_ttl: Option<Duration>,
    pub dropped_messages: u64,
    pub dead_letter_policy: Option<DeadLetterPolicy>,
    pub notify: Arc<Notify>
}

impl Subscription {
    pub fn new(name: String, ack_deadline: Duration, batch_size: usize, max_outstanding_messages: Option<usize>, message_ttl: Option<Duration>, dead_letter_policy: Option<DeadLetterPolicy>) -> Self {

        Self {
            name,
            ack_deadline,
            messages: HashMap::new(),
            ready_queue: VecDeque::new(),
            in_flight: HashMap::new(),
            batch_size,
            dropped_messages: 0,
            max_outstanding_messages,
            message_ttl,
            dead_letter_policy,
            push_config: None,
            notify: Arc::new(Notify::new())
        }
    }

    pub fn new_push(name: String, ack_deadline: Duration, push_config: PushConfig, batch_size: usize, max_outstanding_messages: Option<usize>, message_ttl: Option<Duration>, dead_letter_policy: Option<DeadLetterPolicy>) -> Self {
        Self {
            name,
            ack_deadline,
            messages: HashMap::new(),
            ready_queue: VecDeque::new(),
            in_flight: HashMap::new(),
            batch_size: batch_size,
            dropped_messages: 0,
            max_outstanding_messages,
            message_ttl,
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
    /// helper functions
    /// 1. dead letter policy
    fn dead_letter_policy_evaluator(&self, policy: &DeadLetterPolicy, reason: &str, mut removed_msg: Message) -> (String, Message){

        removed_msg.attributes.insert(
            "x-dead-letter-source-subscription".to_string(),
            self.name.clone()
        );
        removed_msg.attributes.insert(
            "x-dead-letter-original-message-id".to_string(),
            removed_msg.id.clone()
        );
        removed_msg.attributes.insert(
            "x-dead-letter-reason".to_string(),
            reason.to_string()
        );
        removed_msg.attributes.insert(
            "x-dead-letter-delivery-attempts".to_string(),
            removed_msg.delivery_attempt.to_string()
        );

        (policy.dead_letter_queue.clone(), removed_msg)
    }
    /// Pulls message up to request_batch_size
    /// Falls back to self.batch_size if request_batch_size is None or 0
    pub fn pull_batch(&mut self, request_batch_size: Option<usize>) -> PullResponse {
        let max_items = match request_batch_size {
            Some(n) if n > 0 => n,
            _ => self.batch_size
        };

        let now = Instant::now();
        let expiration = now + self.ack_deadline;

        let mut batch = Vec::with_capacity(max_items);
        let mut dead_letter = Vec::new();

        while  batch.len() < max_items{
            let msg_id = match self.ready_queue.pop_front() {
                Some(id) => id,
                None => break // queue is empty
            };

            // Evaluate TTL without holding mutable borrow
            let is_expired = self.messages.get(&msg_id).map_or(false, |msg| {
               self.message_ttl.map_or(false, |ttl| now >= msg.created_at + ttl)
            });

            if is_expired {
                if let Some(removed_msg) = self.messages.remove(&msg_id) {
                    if let Some(policy) = &self.dead_letter_policy {
                        dead_letter.push(self.dead_letter_policy_evaluator(
                            policy,
                            "ttl_expired",
                            removed_msg
                        ));
                    }
                }
            } else if let Some(msg) = self.messages.get_mut(&msg_id) {
                msg.delivery_attempt += 1;
                self.in_flight.insert(msg_id, expiration);
                batch.push(msg.clone());
            }
        }
        PullResponse {
            batch,
            dead_letter
        }
    }

    /// Background janitor: sweeps expired TTL messages from the head of the ready_queue
    pub fn sweep_expired_ttl(&mut self) -> Vec<(String, Message)> {
        let ttl = match self.message_ttl {
            Some(ttl) => ttl,
            None => return Vec::new() // No TTL configured
        };

        let now = Instant::now();
        let mut dead_letter = Vec::new();

        while let Some(front_id) = self.ready_queue.front().cloned() {
            let is_expired = self.messages.get(&front_id).map_or(false, |msg| {
               now >= msg.created_at + ttl
            });

            if is_expired {
                self.ready_queue.pop_front();
                if let Some(removed_msg) = self.messages.remove(&front_id) {
                    if let Some(policy) = &self.dead_letter_policy {
                        dead_letter.push(self.dead_letter_policy_evaluator(
                            policy,
                            "ttl_expired",
                            removed_msg
                        ));
                    }
                }
            } else if !self.messages.contains_key(&front_id) {
                // Stale entry in read_queue -> pop & discard
                self.ready_queue.pop_front();
            } else {
                // Head message is valid; stop sweeping
                break;
            }
        }
        dead_letter
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
        let mut dead_letter = Vec::new();

        // 1. Collect expired IDs
        let expired_ids: Vec<String> = self
            .in_flight
            .iter()
            .filter(|(_, deadline)| now > **deadline)
            .map(|(id, _)| id.clone())
            .collect();

        for msg_id in expired_ids {
            self.in_flight.remove(&msg_id);

            if let Some(msg) = self.messages.get(&msg_id) {
                let exceeds_dlq_attempt = self
                    .dead_letter_policy
                    .as_ref()
                    .map_or(false, |p| msg.delivery_attempt >= p.max_delivery_attempts);

                let is_ttl_expired = self
                    .message_ttl
                    .map_or(false, |ttl| now >= msg.created_at + ttl);

                if exceeds_dlq_attempt || is_ttl_expired {
                    if let Some(removed_msg) = self.messages.remove(&msg_id) {
                        if let Some(policy) = &self.dead_letter_policy {
                            let reason = if is_ttl_expired {
                                "ttl_expired"
                            } else {
                                "max_delivery_attemps_exceeded"
                            };

                            dead_letter.push(self.dead_letter_policy_evaluator(
                                policy,
                                "ttl_expired",
                                removed_msg
                            ));
                        }
                        // If no DLQ policy is set, message drop automatically
                    }
                } else {
                    self.ready_queue.push_front(msg_id);
                }
            }
        }
        dead_letter
    }

    // nack(negative acknowledgment),
    pub fn nack_batch(&mut self, message_ids: &[String]) -> Vec<(String, Message)> {
        let mut dead_letter = Vec::new();

        for message_id in message_ids {
            self.in_flight.remove(message_id);

            if let Some(mut msg) = self.messages.remove(message_id) {
                if let Some(policy) = &self.dead_letter_policy {
                    dead_letter.push(self.dead_letter_policy_evaluator(
                        policy,
                        "nack unable to process the message",
                        msg
                    ));
                }
            }
        }
        dead_letter
    }
}

#[cfg(test)]
mod subscription_tests {
    use super::*;
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;
    use bytes::Bytes;

    fn dummy_msg(payload_str: &str) -> Message {
        Message {
            id: payload_str.to_string(),
            payload: payload_str.as_bytes().to_vec().into(),
            attributes: HashMap::new(),
            delivery_attempt: 0,
            created_at: Instant::now()
        }
    }

    fn create_msg(id: &str) -> Message {
        Message {
            id: id.to_string(),
            payload: b"test payload".to_vec().into(),
            attributes: HashMap::new(),
            delivery_attempt: 0,
            created_at: Instant::now()
        }
    }

    #[test]
    fn test_empty_subscription_pull() {
        let mut sub = Subscription::new("sub-empty".to_string(), Duration::from_secs(10), 5, None, None, None);

        let batch = sub.pull_batch(None);
        assert!(batch.batch.is_empty());

        let batch_zero = sub.pull_batch(Some(0));
        assert!(batch_zero.batch.is_empty());
    }

    #[test]
    fn test_batch_size_boundaries() {
        let mut sub = Subscription::new("sub-bounds".to_string(), Duration::from_secs(10), 2, None, None, None);

        for i in 1..10 {
            sub.push(dummy_msg(&format!("msg-{i}")));
        }

        // batch_size = 0 falls back to self.batch_size (2)
        let batch_zero = sub.pull_batch(Some(0)).batch;
        assert_eq!(batch_zero.len(), 2);

        // batch_size = None falls back to self.batch_size (2)
        let batch_none = sub.pull_batch(None).batch;
        assert_eq!(batch_none.len(), 2);

        // batch_size larger than remaining items (returns remaining 5)
        let batch_size = sub.pull_batch(Some(100)).batch;
        assert_eq!(batch_size.len(), 5);

        // Edge case 4: Queue is now empty
        assert!(sub.pull_batch(None).batch.is_empty());
    }

    #[test]
    fn test_double_ack_and_invalid_ack() {
        let mut sub = Subscription::new("sub-ack".to_string(), Duration::from_secs(10), 1, None, None, None);
        let msg = dummy_msg("hello");
        let msg_id = msg.id.clone();
        sub.push(msg);

        // case 1: ACK non-existent ID
        assert_eq!(sub.ack_batch(&["non-existent-uuid".to_string()]), 0);

        // Pull the message to move it to InFlight
        let pulled = sub.pull_batch(None).batch;
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
        let mut sub = Subscription::new("sub-ttl".to_string(), Duration::from_millis(30), 10, None, None, None);

        let msg1 = dummy_msg("msg-1");
        let msg1_id = msg1.id.clone();
        sub.push(msg1);

        // 1st Pull: attempt counter = 1
        let batch1 = sub.pull_batch(None).batch;
        assert_eq!(batch1[0].delivery_attempt, 1);

        // Immediate pull returns empty (msg1 is InFlight)
        assert!(sub.pull_batch(None).batch.is_empty());

        // Wait for deadline to expire (40ms > 30ms)
        sleep(Duration::from_millis(40));

        // Requeue timed-out messages back to ready_queue
        sub.requeue_expired();

        // 2nd Pull: msg1 re-claimed, attempt counter = 2
        let batch2 = sub.pull_batch(None).batch;
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
        let batch = sub.pull_batch(Some(1)).batch;
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
            None,
        );

        sub.push(create_msg("m1"));
        sub.push(create_msg("m2"));
        sub.push(create_msg("m3"));

        // 1. First pull gets m1 and m2 in strict FIFO order
        let batch_1 = sub.pull_batch(Some(2)).batch;
        assert_eq!(batch_1.len(), 2);
        assert_eq!(batch_1[0].id, "m1");
        assert_eq!(batch_1[1].id, "m2");

        // 2. Next pull gets remaining m3
        let batch_2 = sub.pull_batch(Some(2)).batch;
        assert_eq!(batch_2.len(), 1);
        assert_eq!(batch_2[0].id, "m3");

        // 3. Subsequent pull gets nothing
        let batch_empty = sub.pull_batch(Some(2)).batch;
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
            None,
        );

        sub.push(create_msg("m1"));
        let pulled = sub.pull_batch(Some(1)).batch;
        assert_eq!(pulled.len(), 1);

        // ACK message
        let ack_success = sub.ack_batch(&["m1".to_string()]);
        assert_eq!(ack_success, 1);

        // Pulling again should yield nothing
        let pulled_again = sub.pull_batch(Some(1)).batch;
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
            None,
        );

        sub.push(create_msg("m1"));
        sub.push(create_msg("m2"));

        // Simulate out-of-band deletion or ACK before pull (stale ID in ready_queue)
        sub.pull_batch(Some(2));
        sub.ack_batch(&["m1".to_string()]);
        sleep(Duration::from_millis(20));
        sub.requeue_expired();
        // Pull should skip missing 'm1' silently and return 'm2'
        let batch = sub.pull_batch(Some(2)).batch;
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
            None,
        );

        sub.push(create_msg("m1"));
        let batch = sub.pull_batch(Some(1)).batch;
        assert_eq!(batch.len(), 1);

        // Wait for deadline to pass
        sleep(Duration::from_millis(30));

        // Scan and requeue expired in-flight messages
        sub.requeue_expired();

        // Message should be back in ready_queue and pullable
        let requeued_batch = sub.pull_batch(Some(1)).batch;
        assert_eq!(requeued_batch.len(), 1);
        assert_eq!(requeued_batch[0].id, "m1");
    }

    #[test]
    fn test_subscription_nack_without_dlq_purges_message() {
        let mut sub = Subscription::new(
            "sub-nack-nodlq".to_string(),
            Duration::from_secs(10),
            10,
            None,
            None,
            None,
        );

        sub.push(dummy_msg("msg-1"));
        sub.push(dummy_msg("msg-2"));

        let pulled = sub.pull_batch(Some(2)).batch;
        assert_eq!(pulled.len(), 2);

        // NACK without DLQ policy should delete the messages permanently
        let evicted = sub.nack_batch(&["msg-1".to_string()]);
        assert!(evicted.is_empty(), "No messages should be routed when DLQ is not configured");

        // msg-1 is gone, sub length is now 1 (only msg-2 remains in_flight)
        assert_eq!(sub.len(), 1);

        // ACK msg-2 and ensure queue is empty
        assert_eq!(sub.ack_batch(&["msg-2".to_string()]), 1);
        assert_eq!(sub.len(), 0);
    }

    #[test]
    fn test_subscription_nack_with_dlq_evicts_to_dlq_topic() {
        let dlq_policy = DeadLetterPolicy {
            dead_letter_queue: "dlq-orders".to_string(),
            max_delivery_attempts: 5,
        };

        let mut sub = Subscription::new(
            "sub-nack-dlq".to_string(),
            Duration::from_secs(10),
            10,
            Some(5),
            None,
            Some(dlq_policy)
        );

        sub.push(dummy_msg("poison-msg-101"));

        let pulled = sub.pull_batch(Some(1)).batch;
        assert_eq!(pulled.len(), 1);

        // NACK message with DLQ policy configured
        let evicted = sub.nack_batch(&["poison-msg-101".to_string()]);
        assert_eq!(evicted.len(), 1);

        let (dlq_target, msg) = &evicted[0];
        assert_eq!(dlq_target, "dlq-orders");
        assert_eq!(msg.id, "poison-msg-101");

        // Check enriched metadata attributes
        assert_eq!(
            msg.attributes.get("x-dead-letter-source-subscription").map(|s| s.as_str()),
            Some("sub-nack-dlq")
        );
        assert_eq!(
            msg.attributes.get("x-dead-letter-original-message-id").map(|s| s.as_str()),
            Some("poison-msg-101")
        );

        // Ensure subscription state is completely cleared
        assert_eq!(sub.len(), 0);
    }

    fn mock_message(id: &str) -> Message {
        Message {
            id: id.to_string(),
            payload: Bytes::from_static(b"test-payload"),
            attributes: HashMap::new(),
            delivery_attempt: 0,
            created_at: Instant::now(),
        }
    }

    #[test]
    fn test_sweep_expired_ttl_purges_expired_head_messages() {
        let mut sub = Subscription::new(
            "sub-ttl-sweep".to_string(),
            Duration::from_secs(10),
            10,
            None,
            Some(Duration::from_millis(50)), // 50ms TTL
            None,
        );

        sub.push(mock_message("msg-1"));
        sub.push(mock_message("msg-2"));

        // Sleep to let msg-1 and msg-2 expire
        sleep(Duration::from_millis(60));

        // Push fresh message
        sub.push(mock_message("msg-3-fresh"));

        // Sweep ready_queue
        let dlq_messages = sub.sweep_expired_ttl();

        // Expired messages are purged, no DLQ configured
        assert!(dlq_messages.is_empty());
        assert_eq!(sub.len(), 1);

        // pull_batch should only see the fresh message
        let res = sub.pull_batch(Some(10));
        assert_eq!(res.batch.len(), 1);
        assert_eq!(res.batch[0].id, "msg-3-fresh");
    }

    #[test]
    fn test_sweep_expired_ttl_routes_to_dlq_when_configured() {
        let dlq_policy = DeadLetterPolicy {
            dead_letter_queue: "dlq-topic".to_string(),
            max_delivery_attempts: 5,
        };

        let mut sub = Subscription::new(
            "sub-ttl-dlq".to_string(),
            Duration::from_secs(10),
            10,
            None,
            Some(Duration::from_millis(50)),
            Some(dlq_policy),
        );

        sub.push(mock_message("expired-poison-1"));

        sleep(Duration::from_millis(60));

        let dlq_messages = sub.sweep_expired_ttl();

        assert_eq!(dlq_messages.len(), 1);
        let (target_topic, msg) = &dlq_messages[0];
        assert_eq!(target_topic, "dlq-topic");
        assert_eq!(msg.id, "expired-poison-1");
        assert_eq!(
            msg.attributes.get("x-dead-letter-reason").map(|s| s.as_str()),
            Some("ttl_expired")
        );
        assert_eq!(sub.len(), 0);
    }

    #[test]
    fn test_pull_batch_lazy_ttl_drop() {
        let mut sub = Subscription::new(
            "sub-lazy-ttl".to_string(),
            Duration::from_secs(10),
            10,
            None,
            Some(Duration::from_millis(50)),
            None,
        );

        sub.push(mock_message("msg-1"));
        sleep(Duration::from_millis(60));

        // Message is expired inside ready_queue, pull_batch should lazily discard it
        let res = sub.pull_batch(Some(10));

        assert!(res.batch.is_empty());
        assert_eq!(sub.len(), 0);
    }

    #[test]
    fn test_requeue_expired_evicts_ttl_expired_in_flight() {
        let dlq_policy = DeadLetterPolicy {
            dead_letter_queue: "dlq-topic".to_string(),
            max_delivery_attempts: 10, // High attempt limit
        };

        let mut sub = Subscription::new(
            "sub-in-flight-ttl".to_string(),
            Duration::from_millis(20), // 20ms visibility timeout
            10,
            None,
            Some(Duration::from_millis(50)), // 50ms TTL
            Some(dlq_policy),
        );

        sub.push(mock_message("msg-in-flight"));

        // Pull message into in_flight
        let res = sub.pull_batch(Some(1));
        assert_eq!(res.batch.len(), 1);

        // Sleep past BOTH visibility deadline and TTL expiration
        sleep(Duration::from_millis(60));

        // Requeue should discover that TTL expired while message was in_flight
        let dlq_messages = sub.requeue_expired();

        assert_eq!(dlq_messages.len(), 1);
        let (target_topic, msg) = &dlq_messages[0];
        assert_eq!(target_topic, "dlq-topic");
        assert_eq!(msg.id, "msg-in-flight");
        assert_eq!(
            msg.attributes.get("x-dead-letter-reason").map(|s| s.as_str()),
            Some("ttl_expired")
        );
        assert_eq!(sub.len(), 0);
    }

    #[test]
    fn test_sweep_expired_ttl_noop_when_no_ttl_set() {
        let mut sub = Subscription::new(
            "sub-no-ttl".to_string(),
            Duration::from_secs(10),
            10,
            None,
            None, // No TTL
            None,
        );

        sub.push(mock_message("msg-permanent"));

        let dlq_messages = sub.sweep_expired_ttl();

        assert!(dlq_messages.is_empty());
        assert_eq!(sub.len(), 1);
    }
}