use std::collections::HashMap;
use std::time::Duration;

pub use crate::engine::subscription::Subscription;
use crate::model::{DeadLetterPolicy, Message, PushConfig};



pub struct Topic {
    pub name: String,
    // Maps subscription name to its Subscription state
    pub subscription: HashMap<String, Subscription>
}

impl Topic {
    pub fn new(name: String) -> Self {
        Self {
            name,
            subscription: HashMap::new()
        }
    }

    /// Register a new subscription under this topic
    pub fn create_subscription(&mut self, name: &str, ack_deadline: Duration, batch_size: usize, max_outstanding_messages: Option<usize>, message_ttl: Option<Duration>, dead_letter_policy: Option<DeadLetterPolicy>,) -> bool {
        if self.subscription.contains_key(name) {
            return false; // Already exists
        }
        let sub = Subscription::new(
            name.to_string(),
            ack_deadline,
            batch_size,
            max_outstanding_messages,
            message_ttl,
            dead_letter_policy
        );
        self.subscription.insert(name.to_string(), sub);
        true
    }

    pub fn create_push_subscription(&mut self, name: &str, ack_deadline: Duration, push_config: PushConfig, batch_size: usize, max_outstanding_messages: Option<usize>, message_ttl: Option<Duration>, dead_letter_policy: Option<DeadLetterPolicy>) -> bool {
        if self.subscription.contains_key(name) {
            return false; // Already exits
        }

        let sub = Subscription::new_push(
            name.to_string(),
            ack_deadline,
            push_config,
            batch_size,
            max_outstanding_messages,
            message_ttl,
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

#[cfg(test)]
mod topic_tests {
    use super::*;
    use std::collections::HashMap;

    fn dummy_msg(payload_str: &str) -> Message {
        Message::new(bytes::Bytes::from(payload_str.as_bytes().to_vec()), HashMap::new())
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

        topic.create_subscription("sub-billing", Duration::from_secs(10), 5, None, None, None);
        topic.create_subscription("sub-analytics", Duration::from_secs(10), 5, None, None, None);

        let msg = dummy_msg("Order #99");
        let msg_id = msg.id.clone();
        topic.publish(msg);

        // Both subscriptions must receive their own copy
        let billing_msgs = {
            let sub = topic.get_subscription_mut("sub-billing").unwrap();
            sub.pull_batch(None).batch
        };
        assert_eq!(billing_msgs.len(), 1);

        let analytics_msgs = {
            let sub = topic.get_subscription_mut("sub-analytics").unwrap();
            sub.pull_batch(None).batch
        };
        assert_eq!(analytics_msgs.len(), 1);

        // Acking on sub-billing remove it from sub-billing ONLY
        {
            let billing_sub = topic.get_subscription_mut("sub-billing").unwrap();
            let arr = &[msg_id.clone()];
            assert_eq!(billing_sub.ack_batch(arr), 1);
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
        topic.create_subscription("sub-unbounded", Duration::from_secs(10), 10, None, None, None);
        // Sub B: Capped at 1 message
        topic.create_subscription("sub-capped", Duration::from_secs(10), 10, Some(1), None, None);

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
