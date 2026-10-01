use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Notify;

pub use crate::engine::topic::Topic;
use crate::errors::PubSubError;
use crate::model::{DeadLetterPolicy, Message, PushConfig};
use crate::workers::spawn_push_workers;
use crate::storage::{WalEntry, WalManager};


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
            let topic_arc = self.get_topic(topic_name)?;
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
        let topic_arc = self.get_topic(topic_name)?;
        let mut topic = topic_arc.write().unwrap();

        let sub = topic
            .get_subscription_mut(sub_name)
            .ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;

        let batch = sub.pull_batch(batch_size_override);

        Ok(batch)
    }

    /// Acknowledges a message on a subscription
    pub fn ack_batch(&self, topic_name: &str, sub_name: &str, message_ids: &[String]) -> Result<usize, PubSubError> {
        if message_ids.is_empty() {
            return Ok(0);
        }
        let topic_arc = self.get_topic(topic_name)?;
        let mut topic = topic_arc.write().unwrap();
        let sub = topic.get_subscription_mut(sub_name).ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;


        // 2. WRITE TO WAL
        if let Some(wal) = &self.wal {
            let entry = WalEntry::Ack {
                topic: topic_name.to_string(),
                subscription: sub_name.to_string(),
                message_ids: message_ids.to_vec(),
            };
            wal.append(&entry)
                .map_err(|e| PubSubError::IoError(e.to_string()))?;
        }

        // 3. MUTATE IN-MEMORY STATE
        Ok(sub.ack_batch(message_ids))
    }

    pub fn get_subscription_notify(&self, topic_name: &str, sub_name: &str) -> Result<Arc<Notify>, PubSubError> {
        let topic_arc = self.get_topic(topic_name)?;

        let topic = topic_arc.read().unwrap();

        let sub = topic.get_subscription(sub_name)
            .ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;

        // Returns a clone of the Arc<Notify> pointer
        Ok(sub.notify_handler())
    }

    pub fn process_expired_messages(&self) {
        let mut dead_letters = Vec::new();

        // Collect arc pointers to all topics and drop the outer self.topics lock
        let topic_arcs: Vec<Arc<RwLock<Topic>>> = {
            let topics = self.topics.read().unwrap();
            topics.values().cloned().collect()
        };

        // Iterate each topic, acquire its write lock & run requeue_expired()
        for topic_arc in topic_arcs {
            let mut topic = topic_arc.write().unwrap();

            // Iterate mutably through all subscriptions inside this topic
            for sub in topic.subscription.values_mut() {
                let evicted = sub.requeue_expired();
                dead_letters.extend(evicted);
            }
        } // All `Topic` write locks are dropped HERE

        // Now that ALL locks are released, route dead-letter messages safely
        for (dlq, msg) in dead_letters {
            if !dlq.is_empty() {
                let _ = self.publish(&dlq, msg);
            }
        }
    }

    /// nack to delete failed message and pushed to dlq(if available)
    pub fn nack_batch(&self, topic_name: &str, sub_name: &str, message_ids: &[String]) -> Result<usize, PubSubError> {
        let mut dead_letters = Vec::new();
        {
            let topic_arc = self.get_topic(topic_name)?;

            let mut topic = topic_arc.write().unwrap();

            let mut sub = topic.get_subscription_mut(sub_name)
                .ok_or_else(|| PubSubError::SubscriptionNotFound(sub_name.to_string()))?;

            if let Some(wal) = &self.wal {
                let entry = WalEntry::Nack {
                    topic: topic_name.to_string(),
                    subscription: sub_name.to_string(),
                    message_ids: message_ids.to_vec(),
                };
                wal.append(&entry)
                    .map_err(|e| PubSubError::IoError(e.to_string()))?;
            }

            let evicted = sub.nack_batch(message_ids);
            dead_letters.extend(evicted);
        }

        let mut nack_count = 0;
        for (dlq, msg) in dead_letters {
            self.publish(&dlq, msg);
            nack_count += 1;
        }

        Ok(nack_count)
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
    use crate::engine::subscription::Subscription;
    use std::thread::sleep;


    fn dummy_msg(payload_str: &str) -> Message {
        Message::new(bytes::Bytes::from(payload_str.as_bytes().to_vec()), HashMap::new())
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
                    let msg = Message::new(bytes::Bytes::from(payload), HashMap::new());
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
                            if engine_clone.ack_batch("events", "sub-workers", &[m.id]).is_ok() {}
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
            let _ = engine.ack_batch("events", "sub-workers", &[m.id]);
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
        engine.process_expired_messages();

        // Attempt 2: Pull message again (delivery_attempt = 2)
        let batch2 = engine.pull_batch("orders", "sub-payments", Some(1)).unwrap();
        assert_eq!(batch2.len(), 1);
        assert_eq!(batch2[0].delivery_attempt, 2);

        // Wait for visibility timeout to expire again
        thread::sleep(Duration::from_millis(60));
        engine.process_expired_messages();

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
                    payload: bytes::Bytes::from(msg_payload),
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

    #[test]
    fn test_engine_sweep_routes_poison_message_to_dlq_topic() {
        let engine = Engine::new();

        // 1. Setup primary topic and DLQ topic
        let main_topic = "orders";
        let dlq_topic = "orders-dlq";
        engine.create_topic(main_topic).unwrap();
        engine.create_topic(dlq_topic).unwrap();

        // 2. Setup subscription with DLQ policy (max 1 attempt)
        let dlq_policy = DeadLetterPolicy {
            dead_letter_queue: dlq_topic.to_string(),
            max_delivery_attempts: 1,
        };

        engine
            .create_subscription(
                main_topic,
                "sub-orders",
                Duration::from_millis(20), // 20ms visibility deadline
                Some(10),
                None,
                Some( dlq_topic.to_string()),
                Some(1)
            )
            .unwrap();

        // Create a listener subscription on the DLQ topic to verify receipt
        engine
            .create_subscription(dlq_topic, "sub-dlq-listener", Duration::from_secs(10), Some(10), None, None, None)
            .unwrap();

        // 3. Publish message to main topic
        let msg = dummy_msg("m-poison");
        let msg_id = msg.id.clone();
        engine.publish(main_topic, msg).unwrap();

        // 4. Pull message once (delivery_attempt becomes 1)
        let pulled = engine.pull_batch(main_topic, "sub-orders", Some(1)).unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].id, msg_id);

        // 5. Allow visibility deadline to expire (30ms > 20ms)
        sleep(Duration::from_millis(30));

        // 6. Trigger Engine expiration sweep
        engine.process_expired_messages();

        // 7. Verify message is removed from primary subscription
        let primary_pull = engine.pull_batch(main_topic, "sub-orders", Some(1)).unwrap();
        assert!(primary_pull.is_empty(), "Poison message should be evicted from main subscription");

        // 8. Verify message was routed to the DLQ topic and received by DLQ listener
        let dlq_pulled = engine.pull_batch(dlq_topic, "sub-dlq-listener", Some(1)).unwrap();
        assert_eq!(dlq_pulled.len(), 1, "DLQ topic should contain the evicted message");
        assert_eq!(dlq_pulled[0].id, msg_id);

        // Verify metadata attributes attached by Subscription
        assert_eq!(
            dlq_pulled[0].attributes.get("x-dead-letter-source-subscription").map(|s| s.as_str()),
            Some("sub-orders")
        );
    }

    #[test]
    fn test_engine_sweep_requeues_normal_unexpired_retry() {
        let engine = Engine::new();

        let main_topic = "events";
        let dlq_topic = "events-dlq";
        engine.create_topic(main_topic).unwrap();
        engine.create_topic(dlq_topic).unwrap();

        // DLQ policy allows up to 3 attempts
        let dlq_policy = DeadLetterPolicy {
            dead_letter_queue: dlq_topic.to_string(),
            max_delivery_attempts: 3,
        };

        engine
            .create_subscription(
                main_topic,
                "sub-events",
                Duration::from_millis(20),
                Some(10),
                None,
                Some(dlq_topic.to_string()),
                Some(5)
            )
            .unwrap();
        let msg = dummy_msg("m-retry");
        let msg_id = msg.id.clone();

        engine.publish(main_topic, msg).unwrap();

        // Pull 1 (attempt 1)
        let _ = engine.pull_batch(main_topic, "sub-events", Some(1)).unwrap();
        sleep(Duration::from_millis(30));

        // Sweep should requeue to ready_queue because attempts (1) < max_attempts (3)
        engine.process_expired_messages();

        // Pull 2 should retrieve the message again with incremented attempt
        let repulled = engine.pull_batch(main_topic, "sub-events", Some(1)).unwrap();
        assert_eq!(repulled.len(), 1);
        assert_eq!(repulled[0].id, msg_id);
        assert_eq!(repulled[0].delivery_attempt, 2);
    }

    #[test]
    fn test_subscription_ack_batch_removes_inflight_and_storage() {
        let mut sub = Subscription::new(
            "sub-batch-ack".to_string(),
            Duration::from_secs(10),
            10,
            None,
            None,
        );

        sub.push(dummy_msg("msg-1"));
        sub.push(dummy_msg("msg-2"));
        sub.push(dummy_msg("msg-3"));

        // Pull 3 messages into InFlight
        let pulled = sub.pull_batch(Some(3));
        assert_eq!(pulled.len(), 3);

        let ids: Vec<String> = pulled.iter().map(|m| m.id.clone()).collect();

        // Batch ACK all 3 messages in a single call
        let acked_count = sub.ack_batch(&ids);
        assert_eq!(acked_count, 3);

        // Verify storage is empty and pulling returns nothing
        assert_eq!(sub.len(), 0);
        assert!(sub.pull_batch(Some(10)).is_empty());
    }

    #[test]
    fn test_subscription_ack_batch_handles_partial_and_invalid_ids() {
        let mut sub = Subscription::new(
            "sub-partial-ack".to_string(),
            Duration::from_secs(10),
            10,
            None,
            None,
        );

        sub.push(dummy_msg("valid-1"));
        sub.push(dummy_msg("valid-2"));

        let pulled = sub.pull_batch(Some(2));
        let mut ids: Vec<String> = pulled.iter().map(|m| m.id.clone()).collect();

        // Inject non-existent ID into the batch
        ids.push("ghost-id-999".to_string());

        // Batch ACK should succeed for 2 valid messages and ignore the missing ID
        let acked_count = sub.ack_batch(&ids);
        assert_eq!(acked_count, 2);

        // Re-ACKing the same batch should return 0 (already removed)
        let re_ack_count = sub.ack_batch(&ids);
        assert_eq!(re_ack_count, 0);
    }

    #[test]
    fn test_engine_nack_routes_poison_message_to_dlq() {
        let engine = Engine::new();

        let main_topic = "orders";
        let dlq_topic = "orders-dlq";

        engine.create_topic(main_topic).unwrap();
        engine.create_topic(dlq_topic).unwrap();

        // Create DLT listener subscription
        engine
            .create_subscription(dlq_topic, "sub-dlq-audit", Duration::from_secs(10), Some(10), None, None, None)
            .unwrap();

        // Create main subscription with DLQ enabled
        engine
            .create_subscription(
                main_topic,
                "sub-orders-proc",
                Duration::from_secs(10),
                Some(10),
                None,
                Some(dlq_topic.to_string()),
                Some(3),
            )
            .unwrap();

        // Publish and pull
        let msg = dummy_msg("order-corrupted");
        let msg_id = msg.id.clone();
        engine.publish(main_topic, msg).unwrap();
        let batch = engine.pull_batch(main_topic, "sub-orders-proc", Some(1)).unwrap();
        assert_eq!(batch.len(), 1);

        // NACK through Engine
        let nacked_count = engine
            .nack_batch(main_topic, "sub-orders-proc", &[batch[0].id.clone()])
            .unwrap();
        assert_eq!(nacked_count, 1);

        // Main subscription should be empty
        let main_pull = engine.pull_batch(main_topic, "sub-orders-proc", Some(1)).unwrap();
        assert!(main_pull.is_empty());

        // DLQ subscription should receive the nacked message
        let dlq_pull = engine.pull_batch(dlq_topic, "sub-dlq-audit", Some(1)).unwrap();
        assert_eq!(dlq_pull.len(), 1);
        assert_eq!(dlq_pull[0].id, msg_id);
        assert_eq!(
            dlq_pull[0].attributes.get("x-dead-letter-source-subscription").map(|s| s.as_str()),
            Some("sub-orders-proc")
        );
    }
}