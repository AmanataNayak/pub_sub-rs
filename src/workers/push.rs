use std::time::Duration;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use crate::engine::Engine;
use crate::model::{PushConfig};
use tokio::time::interval;
use tokio::sync::Notify;
use std::sync::Arc;
use tokio::task::JoinSet;

pub fn spawn_push_workers(topic_name: String, sub_name: String, push_config: PushConfig, batch_size: usize, notify: Arc<Notify>, engine: Engine) {
    tokio::spawn(async move {
        let timeout = Duration::from_secs(push_config.timeout_secs.max(1));

        // pre-build static custom header once outside the worker loop
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));

        for (k, v) in &push_config.headers {
            if let (Ok(name), Ok(val)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v)) {
                default_headers.insert(name, val);
            }
        }

        // Configure HTTP Client with persistent connection pooling

        let client = Client::builder()
            .timeout(timeout)
            .default_headers(default_headers)
            .pool_max_idle_per_host(100)
            .build()
            .unwrap_or_else(|_| Client::new());

        let mut ticker = interval(timeout);

        loop {
            // Inner drain loop
            loop {
                let messages = match engine.pull_batch(&topic_name, &sub_name, Some(batch_size)) {
                    Ok(msgs) if !msgs.is_empty() => msgs,
                    _ => break,
                };

                let mut join_set = JoinSet::new();

                // Concurrent HTTP DISPATCH
                for msg in messages {
                    let client = client.clone();
                    let endpoint = push_config.push_endpoint.clone();
                    let sub_name = sub_name.clone();

                    join_set.spawn(async move {
                        let resp = client
                            .post(&endpoint)
                            .header("X-PubSub-Message-Id", &msg.id)
                            .header("X-PubSub-Subscription", &sub_name)
                            .header("X-PubSub-Delivery-Attempt", msg.delivery_attempt)
                            .body(msg.payload.clone())
                            .send()
                            .await;

                        match resp {
                            // Success mark for batch ack
                            Ok(r) if r.status().is_success() => (Some(msg.id), None),
                            // client error of NACK (DLQ/Purge)
                            Ok(r) if r.status().is_client_error() => (None, Some(msg.id)),
                            _ => (None, None)
                        }
                    });
                }

                // collect results & batch lock mutations
                let mut ack_ids = Vec::with_capacity(batch_size);
                let mut nack_ids = Vec::with_capacity(batch_size);


                while let Some(res) = join_set.join_next().await {
                    if let Ok((ack_id, nack_id)) = res {
                        if let Some(id) = ack_id {
                            ack_ids.push(id);
                        }
                        if let Some(id) = nack_id {
                            nack_ids.push(id);
                        }
                    }
                }

                // Acquire engine write lock ONCE per batch instead of 2N times
                if !ack_ids.is_empty() {
                    let _ = engine.ack_batch(&topic_name, &sub_name, &ack_ids);
                }
                if !nack_ids.is_empty() {
                    let _ = engine.nack_batch(&topic_name, &sub_name, &nack_ids);
                }
            }

            // Only sleep here when ready_queue is completely empty (0 messages)
            tokio::select! {
                _ = notify.notified() => {},
                _ = ticker.tick() => {},
            }
        }
    });
}

#[cfg(test)]
mod push_worker_tests {
    use super::*;
    use std::collections::HashMap;
    use crate::model::Message;
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt}; // Trait imports required for socket.read/write
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_spawn_push_worker_direct_success() {
        // 1. Mock HTTP Server listening on an available local port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{}", addr);

        let (tx, mut rx) = mpsc::channel::<String>(1);

        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let n = socket.read(&mut buf).await.unwrap();
                let req_text = String::from_utf8_lossy(&buf[..n]).to_string();
                tx.send(req_text).await.unwrap();

                // Respond with HTTP 200 OK
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        // 2. Setup Engine, Topic, and Base Subscription
        let engine = Engine::new();
        engine.create_topic("worker-topic").unwrap();
        engine
            .create_subscription(
                "worker-topic",
                "worker-sub",
                Duration::from_secs(10),
                Some(1),
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let notify = engine
            .get_subscription_notify("worker-topic", "worker-sub")
            .unwrap();

        // 3. Configure PushConfig
        let mut headers = HashMap::new();
        headers.insert("X-Worker-Source".to_string(), "UnitTest".to_string());

        let push_config = PushConfig {
            push_endpoint: endpoint,
            headers,
            timeout_secs: 2,
        };

        // 4. Directly spawn worker
        spawn_push_workers(
            "worker-topic".to_string(),
            "worker-sub".to_string(),
            push_config,
            1,
            notify.clone(),
            engine.clone(),
        );

        // 5. Publish message & trigger notify signal
        engine
            .publish(
                "worker-topic",
                Message {
                    id: "msg-worker-101".to_string(),
                    payload:bytes::Bytes::from(b"Direct Worker Payload".to_vec()),
                    attributes: HashMap::new(),
                    delivery_attempt: 0,
                    created_at: Instant::now()
                },
            )
            .unwrap();

        notify.notify_waiters();

        // 6. Assert server receives request
        let req_raw = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("Worker should send HTTP request")
            .unwrap();

        let req_lower = req_raw.to_lowercase();
        assert!(req_raw.starts_with("POST"));
        assert!(req_lower.contains("x-worker-source: unittest"));
        assert!(req_lower.contains("x-pubsub-message-id: msg-worker-101"));
        assert!(req_raw.contains("Direct Worker Payload"));

        // 7. Verify message was ACKed out of engine memory on 200 OK
        sleep(Duration::from_millis(150)).await;
        let pending = engine
            .pull_batch("worker-topic", "worker-sub", Some(1))
            .unwrap();
        assert!(
            pending.is_empty(),
            "Worker must ACK message on HTTP 200 response"
        );
    }

    #[tokio::test]
    async fn test_spawn_push_worker_http_failure_no_ack() {
        // 1. Mock Server returning HTTP 500 Error
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{}", addr);

        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;

                // Respond with 500 Internal Server Error
                let response =
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        // 2. Setup Engine with short ack deadline (500ms)
        let engine = Engine::new();
        engine.create_topic("fail-topic").unwrap();
        engine
            .create_subscription(
                "fail-topic",
                "fail-sub",
                Duration::from_millis(500),
                Some(1),
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let notify = engine
            .get_subscription_notify("fail-topic", "fail-sub")
            .unwrap();

        let push_config = PushConfig {
            push_endpoint: endpoint,
            headers: HashMap::new(),
            timeout_secs: 2,
        };

        // 3. Directly spawn worker
        spawn_push_workers(
            "fail-topic".to_string(),
            "fail-sub".to_string(),
            push_config,
            1,
            notify.clone(),
            engine.clone(),
        );

        // 4. Publish message
        engine
            .publish(
                "fail-topic",
                Message {
                    id: "msg-fail-001".to_string(),
                    payload:bytes::Bytes::from(b"Failing Payload".to_vec()),
                    attributes: HashMap::new(),
                    delivery_attempt: 0,
                    created_at: Instant::now()
                },
            )
            .unwrap();

        notify.notify_waiters();

        // 5. Allow worker time to execute POST and receive HTTP 500
        sleep(Duration::from_millis(200)).await;

        // 6. Wait for visibility timeout (500ms) to pass so message returns to Ready state
        sleep(Duration::from_millis(600)).await;

        // 7. Verify message is STILL present in engine memory

        engine.process_expired_messages();

        let pending = engine
            .pull_batch("fail-topic", "fail-sub", Some(1))
            .unwrap();
        assert_eq!(
            pending.len(),
            1,
            "Worker must NOT ACK message when endpoint returns HTTP 500"
        );
        assert_eq!(pending[0].id, "msg-fail-001");
    }
}