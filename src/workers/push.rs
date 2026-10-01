use tokio::sync::mpsc::Receiver;
use std::time::Duration;
use reqwest::Client;
use crate::engine::Engine;
use crate::model::{Message, PushConfig};
use tokio::time::interval;
use tokio::sync::Notify;
use std::sync::Arc;

pub fn spawn_push_workers(topic_name: String, sub_name: String, push_config: PushConfig, notify: Arc<Notify>, engine: Engine) {
    tokio::spawn(async move {
        let client = Client::builder()
            .timeout(Duration::from_secs(push_config.timeout_secs.max(1)))
            .build()
            .unwrap_or_else(|_| Client::new());

        let mut ticker = interval(Duration::from_secs(push_config.timeout_secs.max(1)));

        loop {
            if let Ok(messages) = engine.pull_batch(&topic_name, &sub_name, Some(1)) {
                if let Some(msg) = messages.into_iter().next() {
                    let mut req = client.post(&push_config.push_endpoint)
                        .body(msg.payload.clone())
                        .header("Content-Type", "application/octet-stream")
                        .header("X-PubSub-Message-Id", &msg.id)
                        .header("X-PubSub-Subscription", &sub_name)
                        .header("X-PubSub-Delivery-Attempt", msg.delivery_attempt);

                    // Inject user-defined custom headers
                    for (k, v) in &push_config.headers {
                        req = req.header(k, v);
                    }

                    if let Ok(resp) = req.send().await {
                        if resp.status().is_success() {
                            let _ = engine.ack_batch(&topic_name, &sub_name, &[msg.id]);
                        }
                    }
                }
            }
            // Wait for the new publish notification OR visibility timeout ticks
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
    use std::sync::Arc;
    use std::time::Duration;
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
            notify.clone(),
            engine.clone(),
        );

        // 5. Publish message & trigger notify signal
        engine
            .publish(
                "worker-topic",
                Message {
                    id: "msg-worker-101".to_string(),
                    payload: b"Direct Worker Payload".to_vec(),
                    attributes: HashMap::new(),
                    delivery_attempt: 0,
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
            notify.clone(),
            engine.clone(),
        );

        // 4. Publish message
        engine
            .publish(
                "fail-topic",
                Message {
                    id: "msg-fail-001".to_string(),
                    payload: b"Failing Payload".to_vec(),
                    attributes: HashMap::new(),
                    delivery_attempt: 0,
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