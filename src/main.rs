use std::cmp::max;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

// Include tonic generated protobuf module
pub mod pubsub {
    tonic::include_proto!("pubsub");
}

use pubsub::pub_sub_service_server::PubSubServiceServer;

mod engine;
mod model;
mod service; // Contains your PubSubService struct implementing tonic gRPC handlers
mod wal;
mod errors;
mod push;

use engine::Engine;
use service::MyPubSubService;
use wal::{WalEntry, WalManager};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal_path = "pubsub.wal";

    // 1. First open the WAL manager
    let wal = Arc::new(WalManager::open(wal_path)?);

    // 2. Create Engine initialized with WAL
    let engine = Engine::with_wal(wal);

    // 3. Recover entries directly into the active engine instance
    let recovered_entries = WalManager::recover(wal_path)?;
    println!("Recovered {} entries from WAL", recovered_entries.len());

    for entry in recovered_entries {
        match entry {
            WalEntry::CreateTopic { topic } => {
                let _ = engine.create_topic(&topic);
            }
            WalEntry::CreateSubscription {
                topic,
                subscription,
                ack_deadline_sec,
                batch_size,
                max_outstanding_messages,
                dead_letter_queue,
                max_delivery_attempts,
                push_endpoint,
                headers,
                timeout_secs,

            } => {
                let ack_deadline = Duration::from_secs(ack_deadline_sec);
                if let Some(pe) = push_endpoint {
                    let _ = engine.create_push_subscription(
                        &topic,
                        &subscription,
                        &pe,
                        headers.unwrap(),
                        timeout_secs,
                        ack_deadline,
                        max_outstanding_messages,
                        dead_letter_queue,
                        max_delivery_attempts,
                    );
                } else {
                    let _ = engine.create_subscription(
                        &topic,
                        &subscription,
                        ack_deadline,
                        Some(batch_size),
                        max_outstanding_messages,
                        dead_letter_queue,
                        max_delivery_attempts,
                    );
                }
            }
            WalEntry::Publish { topic, message } => {
                let _ = engine.publish(&topic, message);
            }
            WalEntry::Ack {
                topic,
                subscription,
                message_id,
            } => {
                let _ = engine.ack(&topic, &subscription, &message_id);
            }
        }
    }

    // 4. Instantiate gRPC Service wrapper
    let pubsub_service = MyPubSubService::new(Arc::new(engine));

    // 5. Bind gRPC server to 0.0.0.0:50051
    let addr: SocketAddr = "0.0.0.0:50051".parse()?;
    println!("Pub/Sub gRPC server running on {}", addr);

    Server::builder()
        .add_service(PubSubServiceServer::new(pubsub_service))
        .serve(addr)
        .await?;

    Ok(())
}

