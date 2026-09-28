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
            } => {
                let _ = engine.create_subscription(
                    &topic,
                    &subscription,
                    Duration::from_secs(ack_deadline_sec),
                    Some(batch_size)
                );
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

// fn handle_command(engine: &Engine, cmd: Command) -> Response {
//     match cmd {
//         Command::CreateTopic { topic } => match engine.create_topic(&topic) {
//             Ok(()) => Response::Ok,
//             Err(err) => Response::Error(err.to_string())
//         }
//         Command::CreateSubscription { topic, subscription, ack_deadline_secs } => {
//             match engine.create_subscription(&topic, &subscription, Duration::from_secs(ack_deadline_secs)) {
//                 Ok(()) => Response::Ok,
//                 Err(err) => Response::Error(err.to_string()),
//             }
//         }
//         Command::Publish { topic, payload, attributes} => {
//             let msg = Message::new(payload, attributes);
//             match engine.publish(&topic, msg) {
//                 Ok(_) => Response::Ok,
//                 Err(err) => Response::Error(err.to_string())
//             }
//         }
//         Command::Pull { topic, subscription } => match engine.pull(&topic, &subscription) {
//             Ok(Some(msg)) => Response::Message(msg),
//             Ok(None) => Response::Ok, // Queue empty
//             Err(err) => Response::Error(err.to_string())
//         }
//         Command::Ack { topic, subscription, message_id } => match engine.ack(&topic, &subscription, &message_id) {
//              Ok(()) => Response::Ok,
//             Err(err) => Response::Error(err.to_string())
//         }
//     }
// }
