use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tonic::transport::Server;

// Include tonic generated protobuf module
pub mod pubsub {
    tonic::include_proto!("pubsub");
}

use pubsub::pub_sub_service_server::PubSubServiceServer;

mod engine;
mod model;
mod server; // Contains your PubSubService struct implementing tonic gRPC handlers
mod storage;
mod errors;
mod workers;

use engine::Engine;
use server::MyPubSubService;
use storage::WalManager;
use crate::storage::SnapshotStorage;

pub fn start_background_worker(engine: Arc<Engine>) {
    tokio::spawn(async move {
       let mut interval = time::interval(Duration::from_millis(50));

        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let engine_ref = Arc::clone(&engine);
            // Offload CPU/blocking lock operations off Tokio worker threads
            let _ = tokio::task::spawn_blocking(move || {
                engine_ref.process_expired_messages();
            }).await;
        }
    });
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = Path::new("./data");
    let storage = SnapshotStorage::new(data_dir);

    // Load latest snapshot or initialize empty Engine
    let (mut engine, checkpoint_lsn) = if let Some(snapshot) = storage.load_latest_snapshot()? {
        let lsn = snapshot.header.checkpoint_lsn;
        let restored_engine = Engine::restore_from_snapshot(snapshot);
        (restored_engine, lsn)
    } else {
        (Engine::new(), 0)
    };

    // Replay log entries from WAL that occured after checkpoint_lsn
    let wal = WalManager::open_and_replay(data_dir, |lsn, entry| {
        if lsn > checkpoint_lsn {
            if let Err(e) = engine.apply_wal_entry(entry) {
                eprintln!("Failed to apply WAL entry at LSN {}: {:?}", lsn, e);
            }
        }
    })?;

    // Attach active WalManager (wrapped in Mutex) for live traffic
    engine.wal = Some(Arc::new(Mutex::new(wal)));

    //  Wrap in Arc for concurrent gRPC/HTTP handlers
    let engine_arc = Arc::new(engine);

    // 4. Start the background worker
    start_background_worker(Arc::clone(&engine_arc));

    // 4. Instantiate gRPC Service wrapper
    let pubsub_service = MyPubSubService::new(Arc::clone(&engine_arc));

    // 5. Bind gRPC server to 0.0.0.0:50051
    let addr: SocketAddr = "0.0.0.0:50051".parse()?;
    println!("Pub/Sub gRPC server running on {}", addr);

    Server::builder()
        .add_service(PubSubServiceServer::new(pubsub_service))
        .serve(addr)
        .await?;

    Ok(())
}

