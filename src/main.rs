use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
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
use crate::storage::{run_compaction_pass, SnapshotStorage, CompactorConfig};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = Path::new("./data");
    let storage = Arc::new(SnapshotStorage::new(data_dir));

    // 1. Restore state or initialize empty Engine
    let (mut engine, checkpoint_lsn) = if let Some(snapshot) = storage.load_latest_snapshot()? {
        println!("Restored snapshot at LSN {}", snapshot.header.checkpoint_lsn);
        let lsn = snapshot.header.checkpoint_lsn;
        (Engine::restore_from_snapshot(snapshot), lsn)
    } else {
        (Engine::new(), 0)
    };

    // 2. Replay post-snapshot log records
    let wal = WalManager::open_and_replay(data_dir, |lsn, entry| {
        if lsn > checkpoint_lsn {
            if let Err(e) = engine.apply_wal_entry(entry) {
                eprintln!("Failed to apply WAL entry at LSN {}: {:?}", lsn, e);
            }
        }
    })?;

    // 3. Attach active WAL Manager while engine is still a local mutable variable
    engine.wal = Some(Arc::new(Mutex::new(wal)));

    // 4. Wrap Engine in Arc
    let engine_arc = Arc::new(engine);

    // 5. Setup shutdown channel and start workers via Engine
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let compactor_config = CompactorConfig::default();

    engine_arc.start_workers(Arc::clone(&storage), compactor_config, shutdown_rx);

    // 6. Bind gRPC Service
    let pubsub_service = MyPubSubService::new(Arc::clone(&engine_arc));
    let addr: SocketAddr = "0.0.0.0:50051".parse()?;
    println!("Pub/Sub gRPC server running on {}", addr);

    // 7. Serve gRPC with graceful shutdown listener
    Server::builder()
        .add_service(PubSubServiceServer::new(pubsub_service))
        .serve_with_shutdown(addr, async move {
            tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
            println!("\nShutdown signal received. Stopping background tasks...");

            // Notify background workers
            let _ = shutdown_tx.send(true);

            // Force final snapshot pass (max_bytes_threshold = 0)
            println!("Performing final WAL flush & snapshot...");
            if let Err(e) = run_compaction_pass(&engine_arc, &storage, 0) {
                eprintln!("Error during final shutdown snapshot: {:?}", e);
            } else {
                println!("Final snapshot written successfully.");
            }
        })
        .await?;

    println!("Server shut down cleanly.");
    Ok(())
}