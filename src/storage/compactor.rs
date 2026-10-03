use crate::engine::Engine;
use crate::storage::snapshot_storage::SnapshotStorage;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs::read_to_string;
use tokio::time::{self, MissedTickBehavior};
use tonic::Status;
use crate::storage::WalManager;

#[derive(Clone, Copy, Debug)]
pub struct CompactorConfig {
    pub check_interval: Duration,
    pub max_bytes_threshold: u64
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(300), // 5 min
            max_bytes_threshold: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// Executes a single compaction pass if WAL size or dirty flag demand it.
/// Return OK(true) if a snapshot was captured and written to disk
pub fn run_compaction_pass(engine: &Engine, storage: &SnapshotStorage, max_bytes_threshold: u64) -> std::io::Result<bool>{
    let wal_arc = match &engine.wal {
        None => return Ok(false),
        Some(wal) => Arc::clone(wal)
    };

    // 1. Check trigger condition under brief WAL lock
    let (should_compact, checkpoint_lsn) = {
        let mut wal = wal_arc.lock().unwrap();
        if wal.is_dirty && wal.active_file_bytes >= max_bytes_threshold {
            wal.rotate_segment()?;
            let lsn = wal.current_lsn;
            wal.is_dirty = false;
            (true, lsn)
        } else {
            (false, 0)
        }
    };

    if !should_compact {
        return Ok(false);
    }

    // 2. Extract snapshot DTO using your exact create_snapshot method
    let snapshot = engine.create_snapshot(checkpoint_lsn);

    // 3. Atomically write snapshot to disk
    storage.write_snapshot(&snapshot)?;

    // 4. Purge log segments fully covered by this snapshot checkpoint
    storage.purge_obsolete_wal_segments(checkpoint_lsn)?;

    Ok(true)
}

/// Spawn the background Tokio worker task for automated snapshot compaction
pub fn start_compactor_worker(engine: Arc<Engine>, storage: Arc<SnapshotStorage>, config: CompactorConfig) {
    tokio::spawn(async move {
        let mut interval = time::interval(config.check_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let engine_ref = Arc::clone(&engine);
            let storage_ref = Arc::clone(&storage);

            // Offload CPU serialization and blocking disk I/O off Tokio threads
            let _ = tokio::task::spawn_blocking(move || {
                if let Err(e) = run_compaction_pass(&engine_ref, &storage_ref, config.max_bytes_threshold) {
                    eprintln!("[Compactor Error] Snapshot compaction failed: {:?}", e);
                }
            })
                .await;
        }
    });
}


#[cfg(test)]
mod compactor_tests {
    use super::*;
    use tempfile::tempdir;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use crate::storage::{WalEntry, WalManager};

    #[tokio::test]
    async fn test_background_compactor_triggers_and_purges() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        let storage = Arc::new(SnapshotStorage::new(data_dir));
        let mut wal = WalManager::open_and_replay(data_dir, |_, _| {}).unwrap();

        // Populate entries so active_file_bytes grows
        wal.append(&WalEntry::CreateTopic { topic: "orders".to_string() }).unwrap();
        wal.append(&WalEntry::CreateTopic { topic: "payments".to_string() }).unwrap();

        let mut engine = Engine::new();
        engine.create_topic("orders").unwrap();
        engine.create_topic("payments").unwrap();
        engine.wal = Some(Arc::new(Mutex::new(wal)));

        // Configure aggressive triggers for test: 100-byte threshold, 10ms interval
        let config = CompactorConfig {
            check_interval: Duration::from_millis(10),
            max_bytes_threshold: 50,
        };

        // Run one explicit compactor pass
        let snapshot_created = run_compaction_pass(&engine, &storage, config.max_bytes_threshold).unwrap();

        assert!(snapshot_created, "Compaction should have been triggered by byte threshold");

        // Verify snapshot file exists on disk
        let loaded = storage.load_latest_snapshot().unwrap();
        assert!(loaded.is_some());
        let snap = loaded.unwrap();
        assert_eq!(snap.header.checkpoint_lsn, 2);
        assert!(snap.topics.contains_key("orders"));
        assert!(snap.topics.contains_key("payments"));
    }
}