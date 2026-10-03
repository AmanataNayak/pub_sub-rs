use std::fs;
use std::io::Write;
use std::path::PathBuf;
use futures::sink::drain;
use crate::storage::EngineSnapshot;

pub struct SnapshotStorage {
    data_dir: PathBuf
}

impl SnapshotStorage {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into()
        }
    }

    /// Write snapshot to disk
    pub fn write_snapshot(&self, snapshot: &EngineSnapshot) -> std::io::Result<PathBuf> {
        // if directory not exits create one.
        if !self.data_dir.exists() {
            fs::create_dir_all(&self.data_dir)?;
        }

        let lsn = snapshot.header.checkpoint_lsn;
        let tmp_path = self.data_dir.join(format!("snapshot-{}.tmp", lsn));
        let snapshot_path = self.data_dir.join(format!("snapshot-{}.snap", lsn));

        // Encode to bytes
        let encoded = postcard::to_allocvec(snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;


        // Open with WRITE access using File::create
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(&encoded)?;

        file.sync_all()?;

        drop(file);

        // Atomic rename
        fs::rename(&tmp_path, &snapshot_path)?;

        Ok(snapshot_path)

    }

    /// Finds, validates & loads the newest snapshot file
    pub fn load_latest_snapshot(&self) -> std::io::Result<Option<EngineSnapshot>> {
        if !self.data_dir.exists() {
            return Ok(None);
        }
        let mut snapshots: Vec<(u64, PathBuf)> = Vec::new();

        // Scan data dir for files matching `snapshot-<LSN>.snap`
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |ext| ext == "snap") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(lsn_str) = stem.strip_prefix("snapshot-") {
                        if let Ok(lsn) = lsn_str.parse::<u64>() {
                            snapshots.push((lsn, path));
                        }
                    }
                }
            }
        }

        // Sort ascending by LSN to get the latest checkpoint
        snapshots.sort_by_key(|(lsn, _)| *lsn);

        if let Some((_, latest_path)) = snapshots.pop() {
            let bytes = fs::read(&latest_path)?;

            let snapshot: EngineSnapshot = postcard::from_bytes(&bytes)
                .map_err(|e| std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse snapshot: {}", e),
                ))?;

            // Validate header magic bytes
            if snapshot.header.magic != *b"PSSN" {
                return Err(std::io::Error::new(
                   std::io::ErrorKind::InvalidData,
                   "Snapshot header magic bytes mismatch",
                ));
            }

            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    /// Purges obsolete WAL segment files where max_lsn <= checkpoint_lsn
    pub fn purge_obsolete_wal_segments(&self, checkpoint_lsn: u64) -> std::io::Result<usize> {
        if !self.data_dir.exists() {
            return Ok(0);
        }

        let mut purged_count = 0;

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |ext| ext == "log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(lsn_str) = stem.strip_prefix("wal-") {
                        if let Ok(segment_max_lsn) = lsn_str.parse::<u64>() {
                            if segment_max_lsn <= checkpoint_lsn {
                                fs::remove_file(&path)?;
                                purged_count += 1;
                            }
                        }
                    }
                }
            }
        }
        Ok(purged_count)
    }
}
#[cfg(test)]
mod snapshot_storage_tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;
    use crate::storage::{
        EngineSnapshot,
        PersistentMessage,
        SnapshotHeader,
        SubscriptionState,
        TopicState
    };

    /// Helper that constructs a valid EngineSnapshot matching your exact DTO layout
    fn mock_snapshot(checkpoint_lsn: u64) -> EngineSnapshot {
        let mut topics = HashMap::new();
        let mut messages = HashMap::new();

        // Sample message payload
        let msg_id = "msg-001".to_string();
        messages.insert(
            msg_id.clone(),
            PersistentMessage {
                id: msg_id.clone(),
                payload: b"hello-world".to_vec(),
                attributes: HashMap::from([("env".to_string(), "test".to_string())]),
                delivery_attempt: 1,
                created_at_ms: 1700000000000,
            },
        );

        // Sample topic & subscription state
        let mut subscriptions = HashMap::new();
        subscriptions.insert(
            "orders-sub".to_string(),
            SubscriptionState {
                name: "orders-sub".to_string(),
                ack_deadline_secs: 10,
                push_config: None,
                batch_size: 10,
                max_outstanding_messages: Some(100),
                message_ttl_secs: Some(3600),
                dropped_messages: 0,
                dead_letter_policy: None,
                ready_queue: VecDeque::from([msg_id.clone()]),
                in_flight: HashMap::new(),
            },
        );

        topics.insert(
            "orders".to_string(),
            TopicState {
                name: "orders".to_string(),
                subscriptions,
            },
        );

        EngineSnapshot {
            header: SnapshotHeader {
                magic: *b"PSSN",
                version: 1,
                checkpoint_lsn,
                timestamp_ms: 1700000000000,
            },
            topics,
            messages,
        }
    }

    #[test]
    fn test_atomic_write_and_load_latest() {
        let dir = tempdir().unwrap();
        let storage = SnapshotStorage::new(dir.path());

        let snap10 = mock_snapshot(10);
        let written_path = storage.write_snapshot(&snap10).expect("Write failed");

        // 1. Verify path and tmp cleanup
        assert!(written_path.exists());
        assert_eq!(written_path.file_name().unwrap(), "snapshot-10.snap");
        assert!(!dir.path().join("snapshot-10.tmp").exists());

        // 2. Verify deserialization back into EngineSnapshot DTO
        let loaded = storage.load_latest_snapshot().expect("Load failed");
        assert!(loaded.is_some());

        let loaded_snap = loaded.unwrap();
        assert_eq!(loaded_snap.header.checkpoint_lsn, 10);
        assert_eq!(loaded_snap.header.version, 1);
        assert_eq!(loaded_snap.header.magic, *b"PSSN");

        // 3. Verify nested topics and messages state restored intact
        assert!(loaded_snap.topics.contains_key("orders"));
        assert!(loaded_snap.messages.contains_key("msg-001"));
        let msg = &loaded_snap.messages["msg-001"];
        assert_eq!(msg.payload, b"hello-world");
    }

    #[test]
    fn test_ignores_uncommitted_tmp_files() {
        let dir = tempdir().unwrap();
        let storage = SnapshotStorage::new(dir.path());

        // Save valid snapshot at LSN 10
        storage.write_snapshot(&mock_snapshot(10)).unwrap();

        // Simulate crash mid-write for LSN 20 (leaving partial .tmp file)
        let tmp_path = dir.path().join("snapshot-20.tmp");
        let mut f = File::create(&tmp_path).unwrap();
        f.write_all(b"corrupted uncommitted bytes").unwrap();

        // Loader must ignore .tmp and fall back to snapshot-10
        let loaded = storage
            .load_latest_snapshot()
            .unwrap()
            .expect("Should find snap-10");
        assert_eq!(loaded.header.checkpoint_lsn, 10);
    }

    #[test]
    fn test_picks_highest_lsn() {
        let dir = tempdir().unwrap();
        let storage = SnapshotStorage::new(dir.path());

        storage.write_snapshot(&mock_snapshot(100)).unwrap();
        storage.write_snapshot(&mock_snapshot(250)).unwrap();
        storage.write_snapshot(&mock_snapshot(150)).unwrap();

        let loaded = storage
            .load_latest_snapshot()
            .unwrap()
            .expect("Should find highest snap");
        assert_eq!(loaded.header.checkpoint_lsn, 250);
    }

    #[test]
    fn test_purge_obsolete_wal_segments() {
        let dir = tempdir().unwrap();
        let storage = SnapshotStorage::new(dir.path());

        File::create(dir.path().join("wal-000000000005.log")).unwrap();
        File::create(dir.path().join("wal-000000000010.log")).unwrap();
        File::create(dir.path().join("wal-000000000020.log")).unwrap();

        let purged = storage.purge_obsolete_wal_segments(10).unwrap();

        assert_eq!(purged, 2);
        assert!(!dir.path().join("wal-000000000005.log").exists());
        assert!(!dir.path().join("wal-000000000010.log").exists());
        assert!(dir.path().join("wal-000000000020.log").exists());
    }
}