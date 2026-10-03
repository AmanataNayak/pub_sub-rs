use crate::model::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
pub enum WalEntry {
    CreateTopic {
        topic: String,
    },
    CreateSubscription {
        topic: String,
        subscription: String,
        ack_deadline_sec: u64,
        #[serde(default)]
        batch_size: usize,
        #[serde(default)]
        max_outstanding_messages: Option<usize>,
        #[serde(default)]
        dead_letter_queue: Option<String>,
        #[serde(default)]
        max_delivery_attempts: Option<u32>,
        #[serde(default)]
        message_ttl: Option<Duration>,
        push_endpoint: Option<String>,
        headers: Option<HashMap<String, String>>,
        timeout_secs: Option<u64>,
    },
    Publish {
        topic: String,
        message: Message,
    },
    Ack {
        topic: String,
        subscription: String,
        message_ids: Vec<String>,
    },
    Nack {
        topic: String,
        subscription: String,
        message_ids: Vec<String>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WalRecord {
    pub lsn: u64,
    pub entry: WalEntry,
}

pub struct WalManager {
    data_dir: PathBuf,
    active_file: File,
    pub current_lsn: u64,
    pub active_file_bytes: u64,
    pub is_dirty: bool,
}

impl WalManager {
    /// Opens the target data directory, discovers and replays log segments in order,
    /// tracks the highest LSN, and initializes an active log segment file.
    pub fn open_and_replay<F>(
        data_dir: impl AsRef<Path>,
        mut apply_entry: F,
    ) -> std::io::Result<Self>
    where
        F: FnMut(u64, WalEntry),
    {
        let dir_path = data_dir.as_ref().to_path_buf();
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }

        let mut max_lsn = 0u64;
        let mut segments = Vec::new();

        // 1. Scan directory for wal-<LSN>.log segment files
        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|ext| ext.to_str()) == Some("log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(lsn_str) = stem.strip_prefix("wal-") {
                        if let Ok(lsn) = lsn_str.parse::<u64>() {
                            segments.push((lsn, path));
                        }
                    }
                }
            }
        }

        // Sort segments in ascending order by initial LSN
        segments.sort_by_key(|(lsn, _)| *lsn);

        // 2. Replay all records sequentially
        for (_, segment_path) in &segments {
            let file = File::open(segment_path)?;
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line_str = line?;
                if line_str.trim().is_empty() {
                    continue;
                }

                if let Ok(record) = serde_json::from_str::<WalRecord>(&line_str) {
                    if record.lsn > max_lsn {
                        max_lsn = record.lsn;
                    }
                    apply_entry(record.lsn, record.entry);
                }
            }
        }

        // 3. Open or create active segment file
        let active_segment_lsn = if max_lsn == 0 { 1 } else { max_lsn + 1 };
        let active_path = dir_path.join(format!("wal-{:012}.log", active_segment_lsn));

        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;

        let active_file_bytes = active_file.metadata()?.len();

        Ok(Self {
            data_dir: dir_path,
            active_file,
            current_lsn: max_lsn,
            active_file_bytes,
            is_dirty: false,
        })
    }

    /// Appends a WalEntry to the active segment file wrapped in a WalRecord with an auto-incremented LSN.
    pub fn append(&mut self, entry: &WalEntry) -> std::io::Result<u64> {
        self.current_lsn += 1;
        let record = WalRecord {
            lsn: self.current_lsn,
            entry: entry.clone(),
        };

        let json_str = serde_json::to_string(&record)?;
        let line = format!("{}\n", json_str);
        let bytes = line.as_bytes();

        self.active_file.write_all(bytes)?;
        self.active_file.flush()?;

        self.active_file_bytes += bytes.len() as u64;
        self.is_dirty = true;

        Ok(self.current_lsn)
    }

    /// Flushes and syncs the current active log file, then creates a new active log segment file.
    pub fn rotate_segment(&mut self) -> std::io::Result<()> {
        self.active_file.flush()?;
        self.active_file.sync_all()?;

        let next_segment_lsn = self.current_lsn + 1;
        let next_path = self.data_dir.join(format!("wal-{:012}.log", next_segment_lsn));

        self.active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_path)?;

        self.active_file_bytes = 0;
        Ok(())
    }
}

// Unit test
#[cfg(test)]
mod wal_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_lsn_assignment_and_replay() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        let entry1 = WalEntry::CreateTopic {
            topic: "orders".to_string(),
        };
        let entry2 = WalEntry::CreateTopic {
            topic: "payments".to_string(),
        };

        // 1. Open manager and append entries
        {
            let mut wal = WalManager::open_and_replay(data_dir, |_, _| {}).unwrap();
            let lsn1 = wal.append(&entry1).unwrap();
            let lsn2 = wal.append(&entry2).unwrap();

            assert_eq!(lsn1, 1);
            assert_eq!(lsn2, 2);
            assert_eq!(wal.current_lsn, 2);
        }

        // 2. Re-open (simulating server reboot) and collect recovered records
        let mut recovered = Vec::new();
        let wal_reboot = WalManager::open_and_replay(data_dir, |lsn, entry| {
            recovered.push((lsn, entry));
        })
            .unwrap();

        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].0, 1);
        assert_eq!(recovered[0].1, entry1);
        assert_eq!(recovered[1].0, 2);
        assert_eq!(recovered[1].1, entry2);
        assert_eq!(wal_reboot.current_lsn, 2);
    }

    #[test]
    fn test_wal_segment_rotation() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        let mut wal = WalManager::open_and_replay(data_dir, |_, _| {}).unwrap();

        wal.append(&WalEntry::CreateTopic {
            topic: "topic-1".to_string(),
        })
            .unwrap(); // LSN 1

        wal.append(&WalEntry::CreateTopic {
            topic: "topic-2".to_string(),
        })
            .unwrap(); // LSN 2

        // Rotate segment -> seals active file and starts new log file at next LSN
        wal.rotate_segment().unwrap();

        wal.append(&WalEntry::CreateTopic {
            topic: "topic-3".to_string(),
        })
            .unwrap(); // LSN 3

        // Verify two distinct segment files exist in directory
        let log_files: Vec<_> = std::fs::read_dir(data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("log"))
            .collect();

        assert_eq!(log_files.len(), 2);
    }
}