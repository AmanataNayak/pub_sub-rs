use std::io::{BufRead, Write};
use std::path::Path;
use std::fs::{File, OpenOptions};
use std::sync::Mutex;
use crate::model::{Message};
use serde_json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum WalEntry {
    CreateTopic {
        topic: String
    },
    CreateSubscription {
        topic: String,
        subscription: String,
        ack_deadline_sec: u64,
        #[serde(default)]
        batch_size: Option<usize>,
        #[serde(default)]
        max_outstanding_messages: Option<usize>,
        #[serde(default)]
        dead_letter_queue: Option<String>,
        #[serde(default)]
        max_delivery_attempts: Option<u32>
    },
    Publish {
        topic: String,
        message: Message
    },
    Ack {
        topic: String,
        subscription: String,
        message_id: String
    }
}

pub struct WalManager {
    file: Mutex<File>
}

impl WalManager {
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        Ok(
            Self{
                file: Mutex::new(file)
            }
        )
    }

    pub fn append(&self, entry: &WalEntry) -> std::io::Result<()> {
        // Serialize the entry to a JSON line
        let json_str = serde_json::to_string(&entry)?;

        // Lock the file
        let mut file = self.file.lock().unwrap();

        // Write the json string followed by a new line \n
        writeln!(file, "{}", json_str)?;

        // flush buffer to physical disk immediately
        file.flush()?;

        Ok(())
    }

    /// This function runs when the server boots up to rebuild the state from pubsub.wal
    pub fn recover<P: AsRef<Path>>(path: P) -> std::io::Result<Vec<WalEntry>> {
        if !path.as_ref().exists() {
            return Ok(Vec::new())
        }

        let file = File::open(path)?;
        let mut vec_wal_entry: Vec<WalEntry> = Vec::new();

        for line_result in std::io::BufReader::new(file).lines() {
            let line = line_result?;
            // Skip empty line if any exist
            if line.trim().is_empty() {
                continue;
            }
            // Deserialize the json string
            let json_str = serde_json::from_str::<WalEntry>(&line)?;
            vec_wal_entry.push(json_str);        }

        Ok(vec_wal_entry)
    }
}

// Unit test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_append_recover() {
        let test_wal_path = "test_run.wal";

        // Clean up any old test file
        match std::fs::remove_file(test_wal_path){
            Ok(()) => println!("Deleted successfully"),
            Err(_) => println!("Not found")
        }


        let wal = WalManager::open(test_wal_path).unwrap();

        let entry1 = WalEntry::CreateTopic {
            topic: "order".to_string()
        };

        let entry2 = WalEntry::CreateSubscription {
            topic: "orders".to_string(),
            subscription: "inv-sub".to_string(),
            ack_deadline_sec: 10,
            batch_size: Some(1),
            max_outstanding_messages: None,
            dead_letter_queue: None,
            max_delivery_attempts: None
        };

        // Append entries
        wal.append(&entry1).unwrap();
        wal.append(&entry2).unwrap();

        // Recover entries from disk
        let recovered = WalManager::recover(test_wal_path).unwrap();

        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0], entry1);
        assert_eq!(recovered[1], entry2);
    }
}