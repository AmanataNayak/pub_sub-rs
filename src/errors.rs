use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq)]
pub enum PubSubError {
    TopicAlreadyExist(String),
    TopicNotFound(String),
    SubscriptionAlreadyExists(String),
    SubscriptionNotFound(String),
    MessageNotFound(String),
    IoError(String)
}

impl fmt::Display for PubSubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TopicAlreadyExist(t) => write!(f, "Topic '{t}' already exists"),
            Self::TopicNotFound(t) => write!(f, "Topic '{t}' not found"),
            Self::SubscriptionAlreadyExists(s) => write!(f, "Subscription '{s}' already exists"),
            Self::SubscriptionNotFound(s) => write!(f, "Subscription '{s}' not found"),
            Self::MessageNotFound(m) => write!(f, "Message '{m}' not found or already acknowledged"),
            Self::IoError(m) => write!(f, "IO error during write to WAL '{m}' "),

        }
    }
}