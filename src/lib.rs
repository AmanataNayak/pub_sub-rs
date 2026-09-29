// 1. Generated Protobuf module
pub mod pubsub {
    tonic::include_proto!("pubsub");
}

// 2. Internal domain modules
pub mod errors;
pub mod model;
pub mod workers;
pub mod server;
pub mod storage;
pub mod engine;