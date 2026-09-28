use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::engine::Engine;
use crate::model::Message;
use crate::errors::PubSubError;
use crate::pubsub::pub_sub_service_server::PubSubService;
use crate::pubsub::*;

impl From<PubSubError> for Status {
    fn from(err: PubSubError) -> Self {
        match err {
            PubSubError::TopicAlreadyExist(name) => {
                Status::already_exists(format!("Topic '{}' already exists", name))
            }
            PubSubError::TopicNotFound(name) => {
                Status::not_found(format!("Topic '{}' not found", name))
            }
            PubSubError::SubscriptionAlreadyExists(name) => {
                Status::already_exists(format!("Subscription '{}' already exists", name))
            }
            PubSubError::SubscriptionNotFound(name) => {
                Status::not_found(format!("Subscription '{}' not found", name))
            }
            PubSubError::IoError(e) => {
                Status::internal(format!("Write-ahead log error: {}", e))
            }
            // Fallback for any other internal errors
            err => Status::internal(err.to_string()),
        }
    }
}

pub struct  MyPubSubService {
    pub engine: Arc<Engine>
}

impl MyPubSubService {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl PubSubService for MyPubSubService {
    type StreamingPullStream = Pin<Box<dyn Stream<Item = Result<StreamingPullResponse, Status>> + Send + 'static>>;

    async fn create_topic(&self, request: Request<CreateTopicRequest>) -> Result<Response<CreateTopicResponse>, Status> {
        let req = request.into_inner();

        if req.topic.is_empty() {
            return Err(Status::invalid_argument("topic name cannot be empty"));
        }

        self.engine
        .create_topic(&req.topic)
        .map_err(Status::from)?;

        Ok(Response::new(CreateTopicResponse { success:true }))
    }

    async fn create_subscription(&self, request: Request<CreateSubscriptionRequest>) -> Result<Response<CreateSubscriptionResponse>, Status> {
        let req = request.into_inner();

        if req.topic.is_empty() || req.subscription.is_empty() {
            return Err(Status::invalid_argument("topic and subscription names are required"));
        }

        if req.ack_deadline_secs == 0 {
            return Err(Status::invalid_argument("ack_deadline_secs must be greater than 0"));
        }

        let ack_deadline = std::time::Duration::from_secs(req.ack_deadline_secs);
        let batch_size: Option<usize> = req.batch_size.map(|b| b as usize);
        self.engine
            .create_subscription(&req.topic, &req.subscription, ack_deadline, batch_size)
            .map_err(Status::from)?;

        Ok(Response::new(CreateSubscriptionResponse { success: true }))
    }

    async fn publish(&self, request: Request<PublishRequest>) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();

        if req.topic.is_empty() {
            return Err(Status::invalid_argument("topic name cannot be empty"));
        }

        let msg = Message::new(req.payload, req.attributes);

        let message_id = self.engine
            .publish(&req.topic, msg)
            .map_err(Status::from)?;

        Ok(Response::new(PublishResponse { message_id }))
    }

    async fn streaming_pull(&self, request: Request<Streaming<StreamingPullRequest>>) -> Result<Response<Self::StreamingPullStream>, Status> {
        let mut req_stream = request.into_inner();

        // First message on stream must contain subscription and topic configuration
        let first_req = match req_stream.next().await {
            Some(Ok(req)) => req,
            Some(Err(e)) => return Err(e),
            None => return Err(Status::invalid_argument("Client closed stream immediately")),
        };

        let topic_name = first_req.topic_name;
        let sub_name = first_req.subscription_name;

        let mut max_request: Option<usize> = match first_req.max_messages {
            Some(m) if m > 0 => Some(m as usize),
            _ => None
        };

        if topic_name.is_empty() || sub_name.is_empty() {
            return Err(Status::invalid_argument("Initial request must specify topic_name and subscription_name"));
        }

        // Get the notify handle from engine for this subscription
        let notify = self.engine.get_subscription_notify(&topic_name, &sub_name).map_err(Status::from)?;

        //Channel for sending message down the gRPC output stream
        let (tx, rx) = mpsc::channel(100);

        let engine = self.engine.clone();

        // Process initials Acks if any were sent in the first request
        for ack_id in first_req.ack_ids {
            let _ = engine.ack(&topic_name, &sub_name, &ack_id);
        }

        // Spawn active worker loop for this streaming connection
        tokio::spawn(async  move {
            // Periodic timer to check visibility timeouts even if publish isn't called
            let mut ticker = tokio::time::interval(Duration::from_millis(200));

            loop {
                // Drain any available messages from the subscription and push to gRPC stream
                let messages = engine.pull_batch(&topic_name, &sub_name, max_request).unwrap();
                if !messages.is_empty() {
                    let responses = StreamingPullResponse {
                        messages: messages.into_iter().map(Into::into).collect(),
                    };

                    if tx.send(Ok(responses)).await.is_err() {
                        // Client disconnected, terminate background loop
                        return;
                    }
                }
                // Wait for either client incoming Acks, engine notifications, or timer ticks
                tokio::select! {
                    // Branch A: Incoming Ack request from subscriber
                    incoming = req_stream.next() => {
                        match incoming {
                            Some(Ok(req)) => {
                                for ack_id in req.ack_ids {
                                    let _ = engine.ack(&topic_name, &sub_name, &ack_id);
                                }

                                if let Some(new_max) = req.max_messages.filter(|&m| m > 0) {
                                    max_request = Some(new_max as usize)
                                }
                            },
                            _ => return, // Stream closed or errored, exist loop
                        }
                    }
                   // Branch B: Awakened by publish() on this subscription
                    _ = notify.notified() => {
                        // Loop will restart and pull available messages
                    }

                    // Branch C: Periodic tick to retry expired visibility timeouts
                    _ = ticker.tick() => {
                        // Loop will restart and pull expired messages
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}