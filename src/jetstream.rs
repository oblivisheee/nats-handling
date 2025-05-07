pub mod error;
pub mod handle;

use crate::NatsClient;
pub use async_nats::jetstream::Message as JSMessage;
use async_nats::jetstream::{self, Context};
use async_trait::async_trait;
pub use handle::Handle;

pub mod config {
    pub use consumer::{PullConsumerConfig, PushConsumerConfig};
    pub use stream::StreamConfig;
    pub mod consumer {
        use async_nats::jetstream::consumer;
        pub use consumer::pull::Config as PullConsumerConfig;
        pub use consumer::push::Config as PushConsumerConfig;
        pub use consumer::{AckPolicy, PriorityPolicy, ReplayPolicy};
    }
    pub mod stream {
        use async_nats::jetstream::stream;
        pub use stream::{
            Compression, Config as StreamConfig, ConsumerLimits, DiscardPolicy, Placement,
            Republish, RetentionPolicy, Source, SubjectTransform,
        };
    }
}

pub use error::JetStreamError;
use futures::Stream;

use serde::{Deserialize, Serialize};
use std::pin::Pin;
use tracing::{instrument, trace};

#[derive(Clone)]
pub struct JetStream {
    pub context: Context, // JetStream context for interacting with NATS JetStream
}

impl JetStream {
    /// Creates a new JetStream instance
    pub fn new(client: NatsClient) -> Self {
        trace!("Creating a new JetStream instance");
        let context = jetstream::new(client.client);

        JetStream { context }
    }

    /// Handles a delivery using the provided stream configuration and processor
    pub async fn handle<R: MessageProcessor + Send + 'static + Sync>(
        &self,
        delivery: Delivery,
        stream_config: config::StreamConfig,
        processor: R,
    ) -> Result<handle::Handle, JetStreamError> {
        trace!(
            "Handling delivery with stream configuration: {:?}",
            stream_config
        );
        let stream = self.context.get_or_create_stream(stream_config).await?;
        match delivery {
            Delivery::Pull((config, fetcher)) => {
                trace!("Handling Pull delivery with config: {:?}", config);
                let consumer = stream.create_consumer(config).await?;
                handle::Handle::pull(self.clone(), consumer, fetcher, processor).await
            }
            Delivery::Push(config) => {
                trace!("Handling Push delivery with config: {:?}", config);
                let consumer = stream.create_consumer(config).await?;
                handle::Handle::push(self.clone(), consumer, processor).await
            }
        }
    }

    /// Sends a reply message
    #[instrument(skip_all)]
    pub async fn reply(&self, reply: ReplyMessage) -> Result<(), JetStreamError> {
        trace!("Sending reply to: {}", reply.subject);
        trace!("Reply payload size: {}", reply.payload.len());

        match reply.headers {
            Some(headers) => {
                trace!("Reply has headers: {:?}", headers);
                self.context
                    .publish_with_headers(reply.subject.clone(), headers, reply.payload)
                    .await?;
            }
            None => {
                trace!("Reply has no headers");
                self.context
                    .publish(reply.subject.clone(), reply.payload)
                    .await?;
            }
        }

        trace!("Successfully sent reply to {}", reply.subject);
        Ok(())
    }

    /// Sends an error reply message
    #[instrument(skip_all)]
    pub(crate) async fn reply_err(
        &self,
        err: crate::ReplyErrorMessage,
        msg_source: Message,
    ) -> Result<(), JetStreamError> {
        trace!("Creating error reply message for source: {:?}", msg_source);
        let reply = ReplyMessage {
            subject: msg_source
                .reply
                .clone()
                .unwrap_or_else(|| {
                    trace!("No reply subject found in the source message");
                    "".to_string().into()
                })
                .to_string(),
            payload: err.0.to_string().into(),
            headers: None,
        };
        self.reply(reply).await
    }
}

impl std::ops::Deref for JetStream {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl std::ops::DerefMut for JetStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.context
    }
}

pub enum Delivery {
    Pull((config::PullConsumerConfig, Box<dyn PullFetcher>)), // Pull-based delivery
    Push(config::PushConsumerConfig),                         // Push-based delivery
}

pub trait PullFetcher {
    /// Creates a stream for fetching messages
    fn create_stream(&self) -> Pin<Box<dyn Stream<Item = usize> + Send>>;
}

#[async_trait]
pub trait MessageProcessor {
    type Error: std::error::Error + Send + Sync + 'static;
    /// Processes a message and optionally returns a reply
    async fn process(&self, msg: Message) -> Result<Option<ReplyMessage>, Self::Error>;
}

/// A structure that represents a reply message
#[derive(Clone, Debug)]
pub struct ReplyMessage {
    pub subject: String,                        // Subject to reply to
    pub payload: bytes::Bytes,                  // Payload of the reply
    pub headers: Option<async_nats::HeaderMap>, // Optional headers
}
impl ReplyMessage {
    /// Creates a new ReplyMessage
    pub fn new(subject: String, payload: bytes::Bytes) -> Self {
        trace!("Creating a new ReplyMessage for subject: {}", subject);
        Self {
            subject,
            payload,
            headers: None,
        }
    }
}

/// An easy-to-use function that creates a reply message
pub fn reply(msg: &Message, payload: bytes::Bytes) -> ReplyMessage {
    trace!("Creating a reply message for source message: {:?}", msg);
    ReplyMessage {
        subject: msg.reply.clone().unwrap_or_else(|| "".into()).to_string(),
        payload,
        headers: None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message(async_nats::Message);

impl std::ops::Deref for Message {
    type Target = async_nats::Message;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Message {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Message {
    /// Creates a reply message for the current message
    pub fn reply(&self, payload: bytes::Bytes) -> ReplyMessage {
        trace!("Creating a reply message for Message");
        ReplyMessage {
            subject: self.reply.clone().unwrap_or_else(|| "".into()).to_string(),
            payload,
            headers: None,
        }
    }
}

impl Into<Message> for async_nats::Message {
    fn into(self) -> Message {
        trace!("Converting async_nats::Message into Message");
        Message(self)
    }
}
impl Into<Message> for async_nats::jetstream::Message {
    fn into(self) -> Message {
        trace!("Converting async_nats::jetstream::Message into Message");
        Message(self.into())
    }
}
