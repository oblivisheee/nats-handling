pub mod error;
pub mod handle;

use crate::NatsClient;
pub use async_nats::jetstream::Message as JSMessage;
use async_nats::jetstream::{self, Context};
use async_trait::async_trait;
pub use handle::Handle;
use std::sync::Arc;

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
    #[instrument(skip_all)]
    pub async fn handle<R: MessageProcessor + Send + 'static + Sync>(
        &self,
        delivery: Delivery,
        stream_config: config::StreamConfig,
        processor: R,
        handle_config: handle::HandleConfig,
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
                handle::Handle::pull(consumer, fetcher, Arc::new(processor), handle_config).await
            }
            Delivery::Push(config) => {
                trace!("Handling Push delivery with config: {:?}", config);
                let consumer = stream.create_consumer(config).await?;
                handle::Handle::push(consumer, Arc::new(processor), handle_config).await
            }
        }
    }
}

impl std::ops::Deref for JetStream {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

pub enum Delivery {
    /// Pull-based delivery. First element is the consumer config, second is the fetcher.
    Pull((config::PullConsumerConfig, Box<dyn PullFetcher>)),
    /// Push-based delivery. First element is the consumer config.
    Push(config::PushConsumerConfig), // Push-based delivery
}

pub trait PullFetcher {
    /// Creates a stream for fetching messages
    fn create_stream(&self) -> Pin<Box<dyn Stream<Item = usize> + Send>>;
}

#[async_trait]
pub trait MessageProcessor {
    type Error: std::error::Error + Send + Sync + 'static;
    /// Processes a message and optionally returns a reply
    async fn process(&self, msg: Message) -> Result<(), Self::Error>;
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

impl From<async_nats::jetstream::Message> for Message {
    fn from(msg: async_nats::jetstream::Message) -> Self {
        Message(msg.into())
    }
}
impl From<async_nats::Message> for Message {
    fn from(msg: async_nats::Message) -> Self {
        Message(msg)
    }
}
