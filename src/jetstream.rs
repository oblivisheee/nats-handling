mod error;
mod handle;

use crate::NatsClient;
use async_nats::jetstream::{self, consumer, Context, Message};
use async_trait::async_trait;
pub use error::JetStreamError;
use futures::Stream;
pub use jetstream::stream::Config as StreamConfig;
use std::pin::Pin;
use tracing::{debug, instrument, trace};

#[derive(Clone)]
pub struct JetStream {
    pub context: Context,
}

impl JetStream {
    pub fn new(client: NatsClient) -> Self {
        let context = jetstream::new(client.client);

        JetStream { context }
    }
    pub async fn handle<R: MessageProcessor + Send + 'static>(
        &self,
        delivery: Delivery,
        stream_config: StreamConfig,
        processor: R,
    ) -> Result<handle::Handle, JetStreamError> {
        let stream = self.context.get_or_create_stream(stream_config).await?;
        match delivery {
            Delivery::Pull((config, fetcher)) => {
                let consumer = stream.create_consumer(config).await?;
                handle::Handle::pull(self.clone(), consumer, fetcher, processor).await
            }
            Delivery::Push(config) => {
                let consumer = stream.create_consumer(config).await?;
                handle::Handle::push(self.clone(), consumer, processor).await
            }
        }
    }
    #[instrument(skip_all)]
    pub async fn reply(&self, reply: ReplyMessage) -> Result<(), JetStreamError> {
        debug!("Sending reply to: {}", reply.subject);
        trace!("Reply payload size: {}", reply.payload.len());

        match reply.headers {
            Some(headers) => {
                self.context
                    .publish_with_headers(reply.subject.clone(), headers, reply.payload)
                    .await?;
            }
            None => {
                self.context
                    .publish(reply.subject.clone(), reply.payload)
                    .await?;
            }
        }

        debug!("Successfully sent reply to {}", reply.subject);
        Ok(())
    }
    #[instrument(skip_all)]
    pub(crate) async fn reply_err(
        &self,
        err: crate::ReplyErrorMessage,
        msg_source: Message,
    ) -> Result<(), JetStreamError> {
        trace!("Creating error reply message");
        let reply = ReplyMessage {
            subject: msg_source
                .reply
                .clone()
                .unwrap_or_else(|| {
                    eprint!("No reply subject");
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
    Pull((consumer::pull::Config, Box<dyn PullFetcher>)),
    Push(consumer::push::Config),
}

pub trait PullFetcher {
    fn create_stream(&self) -> Pin<Box<dyn Stream<Item = usize> + Send>>;
}

#[async_trait]
pub trait MessageProcessor {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn process(&self, msg: jetstream::Message) -> Result<Option<ReplyMessage>, Self::Error>;
}

/// A structure that represents a reply message
#[derive(Clone, Debug)]
pub struct ReplyMessage {
    pub subject: String,
    pub payload: bytes::Bytes,
    pub headers: Option<async_nats::HeaderMap>,
}
impl ReplyMessage {
    pub fn new(subject: String, payload: bytes::Bytes) -> Self {
        Self {
            subject,
            payload,
            headers: None,
        }
    }
}
