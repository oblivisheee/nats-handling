//! Nats Handling is a library designed for seamless NATS message handling in Rust. It offers a straightforward API for subscribing to NATS subjects, processing messages, and sending replies.
//! The goal of this library is to provide an experience similar to HTTP handling, but tailored for NATS.
pub mod error;

pub use async_nats::Message;
use async_nats::{Client, ConnectOptions, Subscriber};
use async_trait::async_trait;
use bytes::Bytes;
pub use error::Error;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace};

/// A structure that handles specified NATS subject and responds to messages
#[derive(Debug)]
pub struct Handle {
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

/// A structure that represents a connection to a NATS server
#[derive(Clone, Debug)]
pub struct NatsClient {
    client: Client,
}

impl NatsClient {
    /// Creates a new NATS client and connects to the specified server
    #[instrument(skip_all)]
    pub async fn new(bind: &[&str]) -> Result<Self, Error> {
        info!("Connecting to NATS server at {:?}", bind);
        trace!("Creating ConnectOptions");
        let client = ConnectOptions::new().connect(bind).await?;
        info!("Successfully connected to NATS server");
        Ok(Self { client })
    }
    /// Subscribes to a specified NATS subject
    #[instrument(skip_all)]
    pub async fn subscribe(&self, subject: impl AsRef<str>) -> Result<Subscriber, Error> {
        let subject = subject.as_ref().to_owned();
        info!("Subscribing to subject: {}", subject);
        trace!("Calling client.subscribe with subject: {}", subject);
        let subscription = self.client.subscribe(subject.clone()).await?;
        debug!("Successfully subscribed to {}", subject);
        Ok(subscription)
    }
    /// Publishes a message to a specified NATS subject
    #[instrument(skip_all)]
    pub async fn publish(&self, subject: impl AsRef<str>, payload: Bytes) -> Result<(), Error> {
        let subject = subject.as_ref().to_owned();
        debug!("Publishing message to subject: {}", subject);
        trace!("Payload size: {}", payload.len());
        self.client.publish(subject.clone(), payload).await?;
        debug!("Successfully published to {}", subject);
        Ok(())
    }
    /// Sends a request to a specified NATS subject and returns the response
    #[instrument(skip_all)]
    pub async fn request(
        &self,
        subject: impl AsRef<str>,
        payload: Bytes,
    ) -> Result<Message, Error> {
        let subject = subject.as_ref().to_owned();
        debug!("Sending request to subject: {}", subject);
        trace!("Payload size: {}", payload.len());
        let response = self.client.request(subject.clone(), payload).await?;
        debug!("Received response from {}", subject);
        trace!("Response payload size: {}", response.payload.len());
        Ok(response)
    }
    /// Handles a specified NATS subject and processes messages using the provided processor
    #[instrument(skip_all)]
    pub async fn handle<R: RequestProcessor + 'static>(
        &self,
        subject: impl AsRef<str>,
        processor: R,
    ) -> Result<Handle, Error> {
        let subject = subject.as_ref().to_owned();
        info!("Setting up handler for subject: {}", subject);
        let subject = subject.to_string();
        let mut subscriber = self.subscribe(subject.clone()).await?;

        let moved_processor = processor.clone();
        let moved_subject = subject.clone();
        let client_clone = self.clone();
        let cancel_token = CancellationToken::new();
        let cancel_token_child = cancel_token.clone();

        let handle = tokio::spawn(async move {
            info!("Started message processing loop for {}", moved_subject);
            let stop_signal = cancel_token_child.cancelled();
            tokio::select! {
            _ = async {
                while let Some(message) = subscriber.next().await {
                debug!("Processing message from subject: {}", message.subject);
                trace!("Message payload size: {}", message.payload.len());
                match moved_processor.process(message.clone()).await {
                    Ok(reply) => {
                    debug!("Successfully processed message");
                    if let Err(e) = client_clone.reply(reply).await {
                        error!("Failed to reply to message: {}", e);
                    }
                    }
                    Err(e) => {
                    error!("Failed to process message: {}", e);
                    if let Err(e) = client_clone
                        .reply_err(ReplyErrorMessage(e), message.clone())
                        .await
                    {
                        error!("Failed to reply to message: {}", e);
                    }
                    }
                }
                }
            } => {},
            _ = stop_signal => {
                if let Err(e) = subscriber.unsubscribe().await {
                error!("Failed to unsubscribe from {}: {}", moved_subject, e);
                } else {
                info!("Successfully unsubscribed from {}", moved_subject);
                }
            }
            }
        });

        Ok(Handle {
            cancel: cancel_token,
            handle,
        })
    }
    /// Sends a reply to a message
    #[instrument(skip_all)]
    pub async fn reply(&self, reply: ReplyMessage) -> Result<(), Error> {
        debug!("Sending reply to:  {}", reply.reply);
        trace!("Reply payload size: {}", reply.payload.len());
        self.client
            .publish(reply.reply.clone(), reply.payload.clone())
            .await?;
        debug!("Successfully sent reply to {}", reply.reply);
        Ok(())
    }
    /// Sends an error reply to a message
    async fn reply_err(&self, err: ReplyErrorMessage, msg_source: Message) -> Result<(), Error> {
        trace!("Creating error reply message");
        let reply = ReplyMessage {
            reply: msg_source
                .reply
                .clone()
                .unwrap_or_else(|| {
                    eprint!("No reply subject");
                    "".to_string().into()
                })
                .to_string(),
            payload: err.0.to_string().into(),
        };
        self.reply(reply).await
    }
    /// Handles multiple NATS subjects and processes messages using the provided processor
    #[instrument(skip(processor))]
    pub async fn handle_multiple<R: RequestProcessor + 'static>(
        &self,
        subjects: Vec<&str>,
        processor: R,
    ) -> Result<MultipleHandle, Error> {
        info!("Setting up multiple handlers for subjects: {:?}", subjects);
        let mut handles = Vec::new();
        for subject in subjects.iter() {
            debug!("Setting up handler for subject: {}", subject);

            let subject = subject.to_owned();

            let handle = self.handle(&subject, processor.clone()).await?;
            handles.push(handle);
        }
        Ok(MultipleHandle { handles })
    }
}

/// A structure that handles multiple NATS subject and responds to messages
#[derive(Debug)]
pub struct MultipleHandle {
    handles: Vec<Handle>,
}

impl MultipleHandle {
    /// Closes all subscriptions
    #[instrument(skip_all)]
    pub async fn shutdown(self) -> Result<(), Error> {
        for handle in self.handles {
            handle.shutdown().await;
        }
        Ok(())
    }
    pub async fn abort(&self) {
        for handle in self.handles.iter() {
            handle.abort().await;
        }
    }
}

impl Handle {
    /// Gracefully shuts down the subscription
    #[instrument(skip_all)]
    pub async fn shutdown(self) {
        info!("Initiating shutdown for handle");
        self.cancel.cancel();
        if let Err(e) = self.handle.await {
            error!("handler join error: {:?}", e);
        }
    }
    pub async fn abort(&self) {
        info!("Aborting handle");
        self.cancel.cancel();
        self.handle.abort();
    }
}

/// A trait that defines a request processor for NATS messages
#[async_trait]
pub trait RequestProcessor: Send + Sync + Clone + std::fmt::Debug {
    async fn process(&self, message: Message) -> Result<ReplyMessage, Error>;
}

/// A structure that represents a reply message
#[derive(Clone, Debug)]
pub struct ReplyMessage {
    pub reply: String,
    pub payload: Bytes,
}
/// A structure that represents an error reply message
pub struct ReplyErrorMessage(pub Error);

/// An easy-to-use function that creates a reply message
pub fn reply(msg: &Message, payload: Bytes) -> ReplyMessage {
    ReplyMessage {
        reply: msg.reply.clone().unwrap_or_else(|| "".into()).to_string(),
        payload,
    }
}
