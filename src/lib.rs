//! Nats Handling is a library designed for seamless NATS message handling in Rust. It offers a straightforward API for subscribing to NATS subjects, processing messages, and sending replies.
//! The goal of this library is to provide an experience similar to HTTP handling, but tailored for NATS.
pub mod error;
#[cfg(feature = "jetstream")]
pub mod jetstream;
pub use async_nats::ConnectOptions;
pub use async_nats::Request;
use async_nats::{Client, HeaderMap, Subscriber};
use async_trait::async_trait;
use bytes::Bytes;
pub use error::Error;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace};

/// A structure that handles specified NATS subject and responds to messages
#[derive(Debug)]
pub struct Handle {
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
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
        let client = ConnectOptions::new().connect(bind).await?;
        info!("Successfully connected to NATS server");
        Ok(Self { client })
    }
    #[instrument(skip_all)]
    /// Creates JetStream context from the NATS client
    #[cfg(feature = "jetstream")]
    pub fn jetstream(&self) -> jetstream::JetStream {
        jetstream::JetStream::new(self.clone())
    }

    /// Creates a new NATS client with specified options and connects to the server
    #[instrument(skip_all)]
    pub async fn with_options(bind: &[&str], options: ConnectOptions) -> Result<Self, Error> {
        info!("Connecting to NATS server at {:?}", bind);

        let client = options.connect(bind).await?;

        info!("Successfully connected to NATS server");
        Ok(Self { client })
    }
    /// Subscribes to a specified NATS subject
    #[instrument(skip_all)]
    pub async fn subscribe(&self, subject: impl AsRef<str>) -> Result<Subscriber, Error> {
        let subject = subject.as_ref().to_owned();
        info!("Subscribing to subject: {}", subject);
        trace!("Calling client.subscribe with subject: {}", subject);
        let subscription = self.client.subscribe(subject.to_owned()).await?;
        debug!("Successfully subscribed to {}", subject);
        Ok(subscription)
    }
    /// Publishes a message to a specified NATS subject
    #[instrument(skip_all)]
    pub async fn publish(&self, subject: impl AsRef<str>, payload: Bytes) -> Result<(), Error> {
        let subject = subject.as_ref().to_owned();
        debug!("Publishing message to subject: {}", subject);
        trace!("Payload size: {}", payload.len());
        self.client.publish(subject.to_owned(), payload).await?;
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
        let response = self.client.request(subject.to_owned(), payload).await?;
        debug!("Received response from {}", subject);
        trace!("Response payload size: {}", response.payload.len());
        Ok(Message(response))
    }
    /// Sends a request with headers to a specified NATS subject and returns the response
    #[instrument(skip_all)]
    pub async fn request_with_headers(
        &self,
        subject: impl AsRef<str>,
        payload: Bytes,
        headers: HeaderMap,
    ) -> Result<Message, Error> {
        let subject = subject.as_ref().to_owned();
        debug!("Sending request to subject: {}", subject);
        trace!("Payload size: {}", payload.len());
        let response = self
            .client
            .request_with_headers(subject.clone(), headers, payload)
            .await?;
        debug!("Received response from {}", subject);
        trace!("Response payload size: {}", response.payload.len());
        Ok(Message(response))
    }
    /// Sends a custom request structure to a specified NATS subject and returns the response
    #[instrument(skip_all)]
    pub async fn send_request(
        &self,
        subject: impl AsRef<str>,
        req: Request,
    ) -> Result<Message, Error> {
        let subject = subject.as_ref().to_owned();
        debug!("Sending request to subject: {}", subject);

        let response = self.client.send_request(subject.clone(), req).await?;
        debug!("Received response from {}", subject);
        Ok(Message(response))
    }

    /// Handles a specified NATS subject and processes messages using the provided processor
    #[instrument(skip_all)]
    pub async fn handle<R: MessageProcessor + 'static>(
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
                match moved_processor.process(Message(message.clone())).await {
                    Ok(reply) => {
                    debug!("Successfully processed message");
                    if let Some(reply) = reply {
                        debug!("Sending reply: {:?}", reply);
                        if let Err(e) = client_clone.reply(reply).await {
                            error!("Failed to reply to message: {}", e);
                        }
                    } else {
                        debug!("No reply needed");
                    }

                    }
                    Err(e) => {
                    error!("Failed to process message: {}", e);
                    if let Err(e) = client_clone
                        .reply_err(ReplyErrorMessage(Box::new(e)), Message(message.clone()))
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
            handle: Some(handle),
        })
    }

    /// Sends a reply to a message
    #[instrument(skip_all)]
    pub async fn reply(&self, reply: ReplyMessage) -> Result<(), Error> {
        debug!("Sending reply to: {}", reply.subject);
        trace!("Reply payload size: {}", reply.payload.len());

        match (reply.headers, reply.reply) {
            (Some(headers), Some(reply_subject)) => {
                self.client
                    .publish_with_reply_and_headers(
                        reply.subject.clone(),
                        reply_subject,
                        headers,
                        reply.payload.clone(),
                    )
                    .await?;
            }
            (Some(headers), None) => {
                self.client
                    .publish_with_headers(reply.subject.clone(), headers, reply.payload.clone())
                    .await?;
            }
            (None, Some(reply_subject)) => {
                self.client
                    .publish_with_reply(reply.subject.clone(), reply_subject, reply.payload.clone())
                    .await?;
            }
            (None, None) => {
                self.client
                    .publish(reply.subject.clone(), reply.payload.clone())
                    .await?;
            }
        }

        debug!("Successfully sent reply to {}", reply.subject);
        Ok(())
    }
    /// Sends an error reply to a message
    #[instrument(skip_all)]
    async fn reply_err(&self, err: ReplyErrorMessage, msg_source: Message) -> Result<(), Error> {
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
            reply: None,
        };
        self.reply(reply).await
    }
    /// Handles multiple NATS subjects and processes messages using the provided processor
    #[instrument(skip_all)]
    pub async fn handle_multiple<R: MessageProcessor + 'static>(
        &self,
        subjects: impl IntoIterator<Item = String>,
        processor: R,
    ) -> Result<MultipleHandle, Error> {
        let handles: Vec<Result<Handle, Error>> = futures::stream::iter(subjects)
            .then(|subj| self.handle(subj.clone(), processor.clone()))
            .collect::<Vec<_>>()
            .await;

        let handles: Vec<Handle> = handles.into_iter().collect::<Result<_, _>>()?;

        Ok(MultipleHandle { handles })
    }
    /// Returns the default timeout for requests set when creating the client.
    #[instrument(skip_all)]
    pub fn timeout(&self) -> Option<tokio::time::Duration> {
        self.client.timeout()
    }

    /// Returns last received info from the server.
    #[instrument(skip_all)]
    pub fn server_info(&self) -> async_nats::ServerInfo {
        self.client.server_info()
    }

    /// Returns true if the server version is compatible with the version components.
    #[instrument(skip_all)]
    pub fn is_server_compatible(&self, major: i64, minor: i64, patch: i64) -> bool {
        self.client.is_server_compatible(major, minor, patch)
    }

    /// Flushes the internal buffer ensuring that all messages are sent.
    #[instrument(skip_all)]
    pub async fn flush(&self) -> Result<(), Error> {
        Ok(self.client.flush().await?)
    }

    /// Drains all subscriptions, stops any new messages from being published, and flushes any remaining messages, then closes the connection.
    #[instrument(skip_all)]
    pub async fn drain(&self) -> Result<(), Error> {
        self.client.drain().await.map_err(Into::into)
    }

    /// Returns the current state of the connection.
    #[instrument(skip_all)]
    pub fn connection_state(&self) -> async_nats::connection::State {
        self.client.connection_state()
    }

    /// Forces the client to reconnect.
    #[instrument(skip_all)]
    pub async fn force_reconnect(&self) -> Result<(), Error> {
        self.client.force_reconnect().await.map_err(Into::into)
    }

    /// Subscribes to a subject with a queue group to receive messages.
    #[instrument(skip_all)]
    pub async fn queue_subscribe(
        &self,
        subject: impl AsRef<str>,
        queue_group: impl AsRef<str>,
    ) -> Result<Subscriber, Error> {
        let subject = subject.as_ref().to_owned();
        let queue_group = queue_group.as_ref().to_owned();
        info!(
            "Subscribing to subject: {} with queue group: {}",
            subject, queue_group
        );

        trace!(
            "Calling client.queue_subscribe with subject: {} and queue group: {}",
            subject,
            queue_group
        );
        let subscription = self
            .client
            .queue_subscribe(subject.clone(), queue_group.clone())
            .await?;
        debug!(
            "Successfully subscribed to {} with queue group: {}",
            subject, queue_group
        );

        Ok(subscription)
    }
    /// Returns statistics for the instance of the client throughout its lifecycle.
    #[instrument(skip_all)]
    pub fn statistics(&self) -> std::sync::Arc<async_nats::Statistics> {
        self.client.statistics()
    }

    /// Creates a new globally unique inbox which can be used for replies.
    #[instrument(skip_all)]
    pub fn new_inbox(&self) -> String {
        self.client.new_inbox()
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
    pub async fn abort(&mut self) {
        for handle in self.handles.iter_mut() {
            handle.abort().await;
        }
    }
}

impl Handle {
    /// Gracefully shuts down the subscription
    #[instrument(skip_all)]
    pub async fn shutdown(mut self) {
        info!("Initiating shutdown for handle");
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            if let Err(e) = handle.await {
                error!("handle join error: {:?}", e);
            }
        }
    }
    pub async fn abort(&mut self) {
        info!("Aborting handle");
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
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
    pub fn reply(&self, payload: Bytes) -> ReplyMessage {
        ReplyMessage {
            subject: self.reply.clone().unwrap_or_else(|| "".into()).to_string(),
            payload,
            headers: None,
            reply: None,
        }
    }
}

#[deprecated(note = "Please use MessageProcessor instead")]
pub type RequestProcessor = dyn MessageProcessor;

/// A trait that defines a request processor for NATS messages
#[async_trait]
pub trait MessageProcessor: Send + Sync + Clone {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn process(&self, message: Message) -> Result<Option<ReplyMessage>, Self::Error>;
}

/// A structure that represents a reply message
#[derive(Clone, Debug)]
pub struct ReplyMessage {
    pub reply: Option<String>,
    pub subject: String,
    pub payload: Bytes,
    pub headers: Option<HeaderMap>,
}
impl ReplyMessage {
    pub fn new(subject: String, payload: Bytes) -> Self {
        Self {
            reply: None,
            subject,
            payload,
            headers: None,
        }
    }
}
//TODO: Fix sending custom PublishMessage natively via method
// impl Into<async_nats::PublishMessage> for ReplyMessage {
//     fn into(self) -> async_nats::PublishMessage {
//         async_nats::PublishMessage {
//             subject: self.subject.into(),
//             payload: self.payload,
//             reply: self.reply.map(|s| s.into()),
//             headers: self.headers,
//         }
//     }
// }

/// A structure that represents an error reply message
struct ReplyErrorMessage(pub Box<dyn std::error::Error + Send + Sync>);

/// An easy-to-use function that creates a reply message
pub fn reply(msg: &Message, payload: Bytes) -> ReplyMessage {
    ReplyMessage {
        subject: msg.reply.clone().unwrap_or_else(|| "".into()).to_string(),
        payload,
        headers: None,
        reply: None,
    }
}
