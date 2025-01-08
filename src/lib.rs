//! Nats Handling is a library designed for seamless NATS message handling in Rust. It offers a straightforward API for subscribing to NATS subjects, processing messages, and sending replies.
//! The goal of this library is to provide an experience similar to HTTP handling, but tailored for NATS.

pub use async_nats::Error as NatsError;
pub use async_nats::Message;
use async_nats::{Client, ConnectOptions, Subscriber};
pub use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, trace};

/// A structure that handles specified NATS subject and responds to messages
#[derive(Debug)]
pub struct Handle<T, R> {
    client: T,
    sub: Arc<Mutex<Subscriber>>,
    request_processor: R,
}

/// A structure that represents a connection to a NATS server
#[derive(Clone, Debug)]
pub struct NatsClient {
    client: Client,
}

impl NatsClient {
    /// Creates a new NATS client and connects to the specified server
    #[instrument]
    pub async fn new(bind: &[&str]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to NATS server at {:?}", bind);
        trace!("Creating ConnectOptions");
        let client = ConnectOptions::new().connect(bind).await.map_err(|e| {
            error!("Failed to connect to NATS: {}", e);
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;
        info!("Successfully connected to NATS server");
        Ok(Self { client })
    }
    /// Subscribes to a specified NATS subject
    #[instrument]
    pub async fn subscribe(
        &self,
        subject: String,
    ) -> Result<Subscriber, Box<dyn std::error::Error + Send + Sync>> {
        info!("Subscribing to subject: {}", subject);
        trace!("Calling client.subscribe with subject: {}", subject);
        let subscription = self.client.subscribe(subject.clone()).await.map_err(|e| {
            error!("Failed to subscribe to {}: {}", subject, e);
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;
        debug!("Successfully subscribed to {}", subject);
        Ok(subscription)
    }
    /// Publishes a message to a specified NATS subject
    #[instrument]
    pub async fn publish(
        &self,
        subject: String,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Publishing message to subject: {}", subject);
        trace!("Payload size: {}", payload.len());
        self.client
            .publish(subject.clone(), payload)
            .await
            .map_err(|e| {
                error!("Failed to publish to {}: {}", subject, e);
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
        debug!("Successfully published to {}", subject);
        Ok(())
    }
    /// Sends a request to a specified NATS subject and returns the response
    #[instrument]
    pub async fn request(
        &self,
        subject: String,
        payload: Bytes,
    ) -> Result<Message, Box<dyn std::error::Error + Send + Sync>> {
        debug!("Sending request to subject: {}", subject);
        trace!("Payload size: {}", payload.len());
        let response = self
            .client
            .request(subject.clone(), payload)
            .await
            .map_err(|e| {
                error!("Request failed for {}: {}", subject, e);
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
        debug!("Received response from {}", subject);
        trace!("Response payload size: {}", response.payload.len());
        Ok(response)
    }
    /// Handles a specified NATS subject and processes messages using the provided processor
    #[instrument(skip(processor))]
    pub async fn handle<R: RequestProcessor + 'static>(
        &self,
        subject: &str,
        processor: R,
    ) -> Result<Handle<NatsClient, R>, Box<dyn std::error::Error + Send + Sync>> {
        info!("Setting up handler for subject: {}", subject);
        let subject = subject.to_string();
        let subscriber = Arc::new(Mutex::new(self.subscribe(subject.clone()).await?));
        let moved_sub = subscriber.clone();
        let moved_processor = processor.clone();
        let moved_subject = subject.clone();
        let client_clone = self.clone();

        tokio::spawn(async move {
            info!("Started message processing loop for {}", moved_subject);
            while let Some(message) = moved_sub.lock().await.next().await {
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
        });

        Ok(Handle {
            client: self.clone(),
            sub: subscriber,
            request_processor: processor,
        })
    }
    /// Sends a reply to a message
    #[instrument]
    pub async fn reply(
        &self,
        reply: ReplyMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Sending reply to:  {}", reply.reply);
        trace!("Reply payload size: {}", reply.payload.len());
        self.client
            .publish(reply.reply.clone(), reply.payload.clone())
            .await
            .map_err(|e| {
                error!("Failed to reply to {}: {}", reply.reply, e);
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
        debug!("Successfully sent reply to {}", reply.reply);
        Ok(())
    }
    /// Sends an error reply to a message
    async fn reply_err(
        &self,
        err: ReplyErrorMessage,
        msg_source: Message,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    ) -> Result<MutlipleHandle<NatsClient, R>, Box<dyn std::error::Error + Send + Sync>> {
        info!("Setting up multiple handlers for subjects: {:?}", subjects);
        let mut subs = Vec::new();
        for subject in subjects.iter() {
            debug!("Setting up handler for subject: {}", subject);
            let subscriber = Arc::new(Mutex::new(self.subscribe(subject.to_string()).await?));
            let subject = subject.to_string();

            self.handle(&subject, processor.clone()).await?;
            subs.push(subscriber);
        }
        Ok(MutlipleHandle {
            client: self.clone(),
            subs,
            request_processor: processor,
        })
    }
}

/// A structure that handles multiple NATS subject and responds to messages
#[derive(Debug)]
pub struct MutlipleHandle<T, R> {
    client: T,
    subs: Vec<Arc<Mutex<Subscriber>>>,
    request_processor: R,
}

impl<R: RequestProcessor> MutlipleHandle<NatsClient, R> {
    /// Closes all subscriptions
    #[instrument]
    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for sub in &self.subs {
            let mut sub = sub.lock().await;
            trace!("Unsubscribing from subject");
            sub.unsubscribe().await?;
        }
        Ok(())
    }
}

impl<R: RequestProcessor> Handle<NatsClient, R> {
    /// Closes the subscription
    #[instrument]
    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut sub = self.sub.lock().await;
        trace!("Unsubscribing from subject");
        sub.unsubscribe().await?;
        Ok(())
    }
}

/// A trait that defines a request processor for NATS messages
#[async_trait]
pub trait RequestProcessor: Send + Sync + Clone + std::fmt::Debug {
    async fn process(
        &self,
        message: Message,
    ) -> Result<ReplyMessage, Box<dyn std::error::Error + Send + Sync>>;
}

/// A structure that represents a reply message
#[derive(Clone, Debug)]
pub struct ReplyMessage {
    pub reply: String,
    pub payload: Bytes,
}
/// A structure that represents an error reply message
pub struct ReplyErrorMessage(pub Box<dyn std::error::Error + Send + Sync>);

/// An easy-to-use function that creates a reply message
pub fn reply(msg: &Message, payload: Bytes) -> ReplyMessage {
    ReplyMessage {
        reply: msg.reply.clone().unwrap_or_else(|| "".into()).to_string(),
        payload,
    }
}
