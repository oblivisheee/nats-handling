pub use async_nats::Error as NatsError;
pub use async_nats::Message;
use async_nats::{Client, ConnectOptions, Subscriber};
pub use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument};

#[derive(Debug)]
pub struct Handle<T, R> {
    client: T,
    sub: Arc<Mutex<Subscriber>>,
    request_processor: R,
}

#[derive(Clone, Debug)]
pub struct NatsClient {
    client: Client,
}

impl NatsClient {
    #[instrument]
    pub async fn new(bind: &[&str]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to NATS server at {:?}", bind);
        let client = ConnectOptions::new().connect(bind).await.map_err(|e| {
            error!("Failed to connect to NATS: {}", e);
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;
        info!("Successfully connected to NATS server");
        Ok(Self { client })
    }

    #[instrument]
    pub async fn subscribe(
        &self,
        subject: String,
    ) -> Result<Subscriber, Box<dyn std::error::Error + Send + Sync>> {
        info!("Subscribing to subject: {}", subject);
        let subscription = self.client.subscribe(subject.clone()).await.map_err(|e| {
            error!("Failed to subscribe to {}: {}", subject, e);
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;
        debug!("Successfully subscribed to {}", subject);
        Ok(subscription)
    }

    #[instrument]
    pub async fn publish(
        &self,
        subject: String,
        payload: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Publishing message to subject: {}", subject);
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

    #[instrument]
    pub async fn request(
        &self,
        subject: String,
        payload: Bytes,
    ) -> Result<Message, Box<dyn std::error::Error + Send + Sync>> {
        debug!("Sending request to subject: {}", subject);
        let response = self
            .client
            .request(subject.clone(), payload)
            .await
            .map_err(|e| {
                error!("Request failed for {}: {}", subject, e);
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
        debug!("Received response from {}", subject);
        Ok(response)
    }

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
                match moved_processor.process(message.clone()).await {
                    Ok(reply) => {
                        if let Some(reply) = reply {
                            if let Err(e) = client_clone.reply(reply).await {
                                error!("Error replying to message: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error processing message: {:?}", e);
                        let err_reply = ReplyErrorMessage(e);
                        if let Err(e) = client_clone.reply_err(err_reply, message).await {
                            error!("Error replying to message: {:?}", e);
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

    #[instrument]
    pub async fn reply(
        &self,
        reply: ReplyMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!(
            "Sending reply to subject: {}, reply: {}",
            reply.subject, reply.reply
        );
        self.client
            .publish_with_reply(reply.subject.clone(), reply.reply.clone(), reply.payload)
            .await
            .map_err(|e| {
                error!("Failed to reply to {}: {}", reply.subject, e);
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
        debug!("Successfully sent reply to {}", reply.subject);
        Ok(())
    }
    async fn reply_err(
        &self,
        err: ReplyErrorMessage,
        msg_source: Message,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let reply = ReplyMessage {
            subject: msg_source.subject.to_string(),
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
            let moved_sub = subscriber.clone();
            let moved_processor = processor.clone();
            let subject = subject.to_string();
            let client_clone = self.clone();

            self.handle(&subject, processor).await?;
            subs.push(subscriber);
        }
        Ok(MutlipleHandle {
            client: self.clone(),
            subs,
            request_processor: processor,
        })
    }
}

#[derive(Debug)]
pub struct MutlipleHandle<T, R> {
    client: T,
    subs: Vec<Arc<Mutex<Subscriber>>>,
    request_processor: R,
}

impl<R: RequestProcessor> MutlipleHandle<NatsClient, R> {
    #[instrument]
    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for sub in &self.subs {
            let mut sub = sub.lock().await;
            sub.unsubscribe().await?;
        }
        Ok(())
    }
}

impl<R: RequestProcessor> Handle<NatsClient, R> {
    #[instrument]
    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut sub = self.sub.lock().await;
        sub.unsubscribe().await?;
        Ok(())
    }
}

#[async_trait]
pub trait RequestProcessor: Send + Sync + Clone + std::fmt::Debug + std::marker::Copy {
    async fn process(
        &self,
        message: Message,
    ) -> Result<Option<ReplyMessage>, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone, Debug)]
pub struct ReplyMessage {
    pub subject: String,
    pub reply: String,
    pub payload: Bytes,
}

pub struct ReplyErrorMessage(pub Box<dyn std::error::Error + Send + Sync>);

pub fn reply(msg: Message, payload: Bytes) -> ReplyMessage {
    ReplyMessage {
        subject: msg.subject.clone().to_string(),
        reply: msg.reply.clone().unwrap_or_else(|| "".into()).to_string(),
        payload,
    }
}
