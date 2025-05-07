use super::JetStreamError;
use async_nats::jetstream::consumer::{PullConsumer, PushConsumer};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

pub struct Handle {
    handle: Option<tokio::task::JoinHandle<()>>,
    cancel: CancellationToken,
}

impl Handle {
    /// Initializes a handler for a push-based consumer.
    /// This method spawns a task to process messages received from the push consumer.
    pub async fn push<R: super::MessageProcessor + Send + 'static + Sync>(
        client: super::JetStream,
        consumer: PushConsumer,
        processor: R,
    ) -> Result<Self, JetStreamError> {
        tracing::info!("Initializing push consumer handler...");
        let messages = consumer.messages().await?;
        tracing::info!("Push consumer messages stream initialized.");

        let cancel_token = CancellationToken::new();
        let cancel_token_child = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let mut stream = messages;
            let stop_signal = cancel_token_child.cancelled();
            tokio::select! {
                _ = async {
                    while let Some(message) = stream.next().await {
                        tracing::debug!("Received a message from push consumer.");
                        match message {
                            Ok(msg) => {
                                tracing::debug!("Processing message: {:?}", msg);
                                match processor.process(msg.clone().into()).await {
                                    Ok(reply) => {
                                        tracing::debug!("Message processed successfully.");
                                        if let Some(reply) = reply {
                                            tracing::debug!("Sending reply: {:?}", reply);
                                            if let Err(e) = client.reply(reply).await {
                                                tracing::error!("Error sending reply: {:?}", e);
                                            }
                                        }
                                        if let Err(e) = msg.ack().await {
                                            tracing::error!("Error acknowledging message: {:?}", e);
                                        } else {
                                            tracing::debug!("Message acknowledged successfully.");
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Error processing message: {:?}", e);
                                        if let Err(e) = client
                                            .reply_err(
                                                crate::ReplyErrorMessage(Box::new(e)),
                                                msg.clone().into(),
                                            )
                                            .await
                                        {
                                            tracing::error!("Error sending error reply: {:?}", e);
                                        }
                                        if let Err(e) = msg.ack().await {
                                            tracing::error!("Error acknowledging message: {:?}", e);
                                        } else {
                                            tracing::debug!("Message acknowledged successfully.");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error receiving message: {:?}", e);
                            }
                        }
                    }
                } => {},
                _ = stop_signal => {
                    tracing::info!("Push consumer handler received shutdown signal.");
                }
            }
            tracing::info!("Push consumer handler exiting.");
        });

        tracing::info!("Push consumer handler task spawned.");
        Ok(Self {
            handle: Some(handle),
            cancel: cancel_token,
        })
    }

    /// Initializes a handler for a pull-based consumer.
    /// This method spawns a task to fetch and process messages from the pull consumer.
    pub async fn pull<R: super::MessageProcessor + Send + 'static + Sync>(
        client: super::JetStream,
        consumer: PullConsumer,
        fetcher: Box<dyn super::PullFetcher>,
        processor: R,
    ) -> Result<Self, JetStreamError> {
        tracing::info!("Initializing pull consumer handler...");
        let mut fetch_stream = fetcher.create_stream();
        tracing::info!("Pull fetch stream initialized.");

        let cancel_token = CancellationToken::new();
        let cancel_token_child = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let stop_signal = cancel_token_child.cancelled();
            tokio::select! {
                _ = async {
                    while let Some(c) = fetch_stream.next().await {
                        tracing::debug!("Fetching {} messages from pull consumer.", c);
                        let messages = consumer.fetch().max_messages(c).messages().await;
                        match messages {
                            Ok(mut messages) => {
                                tracing::debug!("Messages fetched successfully.");
                                while let Some(msg) = messages.next().await {
                                    tracing::debug!("Processing message from pull consumer.");
                                    match msg {
                                        Ok(message) => {
                                            tracing::debug!("Processing message: {:?}", message.message);
                                            match processor.process(message.message.clone().into()).await {
                                                Ok(reply) => {
                                                    tracing::debug!("Message processed successfully.");
                                                    if let Some(reply) = reply {
                                                        tracing::debug!("Sending reply: {:?}", reply);
                                                        if let Err(e) = client.reply(reply).await {
                                                            tracing::error!("Error sending reply: {:?}", e);
                                                        }
                                                    }
                                                    if let Err(e) = message.ack().await {
                                                        tracing::error!(
                                                            "Error acknowledging message: {:?}",
                                                            e
                                                        );
                                                    } else {
                                                        tracing::debug!(
                                                            "Message acknowledged successfully."
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::error!("Error processing message: {:?}", e);
                                                    if let Err(e) = client
                                                        .reply_err(
                                                            crate::ReplyErrorMessage(Box::new(e)),
                                                            message.clone().into(),
                                                        )
                                                        .await
                                                    {
                                                        tracing::error!(
                                                            "Error sending error reply: {:?}",
                                                            e
                                                        );
                                                    }
                                                    if let Err(e) = message.ack().await {
                                                        tracing::error!(
                                                            "Error acknowledging message: {:?}",
                                                            e
                                                        );
                                                    } else {
                                                        tracing::debug!(
                                                            "Message acknowledged successfully."
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Error receiving message: {:?}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error fetching messages: {:?}", e);
                                continue;
                            }
                        }
                    }
                } => {},
                _ = stop_signal => {
                    tracing::info!("Pull consumer handler received shutdown signal.");
                }
            }
            tracing::info!("Pull consumer handler exiting.");
        });

        tracing::info!("Pull consumer handler task spawned.");
        Ok(Self {
            handle: Some(handle),
            cancel: cancel_token,
        })
    }

    /// Gracefully shuts down the handler.
    pub async fn shutdown(mut self) {
        tracing::info!("Initiating shutdown for handle.");
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            if let Err(e) = handle.await {
                tracing::error!("Handle join error: {:?}", e);
            }
        }
    }

    /// Aborts the handler immediately.
    pub async fn abort(&mut self) {
        tracing::info!("Aborting handle.");
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
