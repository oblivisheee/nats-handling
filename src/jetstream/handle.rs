use super::JetStreamError;
use async_nats::jetstream::{
    self,
    consumer::{PullConsumer, PushConsumer},
};
use futures::StreamExt;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
pub struct Handle {
    handle: Option<tokio::task::JoinHandle<()>>,
    cancel: CancellationToken,
}

impl Handle {
    /// Initializes a handler for a push-based consumer.
    /// This method spawns a task to process messages received from the push consumer.
    pub async fn push<R: super::MessageProcessor + Send + 'static + Sync>(
        consumer: PushConsumer,
        processor: Arc<R>,
        handle_config: HandleConfig,
    ) -> Result<Self, JetStreamError> {
        tracing::info!("Initializing push consumer handler...");
        let messages = consumer.messages().await?;
        tracing::info!("Push consumer messages stream initialized.");

        let cancel_token = CancellationToken::new();
        let cancel_token_child = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let stream = messages;

            let stop_signal = cancel_token_child.cancelled();
            tokio::select! {
                _ = async {
                    stream
                        .for_each_concurrent(handle_config.concurrency, |message|  {
                            let processor = processor.clone();
                            let handle_config = handle_config.clone();
                            async move {
                                tracing::debug!("Received a message from push consumer.");
                                match message {
                                    Ok(msg) => {
                                        tracing::debug!("Processing message: {:?}", msg);
                                        match processor.process(msg.clone().into()).await {
                                            Ok(_) => {
                                                tracing::debug!("Message processed successfully.");

                                                if let Err(e) = msg.ack().await {
                                                    tracing::error!("Error acknowledging message: {:?}", e);
                                                } else {
                                                    tracing::debug!("Message acknowledged successfully.");
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!("Error processing message: {:?}", e);

                                                if let Err(e) = msg.ack_with(jetstream::AckKind::Nak(handle_config.nak_duration)).await {
                                                    tracing::error!("Error NAK'ing message: {:?}", e);
                                                } else {
                                                    tracing::debug!("Message NAk'd successfully.");
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Error receiving message: {:?}", e);
                                    }
                                }
                            }
                        })
                        .await;
                } => {},
                _ = stop_signal => {
                    tracing::info!("Push consumer handler received shutdown signal.");
                },
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
        consumer: PullConsumer,
        fetcher: Box<dyn super::PullFetcher>,
        processor: Arc<R>,
        handle_config: HandleConfig,
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
                    Ok(messages) => {
                    tracing::debug!("Messages fetched successfully.");
                    messages
                        .for_each_concurrent(handle_config.concurrency, |msg| {
                        let processor = processor.clone();
                        let handle_config = handle_config.clone();
                        async move {
                            tracing::debug!("Processing message from pull consumer.");
                            match msg {
                            Ok(message) => {
                                tracing::debug!("Processing message: {:?}", message.message);
                                match processor.process(message.message.clone().into()).await {
                                Ok(_) => {
                                    tracing::debug!("Message processed successfully.");

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

                                    if let Err(e) = message.ack_with(jetstream::AckKind::Nak(handle_config.nak_duration)).await {
                                    tracing::error!("Error NAK'ing message: {:?}", e);
                                    } else {
                                    tracing::debug!("Message NAk'd successfully.");
                                    }
                                }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error receiving message: {:?}", e);
                            }
                            }
                        }
                        })
                        .await;
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

#[derive(Debug, Clone)]
/// Configuration for the message handler.
pub struct HandleConfig {
    pub nak_duration: Option<std::time::Duration>,
    pub concurrency: usize,
}
impl Default for HandleConfig {
    fn default() -> Self {
        Self {
            nak_duration: Some(std::time::Duration::from_secs(5)),
            concurrency: 1,
        }
    }
}
