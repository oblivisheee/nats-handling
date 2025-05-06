use super::JetStreamError;
use async_nats::jetstream::consumer::{PullConsumer, PushConsumer};
use futures::StreamExt;

pub struct Handle {
    handle: tokio::task::JoinHandle<()>,
}

impl Handle {
    /// Initializes a handler for a push-based consumer.
    /// This method spawns a task to process messages received from the push consumer.
    pub async fn push<R: super::MessageProcessor + Send + 'static>(
        client: super::JetStream,
        consumer: PushConsumer,
        processor: R,
    ) -> Result<Self, JetStreamError> {
        tracing::info!("Initializing push consumer handler...");
        let messages = consumer.messages().await?;
        tracing::info!("Push consumer messages stream initialized.");

        let handle = tokio::spawn(async move {
            let mut stream = messages;
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
            tracing::info!("Push consumer handler exiting.");
        });

        tracing::info!("Push consumer handler task spawned.");
        Ok(Self { handle })
    }

    /// Initializes a handler for a pull-based consumer.
    /// This method spawns a task to fetch and process messages from the pull consumer.
    pub async fn pull<R: super::MessageProcessor + Send + 'static>(
        client: super::JetStream,
        consumer: PullConsumer,
        fetcher: Box<dyn super::PullFetcher>,
        processor: R,
    ) -> Result<Self, JetStreamError> {
        tracing::info!("Initializing pull consumer handler...");
        let mut fetch_stream = fetcher.create_stream();
        tracing::info!("Pull fetch stream initialized.");

        let handle = tokio::spawn(async move {
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
            tracing::info!("Pull consumer handler exiting.");
        });

        tracing::info!("Pull consumer handler task spawned.");
        Ok(Self { handle })
    }
}
