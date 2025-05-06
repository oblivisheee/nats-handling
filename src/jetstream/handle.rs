use super::JetStreamError;
use async_nats::jetstream::consumer::{PullConsumer, PushConsumer};
use futures::StreamExt;
pub struct Handle {
    handle: tokio::task::JoinHandle<()>,
}

impl Handle {
    pub async fn push<R: super::MessageProcessor + Send + 'static>(
        client: super::JetStream,
        consumer: PushConsumer,
        processor: R,
    ) -> Result<Self, JetStreamError> {
        let messages = consumer.messages().await?;

        let handle = tokio::spawn(async move {
            let mut stream = messages;
            while let Some(message) = stream.next().await {
                match message {
                    Ok(msg) => match processor.process(msg.clone()).await {
                        Ok(reply) => {
                            if let Some(reply) = reply {
                                if let Err(e) = client.reply(reply).await {
                                    tracing::error!("Error sending reply: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error processing message: {:?}", e);
                            if let Err(e) = client
                                .reply_err(crate::ReplyErrorMessage(Box::new(e)), msg)
                                .await
                            {
                                tracing::error!("Error sending error reply: {:?}", e);
                            }
                        }
                    },
                    Err(e) => {
                        tracing::error!("Error receiving message: {:?}", e);
                    }
                }
            }
        });
        Ok(Self { handle })
    }
    pub async fn pull<R: super::MessageProcessor + Send + 'static>(
        client: super::JetStream,
        consumer: PullConsumer,
        fetcher: Box<dyn super::PullFetcher>,
        processor: R,
    ) -> Result<Self, JetStreamError> {
        let mut fetch_stream = fetcher.create_stream();
        let handle = tokio::spawn(async move {
            while let Some(c) = fetch_stream.next().await {
                let messages = consumer.fetch().max_messages(c).messages().await;
                match messages {
                    Ok(mut messages) => {
                        while let Some(msg) = messages.next().await {
                            match msg {
                                Ok(message) => match processor.process(message.clone()).await {
                                    Ok(reply) => {
                                        if let Some(reply) = reply {
                                            if let Err(e) = client.reply(reply).await {
                                                tracing::error!("Error sending reply: {:?}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Error processing message: {:?}", e);
                                        if let Err(e) = client
                                            .reply_err(
                                                crate::ReplyErrorMessage(Box::new(e)),
                                                message,
                                            )
                                            .await
                                        {
                                            tracing::error!("Error sending error reply: {:?}", e);
                                        }
                                    }
                                },
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
        });
        Ok(Self { handle })
    }
}
