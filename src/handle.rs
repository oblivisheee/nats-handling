use crate::{
    messages::{Message, MessageProcessor, ReplyErrorMessage},
    NatsClient, Subscriber,
};

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

pub async fn run_handler<R>(
    mut subscriber: Subscriber,
    client: NatsClient,
    processor: R,
    cancel_token_child: CancellationToken,
    moved_subject: String,
) where
    R: MessageProcessor + 'static,
{
    info!("Started message processing loop for {}", moved_subject);
    let stop_signal = cancel_token_child.cancelled();
    let client_clone = client.clone();

    tokio::select! {
        _ = async {
            while let Some(message) = subscriber.next().await {
                debug!("Processing message from subject: {}", message.subject);
                trace!("Message payload size: {}", message.payload.len());
                match processor.process(Message(message.clone())).await {
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
}
