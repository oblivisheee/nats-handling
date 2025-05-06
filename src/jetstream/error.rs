use async_nats::jetstream;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum JetStreamError {
    #[error("Stream info error: {0}")]
    StreamInfoError(#[from] jetstream::stream::InfoError),
    #[error("Stream create error: {0}")]
    StreamCreateError(#[from] jetstream::context::CreateStreamError),
    #[error("Consumer create error: {0}")]
    ConsumerCreateError(#[from] jetstream::stream::ConsumerError),
    #[error("Stream error: {0}")]
    StreamError(#[from] jetstream::consumer::StreamError),
    #[error("Publish error: {0}")]
    PublishError(#[from] async_nats::jetstream::context::PublishError),
}
