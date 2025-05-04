use async_nats::jetstream;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum JetStreamError {
    #[error("Stream info error: {0}")]
    StreamInfoError(#[from] jetstream::stream::InfoError),
}
