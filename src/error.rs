use async_nats::{Error as NatsError, UnsubscribeError};
use thiserror::Error;
#[derive(Error, Debug)]
pub enum Error {
    #[error("NATS error: {0}")]
    NatsError(#[from] NatsError),
    #[error("Failed to unsubscribe: {0}")]
    UnsubscribeError(#[from] UnsubscribeError),
}
