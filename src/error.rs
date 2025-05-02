use async_nats::{Error as NatsError, UnsubscribeError};
use thiserror::Error;
#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    NatsError(#[from] NatsError),
    #[error("Unsubscribe error: {0}")]
    UnsubscribeError(#[from] UnsubscribeError),
}
