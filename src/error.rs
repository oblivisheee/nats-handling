use async_nats::{Error as NatsError, UnsubscribeError};
use thiserror::Error;
#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    NatsError(#[from] NatsError),
    #[error("Unsubscribe error: {0}")]
    UnsubscribeError(#[from] UnsubscribeError),
    #[error("Subscribe error: {0}")]
    SubscribeError(#[from] async_nats::SubscribeError),
    #[error("Connect error: {0}")]
    ConnectionError(#[from] async_nats::ConnectError),
    #[error("Request error: {0}")]
    RequestError(#[from] async_nats::RequestError),
    #[error("Publish error: {0}")]
    PublishError(#[from] async_nats::PublishError),
    #[error("Flush error: {0}")]
    FlushError(#[from] async_nats::client::FlushError),
    #[error("Drain error: {0}")]
    DrainError(#[from] async_nats::client::DrainError),
    #[error("Reconnect error: {0}")]
    ReconnectError(#[from] async_nats::client::ReconnectError),
}
