mod consumer;
mod error;
mod stream;
use crate::NatsClient;
use async_nats::{
    jetstream::{self, message::Message, stream::Config, Context},
    subject,
};
pub use error::JetStreamError;

pub struct JetStreamContext {
    pub context: Context,
}

impl JetStreamContext {
    pub fn new(client: NatsClient) -> Self {
        let context = jetstream::new(client.client);

        JetStreamContext { context }
    }
    pub async fn stream(&self) {}
}
