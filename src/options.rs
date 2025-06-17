use crate::error::Error;
use crate::NatsClient;
use async_nats::ConnectOptions as NatsConnectOptions;
use tracing::info;
#[derive(Debug, Default)]
/// A structure that holds options for connecting to a NATS server
pub struct ConnectOptions(NatsConnectOptions);

impl ConnectOptions {
    /// Creates a new instance of `ConnectOptions` with default settings
    pub fn new() -> Self {
        ConnectOptions(NatsConnectOptions::default())
    }
    /// Configures the NATS client with the provided options
    pub async fn connect(self, bind: &[&str]) -> Result<NatsClient, Error> {
        info!("Connecting to NATS server at {:?}", bind);
        Ok(NatsClient::with_options(bind, self.0).await?)
    }
}

impl std::ops::Deref for ConnectOptions {
    type Target = NatsConnectOptions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for ConnectOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
