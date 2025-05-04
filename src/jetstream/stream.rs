use super::JetStreamError;
use async_nats::jetstream::stream::{self, Stream as InnerStream, StreamInfoBuilder};
pub struct Stream {
    inner: InnerStream,
}

impl Stream {
    pub fn info_builder(&self) -> StreamInfoBuilder {
        self.inner.info_builder()
    }
    pub async fn info(&mut self) -> Result<&stream::Info, JetStreamError> {
        Ok(self.inner.info().await?)
    }
}
