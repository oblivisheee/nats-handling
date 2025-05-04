use async_nats::jetstream::{
    self,
    consumer::{self, Config, Consumer as InnerConcumer},
};

pub struct Consumer {
    inner: InnerConcumer<Config>,
}

impl Consumer {}
