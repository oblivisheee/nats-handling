use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message(pub(crate) async_nats::Message);

impl std::ops::Deref for Message {
    type Target = async_nats::Message;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Message {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Message {
    pub fn reply(&self, payload: Bytes) -> ReplyMessage {
        ReplyMessage {
            subject: self.reply.clone().unwrap_or_else(|| "".into()).to_string(),
            payload,
            headers: None,
            reply: None,
        }
    }
}

/// A trait that defines a request processor for NATS messages
#[async_trait]
pub trait MessageProcessor: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn process(&self, message: Message) -> Result<Option<ReplyMessage>, Self::Error>;
}

/// A structure that represents a reply message
#[derive(Clone, Debug)]
pub struct ReplyMessage {
    pub reply: Option<String>,
    pub subject: String,
    pub payload: Bytes,
    pub headers: Option<HeaderMap>,
}
impl ReplyMessage {
    pub fn new(subject: String, payload: Bytes) -> Self {
        Self {
            reply: None,
            subject,
            payload,
            headers: None,
        }
    }
}
//TODO: Fix sending custom PublishMessage natively via method
// impl Into<async_nats::PublishMessage> for ReplyMessage {
//     fn into(self) -> async_nats::PublishMessage {
//         async_nats::PublishMessage {
//             subject: self.subject.into(),
//             payload: self.payload,
//             reply: self.reply.map(|s| s.into()),
//             headers: self.headers,
//         }
//     }
// }

/// A structure that represents an error reply message
pub(crate) struct ReplyErrorMessage(pub Box<dyn std::error::Error + Send + Sync>);

/// An easy-to-use function that creates a reply message
pub fn reply(msg: &Message, payload: Bytes) -> ReplyMessage {
    ReplyMessage {
        subject: msg.reply.clone().unwrap_or_else(|| "".into()).to_string(),
        payload,
        headers: None,
        reply: None,
    }
}
