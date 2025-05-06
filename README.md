# nats-handling

[![License](https://badgen.net/github/license/oblivisheee/nats-handling)](https://github.com/oblivisheee/nats-handling/blob/main/LICENSE)
[![Crates.io](https://badgen.net/crates/v/nats-handling)](https://crates.io/crates/nats-handling)
![Downloads](https://badgen.net/crates/d/nats-handling)

## Overview

`nats-handling` is a Rust library designed for seamless NATS message handling. It provides a straightforward API for subscribing to NATS subjects, processing messages, and sending replies. The library aims to offer an experience similar to HTTP handling, but tailored for NATS.

## Features

- Asynchronous NATS client
- Publish and subscribe to subjects
- Request-reply pattern support
- Easy message handling with custom processors
- Multiple subject handling
- JetStream support (optional feature)

## Installation

Add the library to your project using:

```console
cargo add nats-handling
```

## Usage

### Connecting to NATS

```rust
use nats_handling::NatsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    Ok(())
}
```

### Publishing a Message

```rust
use nats_handling::NatsClient;
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    client.publish("subject", Bytes::from("Hello, NATS!")).await?;
    Ok(())
}
```

### Subscribing to a Subject

```rust
use nats_handling::NatsClient;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    let mut subscriber = client.subscribe("subject").await?;
    while let Some(message) = subscriber.next().await {
        println!("Received message: {:?}", message);
    }
    Ok(())
}
```

### Request-Reply Pattern

```rust
use nats_handling::NatsClient;
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    let response = client.request("subject", Bytes::from("Request")).await?;
    println!("Received response: {:?}", response);
    Ok(())
}
```

### Handling Messages

To handle messages from a subject, implement the `MessageProcessor` trait and use the `handle` method of `NatsClient`.

```rust
use nats_handling::{reply, Message, NatsClient, ReplyMessage, MessageProcessor};
use async_trait::async_trait;

#[derive(Clone, Debug)]
struct MyProcessor;

#[async_trait]
impl MessageProcessor for MyProcessor {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn process(
        &self,
        message: Message,
    ) -> Result<Option<ReplyMessage>, Self::Error> {
        println!("Processing message: {:?}", message);
        Ok(Some(reply(&message, Bytes::from("response"))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    let processor = MyProcessor;
    let handle = client.handle("subject", processor).await?;
    Ok(())
}
```

### Handling Multiple Subjects

You can handle messages from multiple subjects using the `handle_multiple` method.

```rust
use nats_handling::{reply, Message, NatsClient, ReplyMessage, MessageProcessor};
use async_trait::async_trait;

#[derive(Clone, Debug)]
struct MyProcessor;

#[async_trait]
impl MessageProcessor for MyProcessor {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn process(
        &self,
        message: Message,
    ) -> Result<Option<ReplyMessage>, Self::Error> {
        println!("Processing message: {:?}", message);
        Ok(Some(reply(&message, Bytes::from("response"))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    let processor = MyProcessor;
    let handle = client
        .handle_multiple(vec!["subject1".to_string(), "subject2".to_string()], processor)
        .await?;
    Ok(())
}
```

### JetStream Support

#### Push-Based Consumers

Push-based consumers allow JetStream to deliver messages to your application as they arrive. You can use the `Handle::push` method to process messages with a custom `MessageProcessor`.

```rust
use nats_handling::jetstream::{JetStream, MessageProcessor, ReplyMessage, Message};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Clone, Debug)]
struct MyProcessor;

#[async_trait]
impl MessageProcessor for MyProcessor {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn process(
        &self,
        message: Message,
    ) -> Result<Option<ReplyMessage>, Self::Error> {
        println!("Processing message: {:?}", message);
        Ok(Some(message.reply(Bytes::from("response"))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = nats_handling::NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    let jetstream = JetStream::new(client);
    let consumer_config = nats_handling::config::PushConsumerConfig::default();
    let processor = MyProcessor;

    let handle = jetstream
        .handle(
            nats_handling::Delivery::Push(consumer_config),
            nats_handling::config::StreamConfig::default(),
            processor,
        )
        .await?;
    Ok(())
}
```

#### Pull-Based Consumers

Pull-based consumers allow your application to fetch messages on demand. Use the `Handle::pull` method to process messages with a custom `MessageProcessor` and a `PullFetcher`.

```rust
use nats_handling::jetstream::{JetStream, MessageProcessor, ReplyMessage, Message, PullFetcher};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;

#[derive(Clone, Debug)]
struct MyProcessor;

#[async_trait]
impl MessageProcessor for MyProcessor {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn process(
        &self,
        message: Message,
    ) -> Result<Option<ReplyMessage>, Self::Error> {
        println!("Processing message: {:?}", message);
        Ok(Some(message.reply(Bytes::from("response"))))
    }
}

struct MyPullFetcher;

impl PullFetcher for MyPullFetcher {
    fn create_stream(&self) -> std::pin::Pin<Box<dyn futures::Stream<Item = usize> + Send>> {
        Box::pin(stream::iter(vec![10, 20, 30]))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = nats_handling::NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    let jetstream = JetStream::new(client);
    let consumer_config = nats_handling::config::PullConsumerConfig::default();
    let fetcher = MyPullFetcher;
    let processor = MyProcessor;

    let handle = jetstream
        .handle(
            nats_handling::Delivery::Pull((consumer_config, Box::new(fetcher))),
            nats_handling::config::StreamConfig::default(),
            processor,
        )
        .await?;
    Ok(())
}
```

JetStream support in `nats-handling` makes it easy to build robust and scalable message-driven applications with advanced features like message durability and replay.

## License

This project is licensed under the MIT License.
