# nats-handling

## Overview

`nats-handling` is a Rust library that provides an easy-to-use interface for interacting with NATS, an open-source messaging system. This library leverages asynchronous programming to handle NATS operations efficiently.

## Features

- Asynchronous NATS client
- Publish and subscribe to subjects
- Request-reply pattern support
- Easy message handling with custom processors
- Multiple subject handling

## Installation

Use following command in your project:

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
    client.publish("subject".to_string(), Bytes::from("Hello, NATS!")).await?;
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
    let mut subscriber = client.subscribe("subject".to_string()).await?;
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
    let response = client.request("subject".to_string(), Bytes::from("Request")).await?;
    println!("Received response: {:?}", response);
    Ok(())
}
```

### Handling

To handle messages from a subject, you need to implement the `RequestProcessor` trait and use the `handle` method of `NatsClient`.

```rust
use nats_handling::{async_trait, reply, Message, NatsClient, ReplyMessage, RequestProcessor};

#[derive(Clone, Debug)]
struct MyProcessor;

#[async_trait]
impl RequestProcessor for MyProcessor {
    async fn process(
        &self,
        message: Message,
    ) -> Result<ReplyMessage, Box<dyn std::error::Error + Send + Sync>> {
        println!("Processing message: {:?}", message);
        Ok(reply(&message, "response".into()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await.unwrap();
    let processor = MyProcessor;
    let handle = client
        .handle_multiple(vec!["subject"], processor)
        .await
        .unwrap();
    Ok(())
}

```

### Handling Multiple Subjects

You can also handle messages from multiple subjects using the `handle_multiple` method.

```rust
use nats_handling::{async_trait, reply, Message, NatsClient, ReplyMessage, RequestProcessor};

#[derive(Clone, Debug)]
struct MyProcessor;

#[async_trait]
impl RequestProcessor for MyProcessor {
    async fn process(
        &self,
        message: Message,
    ) -> Result<ReplyMessage, Box<dyn std::error::Error + Send + Sync>> {
        println!("Processing message: {:?}", message);
        Ok(reply(&message, "response".into()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await.unwrap();
    let processor = MyProcessor;
    let handle = client
        .handle_multiple(vec!["subject1", "subject2"], processor)
        .await
        .unwrap();
    Ok(())
}

```

## Plan
- [x] Nats Client implementation
- [x] Trait for processing messages
- [x] Add support for handling NATS subjects
- [ ] Add support for NATS headers
- [ ] Integrate JetStream


## License

This project is licensed under the MIT License.

