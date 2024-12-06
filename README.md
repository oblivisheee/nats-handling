# async-nats-easy

## Overview

`async-nats-easy` is a Rust library that provides an easy-to-use interface for interacting with NATS, an open-source messaging system. This library leverages asynchronous programming to handle NATS operations efficiently.

## Features

- Asynchronous NATS client
- Publish and subscribe to subjects
- Request-reply pattern support
- Easy message handling with custom processors
- Multiple subject handling

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
async-nats-easy = "0.1.0"
```

## Usage

### Connecting to NATS

```rust
use async_nats_easy::NatsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    Ok(())
}
```

### Publishing a Message

```rust
use async_nats_easy::NatsClient;
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
use async_nats_easy::NatsClient;

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
use async_nats_easy::NatsClient;
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await?;
    let response = client.request("subject".to_string(), Bytes::from("Request")).await?;
    println!("Received response: {:?}", response);
    Ok(())
}
```

## License

This project is licensed under the MIT License.