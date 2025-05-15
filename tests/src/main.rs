use async_trait::async_trait;
use nats_handling::jetstream::{
    config::{PushConsumerConfig, StreamConfig},
    Delivery,
};
use nats_handling::NatsClient;
use std::time::Duration;
use thiserror::Error;
use tokio::signal::ctrl_c;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_line_number(true)
        .with_file(true)
        .init();

    let connect_options = nats_handling::ConnectOptions::new()
        .user_and_password(
            "accounts-service".to_owned(),
            "fffa612a5f0d4770664e2fcdec08d26b13d288dc7666f628c4044bd6b32f2ffc1013c93915190c9738f597b6e6233b399b2c8bea4439b0b8de93abde384a07c7".to_owned(),
        )
        .add_client_certificate(
            "../../knotos-backend/certs/nats/client/accounts/client.pem".into(),
            "../../knotos-backend/certs/nats/client/accounts/client.key".into(),
        )
        .add_root_certificates("../../knotos-backend/certs/nats/ca/ca.pem".into());

    let client = NatsClient::with_options(&["127.0.0.1:4222"], connect_options)
        .await
        .expect("Failed to connect to NATS");

    let js = client.jetstream();

    let subject = "orders".to_string();

    #[derive(Debug, Clone)]
    struct MyProcessor;

    #[async_trait]
    impl nats_handling::jetstream::MessageProcessor for MyProcessor {
        type Error = Error;

        async fn process(&self, msg: nats_handling::jetstream::Message) -> Result<(), Self::Error> {
            println!("Received message: {:?}", msg.payload);
            Ok(())
        }
    }

    let stream_config = StreamConfig {
        name: "test_stream".to_string(),
        subjects: vec![subject.clone()],
        max_bytes: 1_000_000,
        no_ack: true,
        ..Default::default()
    };

    let consumer_config = PushConsumerConfig {
        deliver_subject: "test-consumer".to_string(),
        filter_subject: subject.clone(),
        durable_name: Some("test-consumer".to_string()),
        max_ack_pending: 100,
        ..Default::default()
    };

    let processor = MyProcessor;

    let handle = js
        .handle(Delivery::Push(consumer_config), stream_config, processor)
        .await
        .expect("Failed to start JetStream consumer");

    println!("Press Ctrl+C to exit...");
    ctrl_c().await.expect("Failed to listen for Ctrl+C");
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Processing error")]
    ProcessingError,
}
