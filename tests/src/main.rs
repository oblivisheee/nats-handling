use nats_handling::jetstream::{
    config::PullConsumerConfig,
    config::{PushConsumerConfig, StreamConfig},
    Delivery, PullFetcher,
};
use nats_handling::NatsClient;
use tokio::signal::ctrl_c;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_line_number(true)
        .with_file(true)
        .init();
    let connect_options = nats_handling::ConnectOptions::new().user_and_password("authentication-service".to_owned(), "a6841f2a3c33638974568da82607ded4d3d368a3b97290b5fec113dbb2460ed8ab7a1d01df866beed62a56ac74f434e1cc0ccfde8b0cb4a4e807c9a39cb71b55".to_owned());
    let client = NatsClient::with_options(&["nats://localhost:4222"], connect_options)
        .await
        .unwrap();

    let js = client.jetstream();

    let subject = "orders".to_string();
    #[derive(Debug, Clone)]
    struct MyProcessor;
    #[async_trait::async_trait]
    impl nats_handling::jetstream::MessageProcessor for MyProcessor {
        type Error = Error;
        async fn process(
            &self,
            msg: nats_handling::jetstream::Message,
        ) -> Result<Option<nats_handling::jetstream::ReplyMessage>, Self::Error> {
            println!("Received message: {:?}", msg.payload);
            Ok(None)
        }
    }
    let stream_config = StreamConfig {
        name: "test_stream".to_string(),
        subjects: vec![subject.clone()],
        max_bytes: 1_000_000,
        ..Default::default()
    };
    let consumer_config = PullConsumerConfig {
        filter_subject: "orders".to_string(),
        durable_name: Some("test".to_string()),
        max_ack_pending: 100,
        ..Default::default()
    };
    let processor = MyProcessor;
    let handle = js
        .handle(
            Delivery::Pull((consumer_config, Box::new(MyPullFetcher))),
            stream_config,
            MyProcessor,
        )
        .await
        .unwrap();
    println!("Press Ctrl+C to exit...");
    ctrl_c().await.expect("Failed to listen for Ctrl+C");
}

#[derive(Debug, thiserror::Error)]
pub enum Error {}

struct MyPullFetcher;
impl PullFetcher for MyPullFetcher {
    fn create_stream(&self) -> std::pin::Pin<Box<dyn futures::Stream<Item = usize> + Send>> {
        Box::pin(futures::stream::unfold((), |_| async {
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            Some((100, ()))
        }))
    }
}
