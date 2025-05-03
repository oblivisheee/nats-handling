use nats_handling::{reply, NatsClient};
use tokio::signal::ctrl_c;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_line_number(true)
        .with_file(true)
        .init();
    let client = NatsClient::new(&["nats://localhost:4222"]).await.unwrap();
    let subject = "test_subject".to_string();
    #[derive(Debug, Clone)]
    struct MyProcessor;
    #[async_trait::async_trait]
    impl nats_handling::RequestProcessor for MyProcessor {
        async fn process(
            &self,
            msg: nats_handling::Message,
        ) -> Result<nats_handling::ReplyMessage, nats_handling::Error> {
            println!("Received message: {:?}", msg);
            Ok(reply(&msg, b"yopta".to_vec().into()))
        }
    }

    let processor = MyProcessor;
    let handle = client.handle(&subject, processor).await.unwrap();
    println!("Press Ctrl+C to exit...");
    ctrl_c().await.expect("Failed to listen for Ctrl+C");
    handle.shutdown().await;
}
