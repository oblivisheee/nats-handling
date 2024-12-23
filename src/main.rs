use async_nats_easy::{async_trait, reply, Message, NatsClient, ReplyMessage, RequestProcessor};
use bytes::Bytes;

#[derive(Clone, Copy, Debug)]
struct MyProcessor;

#[async_trait]
impl RequestProcessor for MyProcessor {
    async fn process(
        &self,
        message: Message,
    ) -> Result<ReplyMessage, Box<dyn std::error::Error + Send + Sync>> {
        Ok(reply(&message, "yopta_blyat".into()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await.unwrap();
    let processor = MyProcessor;
    let handle = client.handle("subject", processor).await.unwrap();

    // Prevent the program from exiting
    tokio::signal::ctrl_c().await?;
    handle.close().await.unwrap();
    println!("Shutting down gracefully");

    Ok(())
}
