use async_nats_easy::{async_trait, reply, Message, NatsClient, ReplyMessage, RequestProcessor};
use bytes::Bytes;

#[derive(Clone, Copy, Debug)]
struct MyProcessor;

#[async_trait]
impl RequestProcessor for MyProcessor {
    async fn process(
        &self,
        message: Message,
    ) -> Result<Option<ReplyMessage>, Box<dyn std::error::Error + Send + Sync>> {
        println!("Processing message: {:?}", message);
        Ok(Some(reply(message, Bytes::from("Hello, World!"))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = NatsClient::new(&["nats://127.0.0.1:4222"]).await.unwrap();
    let processor = MyProcessor;
    let handle = client.handle("subject", processor).await.unwrap();

    // Prevent the program from exiting
    tokio::signal::ctrl_c().await?;
    println!("Shutting down gracefully");

    Ok(())
}
