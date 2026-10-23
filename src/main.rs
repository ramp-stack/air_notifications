#[tokio::main]
async fn main() { air_notifications::server::NotificationServer::start().await; }
