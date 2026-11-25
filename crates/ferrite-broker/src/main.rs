use ferrite_broker_core::run_server;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ferrite_broker=info".into()),
        )
        .init();

    let bind_addr = "0.0.0.0:1883";
    let listener = TcpListener::bind(bind_addr).await?;
    info!("🚀 FerriteMQ broker running on {}", bind_addr);

    run_server(listener).await?;
    Ok(())
}