use ferrite_broker_core::run_server;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{info, warn};

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:1883";

fn parse_bind_addr(raw: Option<String>) -> Result<SocketAddr, String> {
    let value = raw.unwrap_or_else(|| DEFAULT_BIND_ADDR.to_string());
    value
        .parse::<SocketAddr>()
        .map_err(|e| format!("Invalid FERRITE_BIND_ADDR {value:?}: {e}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ferrite_broker=info,ferrite_broker_core=info".into()),
        )
        .init();

    let bind_addr = parse_bind_addr(std::env::var("FERRITE_BIND_ADDR").ok())?;
    if !bind_addr.ip().is_loopback() {
        warn!(
            "Binding on non-loopback address {}: broker accepts network connections without authentication yet (planned for Phase 6)",
            bind_addr
        );
    }

    let listener = TcpListener::bind(bind_addr).await?;
    info!("🚀 FerriteMQ broker running on {}", bind_addr);

    run_server(listener).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bind_addr_default_when_absent() {
        let addr = parse_bind_addr(None).unwrap();
        assert_eq!(addr, "0.0.0.0:1883".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_parse_bind_addr_valid_custom() {
        let addr = parse_bind_addr(Some("127.0.0.1:1884".to_string())).unwrap();
        assert_eq!(addr, "127.0.0.1:1884".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_parse_bind_addr_invalid_returns_error() {
        let err = parse_bind_addr(Some("not-an-addr".to_string()));
        assert!(err.is_err());
    }
}
