use ferrite_broker_core::run_server;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:1883";

/// Versione provvisoria per TDD: ignora ancora il valore fornito.
fn parse_bind_addr(_raw: Option<String>) -> Result<SocketAddr, String> {
    DEFAULT_BIND_ADDR
        .parse()
        .map_err(|e| format!("invalid default bind address: {e}"))
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
