use ferrite_protocol::ProtocolError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Network I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Connection rejected by broker with return code: {0:?}")]
    ConnectionRefused(ferrite_protocol::ConnectReturnCode),

    #[error("Client channel disconnected or closed unexpectedly")]
    ChannelClosed,

    #[error("Connection closed by peer")]
    ConnectionClosed,
}