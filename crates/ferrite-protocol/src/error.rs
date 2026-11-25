use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Malformed remaining length: exceeds 4-byte MQTT specification limit")]
    MalformedRemainingLength,

    #[error("Unknown or unsupported packet type: {0}")]
    InvalidPacketType(u8),

    #[error("Invalid QoS level: {0}")]
    InvalidQoS(u8),

    #[error("Incomplete buffer: requires additional bytes to complete parsing")]
    Incomplete,

    #[error("Malformed UTF-8 string payload")]
    InvalidUtf8,

    #[error("Packet exceeds maximum allowed size")]
    PacketTooLarge,

    #[error("Invalid packet identifier: 0 is forbidden by MQTT specification")]
    InvalidPacketId,

    #[error("Unsupported MQTT protocol level: {0}")]
    UnsupportedProtocolLevel(u8),
}
