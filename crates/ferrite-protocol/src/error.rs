use thiserror::Error;

#[derive(Debug,PartialEq, Eq, Error)]
pub enum ProtocolError {
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
}