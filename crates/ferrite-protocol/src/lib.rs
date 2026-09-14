pub mod error;
pub mod packet;
pub mod varint;
pub mod codec;

// Ri-esportiamo i tipi principali per renderli accessibili comodamente dall'esterno
pub use error::ProtocolError;
pub use packet::{Packet, PacketType, QoS};
pub use codec::MqttCodec;