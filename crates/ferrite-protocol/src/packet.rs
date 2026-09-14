use crate::error::ProtocolError;
use bytes::Bytes;
use crate::QoS::{AtLeastOnce, AtMostOnce, ExactlyOnce};

/// MQTT Quality of Service levels according to OASIS specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QoS {
    /// QoS 0: At most once delivery (Fire and forget)
    AtMostOnce = 0,
    /// QoS 1: At least once delivery (Acknowledged delivery)
    AtLeastOnce = 1,
    /// QoS 2: Exactly once delivery (Assured delivery via 4-step handshake)
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AtMostOnce),
            1 => Ok(AtLeastOnce),
            2 => Ok(ExactlyOnce),
            other => Err(ProtocolError::InvalidQoS(other))
        }
    }
}

/// MQTT Control Packet types (encoded in the upper 4 bits of the first fixed header byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5,
    PubRel = 6,
    PubComp = 7,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
    Auth = 15,
}

impl TryFrom<u8> for PacketType {
    type Error = ProtocolError;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        let nibble = byte >> 4;
        match nibble {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::ConnAck),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::PubAck),
            5 => Ok(PacketType::PubRec),
            6 => Ok(PacketType::PubRel),
            7 => Ok(PacketType::PubComp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::SubAck),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::UnsubAck),
            12 => Ok(PacketType::PingReq),
            13 => Ok(PacketType::PingResp),
            14 => Ok(PacketType::Disconnect),
            15 => Ok(PacketType::Auth),
            other => Err(ProtocolError::InvalidPacketType(other)),
        }
    }
}

/// High-level strongly-typed MQTT packets.
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    PingReq,
    PingResp,
    Disconnect,
    Publish {
        topic: String,
        qos: QoS,
        retain: bool,
        dup: bool,
        packet_id: Option<u16>,
        payload: Bytes,
    },
}