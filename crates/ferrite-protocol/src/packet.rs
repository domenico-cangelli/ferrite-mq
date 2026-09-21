use crate::error::ProtocolError;
use bytes::Bytes;
use crate::QoS::{AtLeastOnce, AtMostOnce, ExactlyOnce};

/// MQTT Quality of Service levels according to OASIS specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// MQTT Protocol Version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProtocolVersion {
    Mqtt311 = 4,
    Mqtt50 = 5,
}

impl TryFrom<u8> for ProtocolVersion {
    type Error = ProtocolError;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            4 => Ok(ProtocolVersion::Mqtt311),
            5 => Ok(ProtocolVersion::Mqtt50),
            _ => Err(ProtocolError::InvalidPacketType(val)),
        }
    }
}

/// Return code for CONNACK (MQTT 3.1.1)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Accepted = 0,
    RefusedUnacceptableProtocolVersion = 1,
    RefusedIdentifierRejected = 2,
    RefusedServerUnavailable = 3,
    RefusedBadUsernameOrPassword = 4,
    RefusedNotAuthorized = 5,
}

impl TryFrom<u8> for ConnectReturnCode {
    type Error = ProtocolError;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(ConnectReturnCode::Accepted),
            1 => Ok(ConnectReturnCode::RefusedUnacceptableProtocolVersion),
            2 => Ok(ConnectReturnCode::RefusedIdentifierRejected),
            3 => Ok(ConnectReturnCode::RefusedServerUnavailable),
            4 => Ok(ConnectReturnCode::RefusedBadUsernameOrPassword),
            5 => Ok(ConnectReturnCode::RefusedNotAuthorized),
            other => Err(ProtocolError::InvalidPacketType(other)),
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
    Connect {
        protocol_version: ProtocolVersion,
        clean_session: bool,
        keep_alive: u16,
        client_id: String,
        username: Option<String>,
        password: Option<Bytes>,
    },
    ConnAck {
        session_present: bool,
        return_code: ConnectReturnCode,
    },
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