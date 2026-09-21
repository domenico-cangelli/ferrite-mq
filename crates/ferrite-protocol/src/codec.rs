use crate::error::ProtocolError;
use crate::packet::{ConnectReturnCode, Packet, PacketType, ProtocolVersion, QoS};
use crate::varint::{decode_varint, encode_varint};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::str;
use tokio_util::codec::{Decoder, Encoder};
use crate::{SubAckReturnCode, SubscribeTopic};

const DEFAULT_MAX_PACKET_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct MqttCodec {
    max_packet_size: usize,
}

impl Default for MqttCodec {
    fn default() -> Self {
        Self {
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
        }
    }
}

impl MqttCodec {
    pub fn new(max_packet_size: usize) -> Self {
        Self { max_packet_size }
    }

    fn decode_string(buf: &mut BytesMut) -> Result<String, ProtocolError> {
        if buf.remaining() < 2 {
            return Err(ProtocolError::Incomplete);
        }
        let str_len = buf.get_u16() as usize;
        if buf.remaining() < str_len {
            return Err(ProtocolError::Incomplete);
        }
        let str_bytes = buf.split_to(str_len);
        let s = str::from_utf8(&str_bytes).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(s.to_string())
    }

    fn encode_string(src: &str, dst: &mut BytesMut) {
        dst.put_u16(src.len() as u16);
        dst.put_slice(src.as_bytes());
    }
}

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 2 {
            return Ok(None);
        }

        let first_byte = src[0];
        let packet_type = PacketType::try_from(first_byte)?;

        let mut varint_cursor = &src[1..];
        let (remaining_length, varint_bytes) = match decode_varint(&mut varint_cursor)? {
            Some((len, consumed)) => (len, consumed),
            None => return Ok(None),
        };

        if remaining_length > self.max_packet_size {
            return Err(ProtocolError::PacketTooLarge);
        }

        let fixed_header_len = 1 + varint_bytes;
        let total_packet_len = fixed_header_len + remaining_length;

        if src.len() < total_packet_len {
            src.reserve(total_packet_len - src.len());
            return Ok(None);
        }

        src.advance(fixed_header_len);
        let mut body = src.split_to(remaining_length);

        match packet_type {
            PacketType::Connect => {
                let proto_name = Self::decode_string(&mut body)?;
                if proto_name != "MQTT" && proto_name != "MQIsdp" {
                    return Err(ProtocolError::InvalidPacketType(0));
                }

                if body.remaining() < 4 {
                    return Err(ProtocolError::Incomplete);
                }

                let version_byte = body.get_u8();
                let protocol_version = ProtocolVersion::try_from(version_byte)?;

                let connect_flags = body.get_u8();
                let keep_alive = body.get_u16();

                let clean_session = (connect_flags & 0x02) != 0;
                let has_will = (connect_flags & 0x04) != 0;
                let has_username = (connect_flags & 0x80) != 0;
                let has_password = (connect_flags & 0x40) != 0;

                let client_id = Self::decode_string(&mut body)?;

                // Salta topic e payload del Will se presenti (espandibile nei prossimi step)
                if has_will {
                    let _will_topic = Self::decode_string(&mut body)?;
                    if body.remaining() < 2 {
                        return Err(ProtocolError::Incomplete);
                    }
                    let will_payload_len = body.get_u16() as usize;
                    if body.remaining() < will_payload_len {
                        return Err(ProtocolError::Incomplete);
                    }
                    body.advance(will_payload_len);
                }

                let username = if has_username {
                    Some(Self::decode_string(&mut body)?)
                } else {
                    None
                };

                let password = if has_password {
                    if body.remaining() < 2 {
                        return Err(ProtocolError::Incomplete);
                    }
                    let pwd_len = body.get_u16() as usize;
                    if body.remaining() < pwd_len {
                        return Err(ProtocolError::Incomplete);
                    }
                    Some(body.split_to(pwd_len).freeze())
                } else {
                    None
                };

                Ok(Some(Packet::Connect {
                    protocol_version,
                    clean_session,
                    keep_alive,
                    client_id,
                    username,
                    password,
                }))
            }

            PacketType::ConnAck => {
                if body.remaining() < 2 {
                    return Err(ProtocolError::Incomplete);
                }
                let flags = body.get_u8();
                let session_present = (flags & 0x01) != 0;
                let return_code = ConnectReturnCode::try_from(body.get_u8())?;

                Ok(Some(Packet::ConnAck {
                    session_present,
                    return_code,
                }))
            }

            PacketType::PingReq => Ok(Some(Packet::PingReq)),
            PacketType::PingResp => Ok(Some(Packet::PingResp)),
            PacketType::Disconnect => Ok(Some(Packet::Disconnect)),

            PacketType::Publish => {
                let flags = first_byte & 0x0F;
                let dup = (flags & 0x08) != 0;
                let qos_val = (flags >> 1) & 0x03;
                let retain = (flags & 0x01) != 0;
                let qos = QoS::try_from(qos_val)?;

                let topic = Self::decode_string(&mut body)?;

                let packet_id = if qos != QoS::AtMostOnce {
                    if body.remaining() < 2 {
                        return Err(ProtocolError::Incomplete);
                    }
                    Some(body.get_u16())
                } else {
                    None
                };

                let payload: Bytes = body.freeze();

                Ok(Some(Packet::Publish {
                    topic,
                    qos,
                    retain,
                    dup,
                    packet_id,
                    payload,
                }))
            }

            PacketType::Subscribe => {
                // Il variable header di SUBSCRIBE ha obbligatoriamente il Packet ID (2 byte)
                if body.remaining() < 2 {
                    return Err(ProtocolError::Incomplete);
                }
                let packet_id = body.get_u16();

                // Il payload contiene 1 o più tuple: [lunghezza_stringa + stringa + 1_byte_qos]
                let mut topics = Vec::new();
                while body.has_remaining() {
                    let filter = Self::decode_string(&mut body)?;
                    if body.remaining() < 1 {
                        return Err(ProtocolError::Incomplete);
                    }
                    let qos_byte = body.get_u8();
                    let qos = QoS::try_from(qos_byte)?;
                    topics.push(crate::packet::SubscribeTopic { filter, qos });
                }

                if topics.is_empty() {
                    // La specifica MQTT vieta un pacchetto SUBSCRIBE con payload vuoto
                    return Err(ProtocolError::Incomplete);
                }

                Ok(Some(Packet::Subscribe { packet_id, topics }))
            }

            PacketType::SubAck => {
                if body.remaining() < 2 {
                    return Err(ProtocolError::Incomplete);
                }
                let packet_id = body.get_u16();

                let mut return_codes = Vec::new();
                while body.has_remaining() {
                    let code = crate::packet::SubAckReturnCode::try_from(body.get_u8())?;
                    return_codes.push(code);
                }

                Ok(Some(Packet::SubAck { packet_id, return_codes }))
            }

            _ => Ok(None),
        }
    }
}

impl Encoder<Packet> for MqttCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::Connect {
                protocol_version,
                clean_session,
                keep_alive,
                client_id,
                username,
                password,
            } => {
                let mut body = BytesMut::new();

                // Protocol Name ("MQTT")
                Self::encode_string("MQTT", &mut body);

                // Protocol Level
                body.put_u8(protocol_version as u8);

                // Connect Flags
                let mut flags: u8 = 0;
                if clean_session {
                    flags |= 0x02;
                }
                if username.is_some() {
                    flags |= 0x80;
                }
                if password.is_some() {
                    flags |= 0x40;
                }
                body.put_u8(flags);

                // Keep Alive
                body.put_u16(keep_alive);

                // Payload: Client Identifier
                Self::encode_string(&client_id, &mut body);

                // Payload: Username
                if let Some(user) = &username {
                    Self::encode_string(user, &mut body);
                }

                // Payload: Password
                if let Some(pass) = &password {
                    body.put_u16(pass.len() as u16);
                    body.put_slice(pass);
                }

                // Fixed Header
                dst.put_u8((PacketType::Connect as u8) << 4);
                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;
                dst.put_slice(&varint_buf);
                dst.put(body);
            }

            Packet::ConnAck {
                session_present,
                return_code,
            } => {
                dst.put_u8((PacketType::ConnAck as u8) << 4);
                dst.put_u8(2); // Remaining Length per CONNACK è sempre 2 byte
                dst.put_u8(if session_present { 0x01 } else { 0x00 });
                dst.put_u8(return_code as u8);
            }

            Packet::PingReq => {
                dst.put_u8((PacketType::PingReq as u8) << 4);
                dst.put_u8(0);
            }
            Packet::PingResp => {
                dst.put_u8((PacketType::PingResp as u8) << 4);
                dst.put_u8(0);
            }
            Packet::Disconnect => {
                dst.put_u8((PacketType::Disconnect as u8) << 4);
                dst.put_u8(0);
            }

            Packet::Publish {
                topic,
                qos,
                retain,
                dup,
                packet_id,
                payload,
            } => {
                let mut first_byte = (PacketType::Publish as u8) << 4;
                if dup {
                    first_byte |= 0x08;
                }
                first_byte |= (qos as u8) << 1;
                if retain {
                    first_byte |= 0x01;
                }

                let mut remaining_len = 2 + topic.len();
                if qos != QoS::AtMostOnce {
                    remaining_len += 2;
                }
                remaining_len += payload.len();

                dst.put_u8(first_byte);
                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(remaining_len, &mut varint_buf)?;
                dst.put_slice(&varint_buf);

                Self::encode_string(&topic, dst);

                if qos != QoS::AtMostOnce {
                    let id = packet_id.ok_or(ProtocolError::Incomplete)?;
                    dst.put_u16(id);
                }

                dst.put_slice(&payload);
            }

            Packet::Subscribe { packet_id, topics } => {
                // Fixed header flag per SUBSCRIBE: i bit 3..0 devono essere rigorosamente 0010 (0x02)
                let first_byte = ((PacketType::Subscribe as u8) << 4) | 0x02;

                let mut body = BytesMut::new();
                body.put_u16(packet_id);

                for topic in topics {
                    Self::encode_string(&topic.filter, &mut body);
                    body.put_u8(topic.qos as u8);
                }

                dst.put_u8(first_byte);
                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;
                dst.put_slice(&varint_buf);
                dst.put(body);
            }

            Packet::SubAck { packet_id, return_codes } => {
                let first_byte = (PacketType::SubAck as u8) << 4;

                let mut body = BytesMut::new();
                body.put_u16(packet_id);

                for code in return_codes {
                    body.put_u8(code as u8);
                }

                dst.put_u8(first_byte);
                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;
                dst.put_slice(&varint_buf);
                dst.put(body);
            }



        }
        Ok(())
    }
}

#[test]
fn test_connect_and_connack_roundtrip() {
    let mut codec = MqttCodec::default();
    let mut buffer = BytesMut::new();

    let connect = Packet::Connect {
        protocol_version: ProtocolVersion::Mqtt311,
        clean_session: true,
        keep_alive: 60,
        client_id: "ferrite-client-01".to_string(),
        username: Some("admin".to_string()),
        password: Some(Bytes::from_static(b"secret")),
    };

    codec.encode(connect.clone(), &mut buffer).unwrap();
    let decoded = codec.decode(&mut buffer).unwrap();
    assert_eq!(decoded, Some(connect));
    assert!(buffer.is_empty());

    let connack = Packet::ConnAck {
        session_present: false,
        return_code: ConnectReturnCode::Accepted,
    };

    codec.encode(connack.clone(), &mut buffer).unwrap();
    let decoded_ack = codec.decode(&mut buffer).unwrap();
    assert_eq!(decoded_ack, Some(connack));
    assert!(buffer.is_empty());
}

#[test]
fn test_subscribe_and_suback_roundtrip() {
    let mut codec = MqttCodec::default();
    let mut buffer = BytesMut::new();

    let subscribe = Packet::Subscribe {
        packet_id: 101,
        topics: vec![
            SubscribeTopic {
                filter: "sensors/+/temperature".to_string(),
                qos: QoS::AtLeastOnce,
            },
            SubscribeTopic {
                filter: "alarms/#".to_string(),
                qos: QoS::AtMostOnce,
            },
        ],
    };

    codec.encode(subscribe.clone(), &mut buffer).unwrap();
    let decoded = codec.decode(&mut buffer).unwrap();
    assert_eq!(decoded, Some(subscribe));
    assert!(buffer.is_empty());

    let suback = Packet::SubAck {
        packet_id: 101,
        return_codes: vec![SubAckReturnCode::SuccessQoS1, SubAckReturnCode::SuccessQoS0],
    };

    codec.encode(suback.clone(), &mut buffer).unwrap();
    let decoded_ack = codec.decode(&mut buffer).unwrap();
    assert_eq!(decoded_ack, Some(suback));
    assert!(buffer.is_empty());
}