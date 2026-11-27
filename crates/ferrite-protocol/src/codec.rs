use crate::error::ProtocolError;
use crate::packet::{ConnectReturnCode, Packet, PacketType, ProtocolVersion, QoS, Will};
use crate::varint::{decode_varint, encode_varint};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::str;
use tokio_util::codec::{Decoder, Encoder};

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
            return Err(ProtocolError::MalformedPacket("truncated string length"));
        }
        let str_len = buf.get_u16() as usize;
        if buf.remaining() < str_len {
            return Err(ProtocolError::MalformedPacket("truncated string"));
        }
        let str_bytes = buf.split_to(str_len);
        let s = str::from_utf8(&str_bytes).map_err(|_| ProtocolError::InvalidUtf8)?;
        // MQTT-1.5.3-2: le stringhe UTF-8 MQTT non possono contenere U+0000.
        if s.contains('\0') {
            return Err(ProtocolError::InvalidUtf8);
        }
        Ok(s.to_string())
    }

    fn ensure_empty(body: &BytesMut) -> Result<(), ProtocolError> {
        if body.has_remaining() {
            return Err(ProtocolError::MalformedPacket("trailing bytes"));
        }
        Ok(())
    }

    fn encode_bytes(dst: &mut BytesMut, src: &[u8]) -> Result<(), ProtocolError> {
        let len = u16::try_from(src.len()).map_err(|_| ProtocolError::StringTooLong(src.len()))?;
        dst.put_u16(len);
        dst.put_slice(src);
        Ok(())
    }

    fn encode_str(dst: &mut BytesMut, src: &str) -> Result<(), ProtocolError> {
        Self::encode_bytes(dst, src.as_bytes())
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
        let flags = first_byte & 0x0F;

        // MQTT-2.2.2-2: verifica dei flag riservati del fixed header per ogni tipo di pacchetto.
        match packet_type {
            PacketType::Connect
            | PacketType::ConnAck
            | PacketType::PubAck
            | PacketType::SubAck
            | PacketType::UnsubAck
            | PacketType::PingReq
            | PacketType::PingResp
            | PacketType::Disconnect => {
                if flags != 0x00 {
                    return Err(ProtocolError::InvalidFixedHeaderFlags {
                        packet_type: packet_type as u8,
                        flags,
                    });
                }
            }
            PacketType::Subscribe | PacketType::Unsubscribe => {
                if flags != 0x02 {
                    return Err(ProtocolError::InvalidFixedHeaderFlags {
                        packet_type: packet_type as u8,
                        flags,
                    });
                }
            }
            PacketType::Publish
            | PacketType::PubRec
            | PacketType::PubRel
            | PacketType::PubComp
            | PacketType::Auth => {}
        }

        match packet_type {
            PacketType::Connect => {
                let proto_name = Self::decode_string(&mut body)?;
                if proto_name != "MQTT" {
                    return Err(ProtocolError::InvalidProtocolName);
                }

                if body.remaining() < 1 {
                    return Err(ProtocolError::MalformedPacket("truncated connect header"));
                }

                let version_byte = body.get_u8();
                if version_byte != ProtocolVersion::Mqtt311 as u8 {
                    return Err(ProtocolError::UnsupportedProtocolLevel(version_byte));
                }
                let protocol_version = ProtocolVersion::Mqtt311;

                if body.remaining() < 3 {
                    return Err(ProtocolError::MalformedPacket("truncated connect header"));
                }

                let connect_flags = body.get_u8();
                let keep_alive = body.get_u16();

                // MQTT-3.1.2-3: il bit 0 dei connect flags è riservato e deve essere 0.
                if (connect_flags & 0x01) != 0 {
                    return Err(ProtocolError::MalformedPacket("reserved connect flag"));
                }

                let clean_session = (connect_flags & 0x02) != 0;
                let has_will = (connect_flags & 0x04) != 0;
                let will_qos_raw = (connect_flags >> 3) & 0x03;
                let will_retain = (connect_flags & 0x20) != 0;
                let has_username = (connect_flags & 0x80) != 0;
                let has_password = (connect_flags & 0x40) != 0;

                // MQTT-3.1.2-11 / MQTT-3.1.2-15: senza Will Flag, Will QoS e Will Retain devono essere 0.
                if !has_will && (will_qos_raw != 0 || will_retain) {
                    return Err(ProtocolError::MalformedPacket(
                        "will qos or retain set without will flag",
                    ));
                }

                // MQTT-3.1.2-22: Password Flag = 1 richiede Username Flag = 1.
                if has_password && !has_username {
                    return Err(ProtocolError::MalformedPacket(
                        "password flag set without username flag",
                    ));
                }

                let will_qos = if has_will {
                    Some(QoS::try_from(will_qos_raw)?)
                } else {
                    None
                };

                let client_id = Self::decode_string(&mut body)?;

                // Will: topic + payload nel payload del CONNECT; QoS (bit 3-4) e retain (bit 5)
                // stanno invece nei connect flags.
                let will = if let Some(qos) = will_qos {
                    let will_topic = Self::decode_string(&mut body)?;
                    if body.remaining() < 2 {
                        return Err(ProtocolError::MalformedPacket("truncated will payload"));
                    }
                    let will_payload_len = body.get_u16() as usize;
                    if body.remaining() < will_payload_len {
                        return Err(ProtocolError::MalformedPacket("truncated will payload"));
                    }
                    let will_payload = body.split_to(will_payload_len).freeze();
                    Some(Will {
                        topic: will_topic,
                        payload: will_payload,
                        qos,
                        retain: will_retain,
                    })
                } else {
                    None
                };

                let username = if has_username {
                    Some(Self::decode_string(&mut body)?)
                } else {
                    None
                };

                let password = if has_password {
                    if body.remaining() < 2 {
                        return Err(ProtocolError::MalformedPacket("truncated password"));
                    }
                    let pwd_len = body.get_u16() as usize;
                    if body.remaining() < pwd_len {
                        return Err(ProtocolError::MalformedPacket("truncated password"));
                    }
                    Some(body.split_to(pwd_len).freeze())
                } else {
                    None
                };

                Self::ensure_empty(&body)?;

                Ok(Some(Packet::Connect {
                    protocol_version,
                    clean_session,
                    keep_alive,
                    client_id,
                    will,
                    username,
                    password,
                }))
            }

            PacketType::ConnAck => {
                if body.remaining() < 2 {
                    return Err(ProtocolError::MalformedPacket("truncated connack"));
                }
                let flags = body.get_u8();
                let session_present = (flags & 0x01) != 0;
                let return_code = ConnectReturnCode::try_from(body.get_u8())?;
                Self::ensure_empty(&body)?;

                Ok(Some(Packet::ConnAck {
                    session_present,
                    return_code,
                }))
            }

            PacketType::PingReq => {
                Self::ensure_empty(&body)?;
                Ok(Some(Packet::PingReq))
            }
            PacketType::PingResp => {
                Self::ensure_empty(&body)?;
                Ok(Some(Packet::PingResp))
            }
            PacketType::Disconnect => {
                Self::ensure_empty(&body)?;
                Ok(Some(Packet::Disconnect))
            }

            PacketType::Publish => {
                let dup = (flags & 0x08) != 0;
                let qos_val = (flags >> 1) & 0x03;
                let retain = (flags & 0x01) != 0;
                let qos = QoS::try_from(qos_val)?;

                // MQTT-3.3.1-2: il flag DUP deve essere 0 per tutti i PUBLISH QoS 0.
                if qos == QoS::AtMostOnce && dup {
                    return Err(ProtocolError::InvalidFixedHeaderFlags {
                        packet_type: packet_type as u8,
                        flags,
                    });
                }

                let topic = Self::decode_string(&mut body)?;
                if topic.is_empty() {
                    // MQTT-4.7.3-1: il Topic Name di un PUBLISH non può essere vuoto.
                    return Err(ProtocolError::MalformedPacket("empty topic"));
                }

                let packet_id = if qos != QoS::AtMostOnce {
                    if body.remaining() < 2 {
                        return Err(ProtocolError::MalformedPacket("truncated packet id"));
                    }
                    let id = body.get_u16();
                    if id == 0 {
                        return Err(ProtocolError::InvalidPacketId);
                    }
                    Some(id)
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
                    return Err(ProtocolError::MalformedPacket("truncated packet id"));
                }
                let packet_id = body.get_u16();
                if packet_id == 0 {
                    return Err(ProtocolError::InvalidPacketId);
                }

                // Il payload contiene 1 o più tuple: [lunghezza_stringa + stringa + 1_byte_qos]
                let mut topics = Vec::new();
                while body.has_remaining() {
                    let filter = Self::decode_string(&mut body)?;
                    if body.remaining() < 1 {
                        return Err(ProtocolError::MalformedPacket("truncated subscribe qos"));
                    }
                    let qos_byte = body.get_u8();
                    let qos = QoS::try_from(qos_byte)?;
                    topics.push(crate::packet::SubscribeTopic { filter, qos });
                }

                if topics.is_empty() {
                    // La specifica MQTT vieta un pacchetto SUBSCRIBE con payload vuoto
                    return Err(ProtocolError::MalformedPacket("empty subscribe payload"));
                }

                Self::ensure_empty(&body)?;

                Ok(Some(Packet::Subscribe { packet_id, topics }))
            }

            PacketType::SubAck => {
                if body.remaining() < 2 {
                    return Err(ProtocolError::MalformedPacket("truncated packet id"));
                }
                let packet_id = body.get_u16();
                if packet_id == 0 {
                    return Err(ProtocolError::InvalidPacketId);
                }

                let mut return_codes = Vec::new();
                while body.has_remaining() {
                    let code = crate::packet::SubAckReturnCode::try_from(body.get_u8())?;
                    return_codes.push(code);
                }

                Self::ensure_empty(&body)?;

                Ok(Some(Packet::SubAck {
                    packet_id,
                    return_codes,
                }))
            }

            PacketType::PubAck => {
                // PUBACK deve contenere esattamente il packet_id a 2 byte
                if body.remaining() < 2 {
                    return Err(ProtocolError::MalformedPacket("truncated packet id"));
                }
                let packet_id = body.get_u16();
                if packet_id == 0 {
                    return Err(ProtocolError::InvalidPacketId);
                }
                Self::ensure_empty(&body)?;
                Ok(Some(Packet::PubAck { packet_id }))
            }

            PacketType::Unsubscribe => {
                if body.remaining() < 2 {
                    return Err(ProtocolError::MalformedPacket("truncated packet id"));
                }
                let packet_id = body.get_u16();
                if packet_id == 0 {
                    return Err(ProtocolError::InvalidPacketId);
                }

                let mut filters = Vec::new();
                while body.has_remaining() {
                    filters.push(Self::decode_string(&mut body)?);
                }
                if filters.is_empty() {
                    // MQTT-3.10.3-2: UNSUBSCRIBE deve contenere almeno un topic filter.
                    return Err(ProtocolError::MalformedPacket("empty unsubscribe payload"));
                }

                Self::ensure_empty(&body)?;

                Ok(Some(Packet::Unsubscribe { packet_id, filters }))
            }

            PacketType::UnsubAck => {
                if body.remaining() < 2 {
                    return Err(ProtocolError::MalformedPacket("truncated packet id"));
                }
                let packet_id = body.get_u16();
                if packet_id == 0 {
                    return Err(ProtocolError::InvalidPacketId);
                }
                Self::ensure_empty(&body)?;
                Ok(Some(Packet::UnsubAck { packet_id }))
            }

            // Pacchetti previsti da MQTT ma non ancora implementati (QoS 2, AUTH).
            // I loro byte sono già stati consumati dal buffer: restituire `Ok(None)` li
            // farebbe sparire in silenzio, quindi segnaliamo un errore esplicito.
            PacketType::PubRec | PacketType::PubRel | PacketType::PubComp | PacketType::Auth => {
                Err(ProtocolError::InvalidPacketType(packet_type as u8))
            }
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
                will,
                username,
                password,
            } => {
                if protocol_version != ProtocolVersion::Mqtt311 {
                    return Err(ProtocolError::UnsupportedProtocolLevel(
                        protocol_version as u8,
                    ));
                }

                let mut body = BytesMut::new();

                // Protocol Name ("MQTT")
                Self::encode_str(&mut body, "MQTT")?;

                // Protocol Level
                body.put_u8(protocol_version as u8);

                // Connect Flags
                let mut flags: u8 = 0;
                if clean_session {
                    flags |= 0x02;
                }
                if let Some(will) = &will {
                    flags |= 0x04; // Will Flag
                    flags |= (will.qos as u8) << 3; // Will QoS (bit 3-4)
                    if will.retain {
                        flags |= 0x20; // Will Retain
                    }
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
                Self::encode_str(&mut body, &client_id)?;

                // Payload: Will Topic + Will Payload (nell'ordine imposto dalla spec)
                if let Some(will) = &will {
                    Self::encode_str(&mut body, &will.topic)?;
                    Self::encode_bytes(&mut body, &will.payload)?;
                }

                // Payload: Username
                if let Some(user) = &username {
                    Self::encode_str(&mut body, user)?;
                }

                // Payload: Password
                if let Some(pass) = &password {
                    Self::encode_bytes(&mut body, pass)?;
                }

                // Tutto validato: codifichiamo il varint prima di toccare `dst`.
                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;

                // Scrittura atomica in `dst`
                dst.put_u8((PacketType::Connect as u8) << 4);
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
                let id_for_qos = if qos != QoS::AtMostOnce {
                    Some(packet_id.ok_or(ProtocolError::MalformedPacket("missing packet id"))?)
                } else {
                    None
                };

                let mut body = BytesMut::with_capacity(2 + topic.len() + 2 + payload.len());
                Self::encode_str(&mut body, &topic)?;
                if let Some(id) = id_for_qos {
                    body.put_u16(id);
                }
                body.put_slice(&payload);

                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;

                let mut first_byte = (PacketType::Publish as u8) << 4;
                if dup {
                    first_byte |= 0x08;
                }
                first_byte |= (qos as u8) << 1;
                if retain {
                    first_byte |= 0x01;
                }

                dst.put_u8(first_byte);
                dst.put_slice(&varint_buf);
                dst.put(body);
            }

            Packet::Subscribe { packet_id, topics } => {
                // Fixed header flag per SUBSCRIBE: i bit 3..0 devono essere rigorosamente 0010 (0x02)
                let first_byte = ((PacketType::Subscribe as u8) << 4) | 0x02;

                let mut body = BytesMut::new();
                body.put_u16(packet_id);

                for topic in topics {
                    Self::encode_str(&mut body, &topic.filter)?;
                    body.put_u8(topic.qos as u8);
                }

                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;

                dst.put_u8(first_byte);
                dst.put_slice(&varint_buf);
                dst.put(body);
            }

            Packet::SubAck {
                packet_id,
                return_codes,
            } => {
                let first_byte = (PacketType::SubAck as u8) << 4;

                let mut body = BytesMut::new();
                body.put_u16(packet_id);

                for code in return_codes {
                    body.put_u8(code as u8);
                }

                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;

                dst.put_u8(first_byte);
                dst.put_slice(&varint_buf);
                dst.put(body);
            }

            Packet::PubAck { packet_id } => {
                // Fixed Header: tipo 4 (0x40)
                dst.put_u8((PacketType::PubAck as u8) << 4);
                // Remaining Length: esattamente 2 byte (la dimensione dell'u16 packet_id)
                dst.put_u8(2);
                // Variable Header: Packet Identifier
                dst.put_u16(packet_id);
            }

            Packet::Unsubscribe { packet_id, filters } => {
                // MQTT-3.10.1-1: flag fissi 0010 (0x02)
                let first_byte = ((PacketType::Unsubscribe as u8) << 4) | 0x02;

                let mut body = BytesMut::new();
                body.put_u16(packet_id);
                for filter in filters {
                    Self::encode_str(&mut body, &filter)?;
                }

                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(body.len(), &mut varint_buf)?;

                dst.put_u8(first_byte);
                dst.put_slice(&varint_buf);
                dst.put(body);
            }

            Packet::UnsubAck { packet_id } => {
                // Fixed Header: tipo 11 (0xB0), flag 0, remaining length 2
                dst.put_u8((PacketType::UnsubAck as u8) << 4);
                dst.put_u8(2);
                dst.put_u16(packet_id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_and_connack_roundtrip() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        let connect = Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt311,
            clean_session: true,
            keep_alive: 60,
            client_id: "ferrite-client-01".to_string(),
            will: None,
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
                crate::SubscribeTopic {
                    filter: "sensors/+/temperature".to_string(),
                    qos: QoS::AtLeastOnce,
                },
                crate::SubscribeTopic {
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
            return_codes: vec![
                crate::SubAckReturnCode::SuccessQoS1,
                crate::SubAckReturnCode::SuccessQoS0,
            ],
        };

        codec.encode(suback.clone(), &mut buffer).unwrap();
        let decoded_ack = codec.decode(&mut buffer).unwrap();
        assert_eq!(decoded_ack, Some(suback));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_puback_roundtrip() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        let original = Packet::PubAck { packet_id: 1054 };

        // Serializzazione
        codec.encode(original.clone(), &mut buffer).unwrap();
        // Fixed header (0x40), Remaining Length (0x02), Packet ID (0x04, 0x1E per 1054)
        assert_eq!(&buffer[..], &[0x40, 0x02, 0x04, 0x1E]);

        // Deserializzazione
        let decoded = codec.decode(&mut buffer).unwrap();
        assert_eq!(decoded, Some(original));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_puback_incomplete_buffer() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        // Inviamo solo header fisso e il primo byte del packet ID (manca 1 byte)
        buffer.put_slice(&[0x40, 0x02, 0x00]);

        // Il codec non deve andare in errore ma restituire None (attesa dati)
        let result = codec.decode(&mut buffer).unwrap();
        assert_eq!(result, None);

        // Aggiungiamo il byte mancante
        buffer.put_u8(0x2A); // ID: 42
        let result = codec.decode(&mut buffer).unwrap();
        assert_eq!(result, Some(Packet::PubAck { packet_id: 42 }));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_connect_with_will_roundtrip() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        let connect = Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt311,
            clean_session: true,
            keep_alive: 30,
            client_id: "sensor-7".to_string(),
            will: Some(Will {
                topic: "status/sensor-7".to_string(),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::AtLeastOnce,
                retain: true,
            }),
            username: None,
            password: None,
        };

        codec.encode(connect.clone(), &mut buffer).unwrap();

        // Connect flags: clean session (0x02) | will (0x04) | will QoS 1 (0x08) | will retain (0x20)
        // Layout: header(1) + remaining length(1) + "MQTT"(2+4) + level(1) -> flags al byte 9
        assert_eq!(buffer[9], 0x02 | 0x04 | 0x08 | 0x20);

        let decoded = codec.decode(&mut buffer).unwrap();
        assert_eq!(decoded, Some(connect));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_unsupported_packet_returns_error_and_consumes_bytes() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        // PUBREC (tipo 5) con packet id 1, seguito da un PINGREQ valido
        buffer.put_slice(&[0x50, 0x02, 0x00, 0x01]);
        buffer.put_slice(&[0xC0, 0x00]);

        let result = codec.decode(&mut buffer);
        assert!(matches!(result, Err(ProtocolError::InvalidPacketType(5))));

        // Il pacchetto non supportato è stato consumato: il successivo si decodifica normalmente
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(Packet::PingReq));
    }

    #[test]
    fn test_packet_id_zero_rejected_for_publish_qos1_and_qos2() {
        let mut codec = MqttCodec::default();

        for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
            let mut buffer = BytesMut::new();
            codec
                .encode(
                    Packet::Publish {
                        topic: "a/b".to_string(),
                        qos,
                        retain: false,
                        dup: false,
                        packet_id: Some(0),
                        payload: Bytes::from_static(b"hi"),
                    },
                    &mut buffer,
                )
                .unwrap();

            let result = codec.decode(&mut buffer);
            assert!(
                matches!(result, Err(ProtocolError::InvalidPacketId)),
                "atteso InvalidPacketId per PUBLISH {:?}",
                qos
            );
        }
    }

    #[test]
    fn test_packet_id_zero_rejected_for_puback_subscribe_suback() {
        let mut codec = MqttCodec::default();

        // PUBACK con packet_id = 0
        let mut buf_puback = BytesMut::new();
        codec
            .encode(Packet::PubAck { packet_id: 0 }, &mut buf_puback)
            .unwrap();
        assert!(matches!(
            codec.decode(&mut buf_puback),
            Err(ProtocolError::InvalidPacketId)
        ));

        // SUBSCRIBE con packet_id = 0
        let mut buf_sub = BytesMut::new();
        codec
            .encode(
                Packet::Subscribe {
                    packet_id: 0,
                    topics: vec![crate::SubscribeTopic {
                        filter: "a/b".to_string(),
                        qos: QoS::AtMostOnce,
                    }],
                },
                &mut buf_sub,
            )
            .unwrap();
        assert!(matches!(
            codec.decode(&mut buf_sub),
            Err(ProtocolError::InvalidPacketId)
        ));

        // SUBACK con packet_id = 0
        let mut buf_suback = BytesMut::new();
        codec
            .encode(
                Packet::SubAck {
                    packet_id: 0,
                    return_codes: vec![crate::SubAckReturnCode::SuccessQoS0],
                },
                &mut buf_suback,
            )
            .unwrap();
        assert!(matches!(
            codec.decode(&mut buf_suback),
            Err(ProtocolError::InvalidPacketId)
        ));
    }

    #[test]
    fn test_connect_unsupported_protocol_level_3_and_5() {
        let mut codec = MqttCodec::default();

        // Livello 5 (MQTT 5.0) costruito in byte grezzi (l'encoder rifiuta Mqtt50)
        let mut buf_v5 = BytesMut::from(
            &[
                0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
            ][..],
        );
        assert!(matches!(
            codec.decode(&mut buf_v5),
            Err(ProtocolError::UnsupportedProtocolLevel(5))
        ));
        assert!(buf_v5.is_empty());

        // Livello 3 con nome protocollo "MQTT" (costruito a mano)
        let mut buf_v3 = BytesMut::new();
        // Fixed header: CONNECT (0x10), remaining length = 12
        // Variable header: "MQTT" (0x00, 0x04, 'M','Q','T','T'), level=3, flags=0x02, keep_alive=60
        // Payload: empty client_id (0x00, 0x00)
        buf_v3.put_slice(&[
            0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x03, 0x02, 0x00, 0x3C, 0x00, 0x00,
        ]);
        assert!(matches!(
            codec.decode(&mut buf_v3),
            Err(ProtocolError::UnsupportedProtocolLevel(3))
        ));
        assert!(buf_v3.is_empty());
    }

    #[test]
    fn test_publish_qos0_with_dup_is_rejected() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        codec
            .encode(
                Packet::Publish {
                    topic: "t/x".to_string(),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    dup: true,
                    packet_id: None,
                    payload: Bytes::from_static(b"hi"),
                },
                &mut buffer,
            )
            .unwrap();

        let result = codec.decode(&mut buffer);
        assert!(matches!(
            result,
            Err(ProtocolError::InvalidFixedHeaderFlags {
                packet_type: 3,
                flags: 0x08
            })
        ));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_unsubscribe_and_unsuback_roundtrip() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        // UNSUBSCRIBE con un singolo filtro
        let unsub_single = Packet::Unsubscribe {
            packet_id: 7,
            filters: vec!["a/b".to_string()],
        };
        codec.encode(unsub_single.clone(), &mut buffer).unwrap();
        assert_eq!(buffer[0], 0xA2);
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(unsub_single));
        assert!(buffer.is_empty());

        // UNSUBSCRIBE con più filtri
        let unsub_multi = Packet::Unsubscribe {
            packet_id: 42,
            filters: vec!["sensors/+/temp".to_string(), "alerts/#".to_string()],
        };
        codec.encode(unsub_multi.clone(), &mut buffer).unwrap();
        assert_eq!(buffer[0], 0xA2);
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(unsub_multi));
        assert!(buffer.is_empty());

        // UNSUBACK
        let unsuback = Packet::UnsubAck { packet_id: 42 };
        codec.encode(unsuback.clone(), &mut buffer).unwrap();
        assert_eq!(&buffer[..], &[0xB0, 0x02, 0x00, 0x2A]);
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(unsuback));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_unsubscribe_and_unsuback_validation_errors() {
        let mut codec = MqttCodec::default();

        // UNSUBSCRIBE flag sbagliati (0xA0 invece di 0xA2)
        let mut buf = BytesMut::from(&[0xA0, 0x05, 0x00, 0x01, 0x00, 0x01, b'a'][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidFixedHeaderFlags {
                packet_type: 10,
                flags: 0x00
            })
        ));

        // UNSUBACK flag sbagliati (0xB1 invece di 0xB0)
        let mut buf = BytesMut::from(&[0xB1, 0x02, 0x00, 0x01][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidFixedHeaderFlags {
                packet_type: 11,
                flags: 0x01
            })
        ));

        // UNSUBSCRIBE con zero filtri (MQTT-3.10.3-2)
        let mut buf = BytesMut::from(&[0xA2, 0x02, 0x00, 0x01][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::MalformedPacket(_))
        ));

        // UNSUBSCRIBE con packet_id = 0
        let mut buf = BytesMut::from(&[0xA2, 0x05, 0x00, 0x00, 0x00, 0x01, b'a'][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidPacketId)
        ));

        // UNSUBACK con packet_id = 0
        let mut buf = BytesMut::from(&[0xB0, 0x02, 0x00, 0x00][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidPacketId)
        ));

        // UNSUBACK con byte in eccesso (remaining length 3)
        let mut buf = BytesMut::from(&[0xB0, 0x03, 0x00, 0x01, 0xFF][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::MalformedPacket("trailing bytes"))
        ));
    }

    #[test]
    fn test_unsubscribe_and_unsuback_incomplete_buffer() {
        let mut codec = MqttCodec::default();

        // UNSUBSCRIBE spezzato a metà
        let mut buf = BytesMut::from(&[0xA2, 0x05, 0x00, 0x01, 0x00][..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        buf.put_slice(&[0x01, b'a']);
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Packet::Unsubscribe {
                packet_id: 1,
                filters: vec!["a".to_string()],
            })
        );
        assert!(buf.is_empty());

        // UNSUBACK spezzato a metà
        let mut buf = BytesMut::from(&[0xB0, 0x02, 0x00][..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        buf.put_u8(0x09);
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Packet::UnsubAck { packet_id: 9 })
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_fixed_header_flags_validation_for_all_packet_types() {
        let mut codec = MqttCodec::default();

        // Tipi che richiedono flag 0b0000: CONNECT(1), CONNACK(2), PUBACK(4),
        // SUBACK(9), UNSUBACK(11), PINGREQ(12), PINGRESP(13), DISCONNECT(14).
        let zero_flag_packets: &[(u8, &[u8])] = &[
            (1, &[0x11, 0x00]),
            (2, &[0x21, 0x02, 0x00, 0x00]),
            (4, &[0x41, 0x02, 0x00, 0x01]),
            (9, &[0x91, 0x03, 0x00, 0x01, 0x00]),
            (11, &[0xB1, 0x02, 0x00, 0x01]),
            (12, &[0xC1, 0x00]),
            (13, &[0xD1, 0x00]),
            (14, &[0xE1, 0x00]),
        ];
        for &(expected_type, raw) in zero_flag_packets {
            let mut buf = BytesMut::from(raw);
            assert!(
                matches!(
                    codec.decode(&mut buf),
                    Err(ProtocolError::InvalidFixedHeaderFlags {
                        packet_type,
                        flags: 0x01
                    }) if packet_type == expected_type
                ),
                "atteso InvalidFixedHeaderFlags per tipo {expected_type}"
            );
        }

        // Tipi che richiedono flag 0b0010: SUBSCRIBE(8) e UNSUBSCRIBE(10).
        let two_flag_packets: &[(u8, &[u8])] = &[
            (8, &[0x80, 0x06, 0x00, 0x01, 0x00, 0x01, b'a', 0x00]),
            (10, &[0xA0, 0x05, 0x00, 0x01, 0x00, 0x01, b'a']),
        ];
        for &(expected_type, raw) in two_flag_packets {
            let mut buf = BytesMut::from(raw);
            assert!(
                matches!(
                    codec.decode(&mut buf),
                    Err(ProtocolError::InvalidFixedHeaderFlags {
                        packet_type,
                        flags: 0x00
                    }) if packet_type == expected_type
                ),
                "atteso InvalidFixedHeaderFlags per tipo {expected_type}"
            );
        }

        // PUBLISH QoS 3 (bits 2..1 = 0b11 -> 0x36)
        let mut buf_qos3 = BytesMut::from(&[0x36, 0x03, 0x00, 0x01, b'a'][..]);
        assert!(matches!(
            codec.decode(&mut buf_qos3),
            Err(ProtocolError::InvalidQoS(3))
        ));
    }

    #[test]
    fn test_trailing_bytes_rejected_for_all_packet_types() {
        let mut codec = MqttCodec::default();

        // CONNECT valido + 1 byte in eccesso (remaining length 13 invece di 12)
        let mut connect = BytesMut::from(
            &[
                0x10, 0x0D, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00,
                0xFF,
            ][..],
        );
        assert!(matches!(
            codec.decode(&mut connect),
            Err(ProtocolError::MalformedPacket("trailing bytes"))
        ));

        // CONNACK + 1 byte in eccesso
        let mut connack = BytesMut::from(&[0x20, 0x03, 0x00, 0x00, 0xFF][..]);
        assert!(matches!(
            codec.decode(&mut connack),
            Err(ProtocolError::MalformedPacket("trailing bytes"))
        ));

        // PUBACK + 1 byte in eccesso
        let mut puback = BytesMut::from(&[0x40, 0x03, 0x00, 0x01, 0xFF][..]);
        assert!(matches!(
            codec.decode(&mut puback),
            Err(ProtocolError::MalformedPacket("trailing bytes"))
        ));

        // UNSUBACK + 1 byte in eccesso
        let mut unsuback = BytesMut::from(&[0xB0, 0x03, 0x00, 0x01, 0xFF][..]);
        assert!(matches!(
            codec.decode(&mut unsuback),
            Err(ProtocolError::MalformedPacket("trailing bytes"))
        ));

        // PINGREQ, PINGRESP, DISCONNECT con 1 byte in eccesso
        for raw in [
            &[0xC0, 0x01, 0x00][..],
            &[0xD0, 0x01, 0x00][..],
            &[0xE0, 0x01, 0x00][..],
        ] {
            let mut buf = BytesMut::from(raw);
            assert!(matches!(
                codec.decode(&mut buf),
                Err(ProtocolError::MalformedPacket("trailing bytes"))
            ));
        }

        // SUBSCRIBE e UNSUBSCRIBE con byte troncato alla fine (frame completo ma corpo malformato)
        let mut sub_junk =
            BytesMut::from(&[0x82, 0x07, 0x00, 0x01, 0x00, 0x01, b'a', 0x00, 0xFF][..]);
        assert!(matches!(
            codec.decode(&mut sub_junk),
            Err(ProtocolError::MalformedPacket(_))
        ));

        let mut unsub_junk = BytesMut::from(&[0xA2, 0x06, 0x00, 0x01, 0x00, 0x01, b'a', 0xFF][..]);
        assert!(matches!(
            codec.decode(&mut unsub_junk),
            Err(ProtocolError::MalformedPacket(_))
        ));

        // SUBACK con codice non valido alla fine / corpo vuoto
        let mut suback_bad = BytesMut::from(&[0x90, 0x04, 0x00, 0x01, 0x00, 0x7F][..]);
        assert!(codec.decode(&mut suback_bad).is_err());
    }

    #[test]
    fn test_connect_flags_and_protocol_name_validation() {
        let mut codec = MqttCodec::default();

        // Helper: costruisce un CONNECT minimo con connect_flags personalizzati.
        let make_connect = |flags: u8| -> BytesMut {
            BytesMut::from(
                &[
                    0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, flags, 0x00, 0x3C, 0x00,
                    0x00,
                ][..],
            )
        };

        // (a) Bit riservato a 1 (0x01) -> MalformedPacket("reserved connect flag")
        let mut buf = make_connect(0x03);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::MalformedPacket("reserved connect flag"))
        ));

        // (b) Will Flag = 0 con Will QoS != 0 (0x08) o Will Retain = 1 (0x20)
        let mut buf = make_connect(0x02 | 0x08);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::MalformedPacket(_))
        ));
        let mut buf = make_connect(0x02 | 0x20);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::MalformedPacket(_))
        ));

        // (c) Password Flag = 1 senza Username Flag (0x40)
        let mut buf = make_connect(0x02 | 0x40);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::MalformedPacket(_))
        ));

        // (d) Nome protocollo diverso da "MQTT" -> InvalidProtocolName
        let mut buf_proto = BytesMut::from(
            &[
                0x10, 0x0C, 0x00, 0x04, b'H', b'T', b'T', b'P', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00,
            ][..],
        );
        assert!(matches!(
            codec.decode(&mut buf_proto),
            Err(ProtocolError::InvalidProtocolName)
        ));

        // (e) Will Flag = 1 con Will QoS = 3 (0x04 | 0x18) -> InvalidQoS(3)
        let mut buf = make_connect(0x02 | 0x04 | 0x18);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidQoS(3))
        ));
    }

    #[test]
    fn test_string_nul_char_and_empty_publish_topic_rejected() {
        let mut codec = MqttCodec::default();

        // Stringa UTF-8 con U+0000 in PUBLISH topic -> InvalidUtf8 (MQTT-1.5.3-2)
        let mut buf_nul = BytesMut::from(&[0x30, 0x05, 0x00, 0x03, b'a', 0x00, b'b'][..]);
        assert!(matches!(
            codec.decode(&mut buf_nul),
            Err(ProtocolError::InvalidUtf8)
        ));

        // Stringa UTF-8 con U+0000 in CONNECT client_id -> InvalidUtf8
        let mut buf_cid_nul = BytesMut::from(
            &[
                0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x02,
                b'a', 0x00,
            ][..],
        );
        assert!(matches!(
            codec.decode(&mut buf_cid_nul),
            Err(ProtocolError::InvalidUtf8)
        ));

        // PUBLISH con topic vuoto -> MalformedPacket("empty topic") (MQTT-4.7.3-1)
        let mut buf_empty_topic = BytesMut::from(&[0x30, 0x02, 0x00, 0x00][..]);
        assert!(matches!(
            codec.decode(&mut buf_empty_topic),
            Err(ProtocolError::MalformedPacket("empty topic"))
        ));
    }

    #[test]
    fn test_encoder_rejects_oversized_fields_and_missing_packet_id_atomically() {
        let mut codec = MqttCodec::default();
        let initial_prefix = b"existing-bytes";

        // 1. Topic da 70000 byte -> StringTooLong(70000), dst invariato
        let mut dst = BytesMut::from(&initial_prefix[..]);
        let res = codec.encode(
            Packet::Publish {
                topic: "t".repeat(70_000),
                qos: QoS::AtMostOnce,
                retain: false,
                dup: false,
                packet_id: None,
                payload: Bytes::new(),
            },
            &mut dst,
        );
        assert!(matches!(res, Err(ProtocolError::StringTooLong(70_000))));
        assert_eq!(&dst[..], initial_prefix);

        // 2. Will payload da 70000 byte -> StringTooLong(70000), dst invariato
        let mut dst = BytesMut::from(&initial_prefix[..]);
        let res = codec.encode(
            Packet::Connect {
                protocol_version: ProtocolVersion::Mqtt311,
                clean_session: true,
                keep_alive: 60,
                client_id: "c1".into(),
                will: Some(Will {
                    topic: "will/t".into(),
                    payload: Bytes::from(vec![b'w'; 70_000]),
                    qos: QoS::AtMostOnce,
                    retain: false,
                }),
                username: None,
                password: None,
            },
            &mut dst,
        );
        assert!(matches!(res, Err(ProtocolError::StringTooLong(70_000))));
        assert_eq!(&dst[..], initial_prefix);

        // 3. Password da 70000 byte -> StringTooLong(70000), dst invariato
        let mut dst = BytesMut::from(&initial_prefix[..]);
        let res = codec.encode(
            Packet::Connect {
                protocol_version: ProtocolVersion::Mqtt311,
                clean_session: true,
                keep_alive: 60,
                client_id: "c1".into(),
                will: None,
                username: Some("user".into()),
                password: Some(Bytes::from(vec![b'p'; 70_000])),
            },
            &mut dst,
        );
        assert!(matches!(res, Err(ProtocolError::StringTooLong(70_000))));
        assert_eq!(&dst[..], initial_prefix);

        // 4. PUBLISH QoS 1 con packet_id: None -> MalformedPacket("missing packet id"), dst invariato
        let mut dst = BytesMut::from(&initial_prefix[..]);
        let res = codec.encode(
            Packet::Publish {
                topic: "a/b".into(),
                qos: QoS::AtLeastOnce,
                retain: false,
                dup: false,
                packet_id: None,
                payload: Bytes::from_static(b"data"),
            },
            &mut dst,
        );
        assert!(matches!(
            res,
            Err(ProtocolError::MalformedPacket("missing packet id"))
        ));
        assert_eq!(&dst[..], initial_prefix);
    }

    #[test]
    fn test_encode_connect_mqtt50_rejected_without_writing() {
        let mut codec = MqttCodec::default();
        let initial_prefix = b"untouched";
        let mut dst = BytesMut::from(&initial_prefix[..]);

        let res = codec.encode(
            Packet::Connect {
                protocol_version: ProtocolVersion::Mqtt50,
                clean_session: true,
                keep_alive: 60,
                client_id: "c-v5".into(),
                will: None,
                username: None,
                password: None,
            },
            &mut dst,
        );

        assert!(matches!(
            res,
            Err(ProtocolError::UnsupportedProtocolLevel(5))
        ));
        assert_eq!(&dst[..], initial_prefix);
    }

    #[test]
    fn test_decoder_caps_eager_reserve_on_incomplete_large_frame() {
        let mut codec = MqttCodec::default();
        let mut buf = BytesMut::new();

        // Fixed header di un PUBLISH QoS 0 che dichiara 1 MiB (1_048_576 byte) di corpo,
        // senza inviare alcun byte del corpo.
        buf.put_u8(0x30);
        let mut varint = Vec::with_capacity(4);
        encode_varint(1024 * 1024, &mut varint).unwrap();
        buf.put_slice(&varint);

        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(
            buf.capacity() < 16 * 1024,
            "il decoder ha prenotato troppa memoria prima di ricevere il corpo: capacity={}",
            buf.capacity()
        );
    }
}
