use crate::error::ProtocolError;
use crate::packet::{Packet, PacketType, QoS};
use crate::varint::{decode_varint, encode_varint};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::str;
use tokio_util::codec::{Decoder, Encoder};

/// Maximum allowed packet size (default: 2 MB) to prevent denial-of-service memory exhaustion.
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

    /// Helper to decode a 2-byte length-prefixed UTF-8 string according to the MQTT specification.
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

    /// Helper to encode a 2-byte length-prefixed UTF-8 string into the destination buffer.
    fn encode_string(src: &str, dst: &mut BytesMut) {
        dst.put_u16(src.len() as u16);
        dst.put_slice(src.as_bytes());
    }
}

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Step 1: We need at least 2 bytes (Fixed Header: 1st byte type/flags + at least 1 byte varint)
        if src.len() < 2 {
            return Ok(None);
        }

        let first_byte = src[0];
        let packet_type = PacketType::try_from(first_byte)?;

        // Clone view over the buffer starting from byte 1 to attempt varint decoding without mutating src yet
        let mut varint_cursor = &src[1..];
        let (remaining_length, varint_bytes) = match decode_varint(&mut varint_cursor)? {
            Some((len, consumed)) => (len, consumed),
            None => return Ok(None), // Not enough bytes to read the remaining length yet
        };

        if remaining_length > self.max_packet_size {
            return Err(ProtocolError::PacketTooLarge);
        }

        let fixed_header_len = 1 + varint_bytes;
        let total_packet_len = fixed_header_len + remaining_length;

        // Step 2: Check if the complete packet body has arrived in the buffer
        if src.len() < total_packet_len {
            // Tell Tokio to wait for more TCP packets
            src.reserve(total_packet_len - src.len());
            return Ok(None);
        }

        // Advance buffer past the fixed header
        src.advance(fixed_header_len);

        // Split out exactly the remaining length bytes for this packet (zero-copy slice)
        let mut body = src.split_to(remaining_length);

        // Step 3: Parse specific packet bodies
        match packet_type {
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

                // Zero-copy: freeze the remaining bytes in the buffer directly into Bytes
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

            _ => {
                // Other packets will be wired up incrementally
                Ok(None)
            }
        }
    }
}

impl Encoder<Packet> for MqttCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::PingReq => {
                dst.put_u8((PacketType::PingReq as u8) << 4);
                dst.put_u8(0); // Remaining Length = 0
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

                // Calculate variable header length
                // topic length (2 bytes) + topic UTF-8 bytes + optional packet_id (2 bytes)
                let mut remaining_len = 2 + topic.len();
                if qos != QoS::AtMostOnce {
                    remaining_len += 2;
                }
                remaining_len += payload.len();

                // 1. First byte
                dst.put_u8(first_byte);

                // 2. Remaining length varint
                let mut varint_buf = Vec::with_capacity(4);
                encode_varint(remaining_len, &mut varint_buf)?;
                dst.put_slice(&varint_buf);

                // 3. Topic string
                Self::encode_string(&topic, dst);

                // 4. Packet Identifier (only if QoS 1 or QoS 2)
                if qos != QoS::AtMostOnce {
                    let id = packet_id.ok_or(ProtocolError::Incomplete)?;
                    dst.put_u16(id);
                }

                // 5. Payload
                dst.put_slice(&payload);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pingreq_encode_decode() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        codec.encode(Packet::PingReq, &mut buffer).unwrap();
        assert_eq!(&buffer[..], &[0xC0, 0x00]);

        let decoded = codec.decode(&mut buffer).unwrap();
        assert_eq!(decoded, Some(Packet::PingReq));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_publish_roundtrip_qos0() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        let original = Packet::Publish {
            topic: "sensors/temperature".to_string(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            payload: Bytes::from_static(b"23.5"),
        };

        codec.encode(original.clone(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap();

        assert_eq!(decoded, Some(original));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_publish_roundtrip_qos1() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        let original = Packet::Publish {
            topic: "cmd/reboot".to_string(),
            qos: QoS::AtLeastOnce,
            retain: true,
            dup: false,
            packet_id: Some(42),
            payload: Bytes::from_static(b"now"),
        };

        codec.encode(original.clone(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap();

        assert_eq!(decoded, Some(original));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_partial_packet_streaming() {
        let mut codec = MqttCodec::default();
        let mut buffer = BytesMut::new();

        let original = Packet::Publish {
            topic: "telemetry".to_string(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            payload: Bytes::from_static(b"large_payload_chunk"),
        };

        let mut full_buffer = BytesMut::new();
        codec.encode(original.clone(), &mut full_buffer).unwrap();

        // Feed only first 5 bytes (incomplete)
        buffer.put_slice(&full_buffer[..5]);
        assert_eq!(codec.decode(&mut buffer).unwrap(), None);

        // Feed the rest of the bytes
        buffer.put_slice(&full_buffer[5..]);
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(original));
        assert!(buffer.is_empty());
    }
}