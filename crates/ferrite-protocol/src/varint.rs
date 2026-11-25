use crate::error::ProtocolError;
use bytes::Buf;

/// Decodes an MQTT Variable Byte Integer from a buffer.
///
/// Returns:
/// - `Ok(Some((value, bytes_consumed)))` if a complete number was read.
/// - `Ok(None)` if the buffer does not have enough bytes yet (streaming/partial read).
/// - `Err(ProtocolError)` if the sequence exceeds 4 bytes.
pub fn decode_varint<B: Buf + Clone>(buf: &mut B) -> Result<Option<(usize, usize)>, ProtocolError> {
    let mut multiplier: usize = 1;
    let mut value: usize = 0;
    let mut bytes_read: usize = 0;

    let available = buf.remaining();
    let mut cursors = buf.clone();

    loop {
        if bytes_read >= available {
            return Ok(None);
        }

        let encoded_byte = cursors.get_u8();
        bytes_read += 1;

        value += ((encoded_byte & 0x7F) as usize) * multiplier;

        if multiplier > 128 * 128 * 128 {
            return Err(ProtocolError::MalformedRemainingLength)
        }

        multiplier *= 128;

        // Continuation bit is 0 -> termination reached
        if (encoded_byte & 0x80) == 0 {
            break;
        }

        if bytes_read >= 4 {
            return Err(ProtocolError::MalformedRemainingLength);
        }
    }

    buf.advance(bytes_read);
    Ok(Some((value, bytes_read)))
}

/// Encodes an unsigned integer into an MQTT Variable Byte Integer sequence.
pub fn encode_varint(mut value: usize, dst: &mut Vec<u8>) -> Result<(), ProtocolError> {
    if value > 268_435_455 {
        return Err(ProtocolError::MalformedRemainingLength);
    }

    loop {
        let mut encoded_byte = (value % 128) as u8;
        value /= 128;

        if value > 0 {
            encoded_byte |= 0x80;
        }

        dst.push(encoded_byte);

        if value == 0 {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_varint_single_byte() {
        let mut buf = Bytes::from_static(&[0x00]);
        assert_eq!(decode_varint(&mut buf).unwrap(), Some((0, 1)));

        let mut buf = Bytes::from_static(&[0x7F]);
        assert_eq!(decode_varint(&mut buf).unwrap(), Some((127, 1)));
    }

    #[test]
    fn test_varint_multi_byte() {
        // 128 encoded as two bytes: [0x80, 0x01]
        let mut buf = Bytes::from_static(&[0x80, 0x01]);
        assert_eq!(decode_varint(&mut buf).unwrap(), Some((128, 2)));

        // 16383 encoded as: [0xFF, 0x7F]
        let mut buf = Bytes::from_static(&[0xFF, 0x7F]);
        assert_eq!(decode_varint(&mut buf).unwrap(), Some((16383, 2)));
    }

    #[test]
    fn test_varint_incomplete() {
        // Continuation bit is set (0x80) but second byte is missing
        let mut buf = Bytes::from_static(&[0x80]);
        assert_eq!(decode_varint(&mut buf).unwrap(), None);
    }

    #[test]
    fn test_varint_overflow() {
        // 5 consecutive bytes with continuation bits -> exceeds 4 bytes limit
        let mut buf = Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x01]);

        // Sostituisci assert_eq! con assert!(matches!(...))
        assert!(matches!(
        decode_varint(&mut buf),
        Err(ProtocolError::MalformedRemainingLength)
    ));
    }
}