use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use std::io;

/// Represents a RESP (Redis Serialization Protocol) value.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
}

pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match src[0] {
            b'+' => Ok(Self::decode_simple_string(src)),
            b'-' => Ok(Self::decode_error(src)),
            b':' => Self::decode_integer(src),
            b'$' => Self::decode_bulk_string(src),
            b'*' => self.decode_array(src),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown RESP type")),
        }
    }
}

impl RespCodec {
    fn decode_simple_string(src: &mut BytesMut) -> Option<RespValue> {
        if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
            let line = src.split_to(pos).split_off(1); // Skip '+'
            src.advance(2); // Skip CRLF
            let s = String::from_utf8_lossy(&line).into_owned();
            Some(RespValue::SimpleString(s))
        } else {
            None
        }
    }

    fn decode_error(src: &mut BytesMut) -> Option<RespValue> {
        if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
            let line = src.split_to(pos).split_off(1); // Skip '-'
            src.advance(2); // Skip CRLF
            let s = String::from_utf8_lossy(&line).into_owned();
            Some(RespValue::Error(s))
        } else {
            None
        }
    }

    fn decode_integer(src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
        if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
            let line = src.split_to(pos).split_off(1); // Skip ':'
            src.advance(2); // Skip CRLF
            let s = String::from_utf8_lossy(&line);
            let n = s.parse::<i64>().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid integer"))?;
            Ok(Some(RespValue::Integer(n)))
        } else {
            Ok(None)
        }
    }

    fn decode_bulk_string(src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
        if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
            let len_line = src.windows(pos).next().unwrap();
            let len_str = String::from_utf8_lossy(&len_line[1..]);
            let len = len_str.parse::<i32>().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string length"))?;

            if len == -1 {
                src.advance(pos + 2);
                return Ok(Some(RespValue::BulkString(None)));
            }
            
            if len < 0 {
                 return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string length"));
            }

            let ulen = len as usize;
            if src.len() < pos + 2 + ulen + 2 {
                return Ok(None); // Need more data
            }

            src.advance(pos + 2);
            let data = src.split_to(ulen).freeze();
            src.advance(2); // Skip CRLF
            Ok(Some(RespValue::BulkString(Some(data))))
        } else {
            Ok(None)
        }
    }

    fn decode_array(&mut self, src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
        if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
            let len_line = &src[1..pos];
            let len_str = String::from_utf8_lossy(len_line);
            let len = len_str.parse::<i32>().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array length"))?;

            if len == -1 {
                src.advance(pos + 2);
                return Ok(Some(RespValue::Array(None)));
            }

            if len < 0 {
                 return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid array length"));
            }

            let ulen = len as usize;
            
            // We need to keep track of how many bytes we've "virtually" consumed
            let mut offset = pos + 2;
            
            for _ in 0..ulen {
                if offset >= src.len() {
                    return Ok(None);
                }
                
                match Self::check_enough_data(&src[offset..])? {
                    Some(consumed) => offset += consumed,
                    None => return Ok(None),
                }
            }

            // If we get here, we have enough data! Now we can actually consume it.
            src.advance(pos + 2);
            let mut items = Vec::with_capacity(ulen);
            for _ in 0..ulen {
                items.push(self.decode(src)?.unwrap());
            }
            Ok(Some(RespValue::Array(Some(items))))
        } else {
            Ok(None)
        }
    }

    /// Returns Some(consumed_bytes) if a full RESP value is present at the start of src.
    fn check_enough_data(src: &[u8]) -> Result<Option<usize>, io::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match src[0] {
            b'+' | b'-' | b':' => {
                if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
                    Ok(Some(pos + 2))
                } else {
                    Ok(None)
                }
            }
            b'$' => {
                if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
                    let len_str = String::from_utf8_lossy(&src[1..pos]);
                    let len = len_str.parse::<i32>().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string length"))?;
                    if len == -1 {
                        return Ok(Some(pos + 2));
                    }
                    if len < 0 {
                         return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string length"));
                    }
                    let total = pos + 2 + len as usize + 2;
                    if src.len() >= total {
                        Ok(Some(total))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            b'*' => {
                if let Some(pos) = src.windows(2).position(|w| w == b"\r\n") {
                    let len_str = String::from_utf8_lossy(&src[1..pos]);
                    let len = len_str.parse::<i32>().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array length"))?;
                    if len == -1 {
                        return Ok(Some(pos + 2));
                    }
                    if len < 0 {
                         return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid array length"));
                    }
                    let mut offset = pos + 2;
                    for _ in 0..len {
                        if offset >= src.len() {
                            return Ok(None);
                        }
                        match Self::check_enough_data(&src[offset..])? {
                            Some(consumed) => offset += consumed,
                            None => return Ok(None),
                        }
                    }
                    Ok(Some(offset))
                } else {
                    Ok(None)
                }
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown RESP type")),
        }
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(n.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(data.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&data);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                dst.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(items)) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for item in items {
                    self.encode(item, dst)?;
                }
            }
        }
        Ok(())
    }
}
