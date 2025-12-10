use crc32fast::Hasher;
use std::io::{self, Read, Write};

#[derive(Debug, Clone, Copy)]
pub enum RecordKind {
    Set = 1,
    Delete = 2,
}

impl RecordKind {
    fn as_byte(self) -> u8 {
        self as u8
    }

    fn from_byte(byte: u8) -> io::Result<Self> {
        match byte {
            1 => Ok(RecordKind::Set),
            2 => Ok(RecordKind::Delete),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown record kind {byte}"),
            )),
        }
    }
}

// a record decoded from the binary format
// the on-disk binary format is (little endian unless noted):
// [length:u32][crc32:u32][kind:u8][key_length:varint][key][value_length:varint][value]
pub struct DecodedRecord {
    pub kind: RecordKind, // 1 for set, 2 for delete
    pub key: String,
    pub value: Vec<u8>,
    pub crc32: u32,        // checksum of each record
    pub length: u32,       // length of the record payload
    pub key_length: u32,   // length of the key portion
    pub value_length: u32, // length of the value portion
    pub timestamp: u64,
}

pub fn write_record<W: Write>(
    writer: &mut W,
    kind: RecordKind,
    key: &str,
    value: &[u8],
) -> io::Result<()> {
    let key_len: u32 = key
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key too large"))?;
    let value_len: u32 = value
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "value too large"))?;
    // [kind][key_len_varint][key][value_len_varint][value]
    // When key/value are 6 bytes each the payload is 15 bytes:
    //   01 06 75 73 65 72 3A 31 06 48 72 75 73 68 69
    let key_len_encoded = encode_var_u32(key_len);
    let value_len_encoded = encode_var_u32(value_len);

    let payload_len = 1 + key_len_encoded.len() + key.len() + value_len_encoded.len() + value.len();

    let mut payload = Vec::with_capacity(payload_len);
    payload.push(kind.as_byte());
    payload.extend_from_slice(&key_len_encoded);
    payload.extend_from_slice(key.as_bytes());
    payload.extend_from_slice(&value_len_encoded);
    payload.extend_from_slice(value);

    let length: u32 = payload
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "record too large"))?;

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc32 = hasher.finalize();

    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(&crc32.to_le_bytes())?;
    writer.write_all(&payload)?;
    Ok(())
}

pub fn read_record<R: Read>(reader: &mut R) -> io::Result<Option<DecodedRecord>> {
    let length = match read_u32_or_eof(reader)? {
        Some(len) => len,
        None => return Ok(None),
    };
    let crc32 = read_u32(reader, "crc32")?;

    let payload_len: usize = length
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "record length too large"))?;
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let computed_crc = hasher.finalize();
    if computed_crc != crc32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "crc mismatch while reading record",
        ));
    }

    let mut cursor = 0usize;

    let kind_byte = *payload.get(cursor).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "record payload missing kind byte",
        )
    })?;
    cursor += 1;
    let kind = RecordKind::from_byte(kind_byte)?;

    let key_len = decode_var_u32(&payload, &mut cursor)?;
    let key_len_usize: usize = key_len
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "key too large in record"))?;
    let key_end = cursor
        .checked_add(key_len_usize)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "key length overflow"))?;
    if key_end > payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "record truncated while reading key",
        ));
    }
    let key_bytes = &payload[cursor..key_end];
    cursor = key_end;

    let key = String::from_utf8(key_bytes.to_vec())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "key bytes are not valid UTF-8"))?;

    let value_len = decode_var_u32(&payload, &mut cursor)?;
    let value_len_usize: usize = value_len
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "value too large in record"))?;
    let value_end = cursor
        .checked_add(value_len_usize)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "value length overflow"))?;
    if value_end > payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "record truncated while reading value",
        ));
    }
    let value = payload[cursor..value_end].to_vec();
    cursor = value_end;

    if cursor != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record payload contains unexpected trailing bytes",
        ));
    }

    Ok(Some(DecodedRecord {
        kind,
        key,
        value,
        crc32,
        length,
        key_length: key_len,
        value_length: value_len,
        timestamp: 0,
    }))
}

fn read_u32<R: Read>(reader: &mut R, label: &str) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(|err| {
        io::Error::new(err.kind(), format!("unable to read {label} length: {err}"))
    })?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u32_or_eof<R: Read>(reader: &mut R) -> io::Result<Option<u32>> {
    let mut buf = [0u8; 4];
    let mut read = 0;
    while read < 4 {
        match reader.read(&mut buf[read..])? {
            0 if read == 0 => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated record length header",
                ));
            }
            n => read += n,
        }
    }
    Ok(Some(u32::from_le_bytes(buf)))
}

fn encode_var_u32(mut value: u32) -> Vec<u8> {
    let mut encoded = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if value == 0 {
            break;
        }
    }
    encoded
}

fn decode_var_u32(buffer: &[u8], cursor: &mut usize) -> io::Result<u32> {
    let mut value = 0u32;
    let mut shift = 0;
    for _ in 0..5 {
        let byte = *buffer.get(*cursor).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated varint while reading record",
            )
        })?;
        *cursor += 1;
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "varint too long while decoding record",
    ))
}
