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

pub struct DecodedRecord {
    pub kind: RecordKind,
    pub key: String,
    pub value: Vec<u8>,
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

    writer.write_all(&[kind.as_byte()])?;
    writer.write_all(&key_len.to_le_bytes())?;
    writer.write_all(&value_len.to_le_bytes())?;
    writer.write_all(key.as_bytes())?;
    writer.write_all(value)?;
    Ok(())
}

pub fn read_record<R: Read>(reader: &mut R) -> io::Result<Option<DecodedRecord>> {
    let mut kind_buf = [0u8; 1];
    let read_bytes = reader.read(&mut kind_buf)?;
    if read_bytes == 0 {
        return Ok(None);
    }

    if read_bytes < 1 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated record kind",
        ));
    }

    let kind = RecordKind::from_byte(kind_buf[0])?;

    let key_len = read_u32(reader, "key")?;
    let value_len = read_u32(reader, "value")?;

    let mut key_buf = vec![0u8; key_len as usize];
    reader.read_exact(&mut key_buf)?;

    let mut value_buf = vec![0u8; value_len as usize];
    reader.read_exact(&mut value_buf)?;

    let key = String::from_utf8(key_buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "key bytes are not valid UTF-8"))?;

    Ok(Some(DecodedRecord {
        kind,
        key,
        value: value_buf,
    }))
}

fn read_u32<R: Read>(reader: &mut R, label: &str) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(|err| {
        io::Error::new(err.kind(), format!("unable to read {label} length: {err}"))
    })?;
    Ok(u32::from_le_bytes(buf))
}
