#[derive(Clone, Debug)]
pub enum Value {
    Present(Vec<u8>),
    Deleted,
}

impl Value {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Value::Present(bytes)
    }

    pub fn tombstone() -> Self {
        Value::Deleted
    }

    pub fn as_option(&self) -> Option<Vec<u8>> {
        match self {
            Value::Present(bytes) => Some(bytes.clone()),
            Value::Deleted => None,
        }
    }
}
