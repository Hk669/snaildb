use crate::utils::record::RecordKind;

#[derive(Debug)]
pub enum WriteCommand {
    WriteRecord {
        kind: RecordKind,
        key: String,
        value: Vec<u8>,
    },
    Flush,
    Reset,
    Shutdown,
}

