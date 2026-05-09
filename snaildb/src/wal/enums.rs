use crate::utils::record::RecordKind;
use std::{io, sync::mpsc};

#[derive(Debug)]
pub enum WriteCommand {
    WriteRecord {
        kind: RecordKind,
        key: String,
        value: Vec<u8>,
    },
    Flush {
        ack: mpsc::Sender<io::Result<()>>,
    },
    Reset {
        ack: mpsc::Sender<io::Result<()>>,
    },
    Shutdown,
}

