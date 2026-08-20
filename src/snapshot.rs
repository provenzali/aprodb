use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use crate::{
    AproError, Result,
    record::{FrameRead, Record, read_frame, write_frame},
};

const SNAPSHOT_MAGIC: [u8; 8] = *b"APSNAP01";

pub(crate) fn load(path: &Path) -> Result<Vec<Record>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if magic != SNAPSHOT_MAGIC {
        return Err(AproError::Corrupt("invalid snapshot magic number".into()));
    }
    let mut records = Vec::new();
    loop {
        match read_frame(&mut file)? {
            FrameRead::Record(record) => records.push(record),
            FrameRead::Eof => break,
            FrameRead::Truncated => {
                return Err(AproError::Corrupt("truncated snapshot".into()));
            }
        }
    }
    Ok(records)
}

pub(crate) fn save(path: &Path, records: &[Record]) -> Result<()> {
    let temp = path.with_extension("snapshot.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&SNAPSHOT_MAGIC)?;
    for record in records {
        write_frame(&mut file, record)?;
    }
    file.flush()?;
    file.sync_all()?;
    drop(file);

    if path.exists() {
        fs::remove_file(path)?;
    }
    fs::rename(temp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::save;
    use crate::{
        Value,
        compression::CompressionChannel,
        record::{Operation, Record},
    };

    #[test]
    fn v0_1_snapshot_matches_golden_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("aprodb.snapshot");
        let mut compression = CompressionChannel::new(1, 32).unwrap();
        let stored = compression.compress(&Value::Text("hello".into())).unwrap();
        save(
            &path,
            &[Record {
                sequence: 7,
                key: "key".into(),
                operation: Operation::Put(stored),
            }],
        )
        .unwrap();
        let expected =
            hex::decode(include_str!("../tests/golden/v0_1_snapshot.hex").trim()).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), expected);
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
