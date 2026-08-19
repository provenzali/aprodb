use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    Result,
    engine::Durability,
    record::{FrameRead, Record, read_frame, write_frame},
};

pub(crate) struct Wal {
    file: File,
    durability: Durability,
}

impl Wal {
    pub(crate) fn open(path: &Path, durability: Durability) -> Result<(Self, Vec<Record>)> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::Start(0))?;

        let mut records = Vec::new();
        let mut valid_len = 0u64;
        loop {
            match read_frame(&mut file)? {
                FrameRead::Record(record) => {
                    valid_len = file.stream_position()?;
                    records.push(record);
                }
                FrameRead::Eof => break,
                FrameRead::Truncated => {
                    file.set_len(valid_len)?;
                    file.sync_data()?;
                    break;
                }
            }
        }
        file.seek(SeekFrom::End(0))?;
        Ok((Self { file, durability }, records))
    }

    pub(crate) fn append_batch(&mut self, records: &[Record]) -> Result<()> {
        for record in records {
            write_frame(&mut self.file, record)?;
        }
        self.file.flush()?;
        if self.durability == Durability::SyncData {
            self.file.sync_data()?;
        }
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
