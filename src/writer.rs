use std::{path::PathBuf, rc::Rc};

use anyhow::{Context, Result, anyhow};
use futures::AsyncWriteExt;
use glommio::{
    ByteSliceMutExt,
    io::{DmaFile, ImmutableFileBuilder},
};

pub struct Writer {
    pub(crate) path: PathBuf,
    pub(crate) keys: caos::Writer<u64>,
    pub(crate) keys_file: Rc<DmaFile>,
    pub(crate) table_offsets: Vec<caos::Writer<u64>>,
    pub(crate) table_offsets_files: Vec<Rc<DmaFile>>,
    pub(crate) table_names: Vec<String>,
    pub(crate) table_files: Vec<Rc<DmaFile>>,
    pub(crate) write_offsets: Vec<u64>,
    pub(crate) length: u64,
}

// This order should ensure that we don't lose any data and the writes are completely atomic and serializable.
// Also it ensures that we don't corrupt anything in case of any kind of interruption.
// Another point is that on a restart we shouldn't lose any data that was previously visible in memory, this is why we write to in memory structures after we ensure files are all updated.
// Note: we assume that in memory writes should never fail so we should crash the program if any of them fail.
// Write order:
// 1) write to the data files
// 2) write to the table offset files
// 3) write to the keys file
// 4) create a new length file and rename it onto the old one
// 5) update write offsets for future writes
// 6) update length for future writes
// 7) write the offsets into the in memory table_offsets
// 8) write the key into in memory keys

impl Writer {
    pub fn table_names(&self) -> &[String] {
        &self.table_names
    }

    pub async fn append(&mut self, key: u64, values: Vec<Vec<u8>>) -> Result<()> {
        if values.len() != self.table_names.len() {
            return Err(anyhow!(
                "number of values ({}) does not equal the number of tables ({})",
                values.len(),
                self.table_names.len()
            ));
        }

        let new_write_offsets = self
            .write_offsets
            .iter()
            .zip(values.iter())
            .map(|(&offset, val)| offset + u64::try_from(val.len()).unwrap())
            .collect::<Vec<u64>>();

        // 1) write the values to data files
        let mut futs = Vec::with_capacity(self.table_names.len());
        for ((file, &offset), value) in self
            .table_files
            .iter()
            .zip(self.write_offsets.iter())
            .zip(values.into_iter())
        {
            let file = file.clone();
            futs.push(async move { read_write_at(&file, &value, offset).await });
        }
        futures::future::try_join_all(futs)
            .await
            .context("write to table data files")?;

        // 2) write to the table offset files
        let offset_write_offset = self.length * 8;
        let mut futs = Vec::with_capacity(self.table_names.len());
        for (file, &offset) in self
            .table_offsets_files
            .iter()
            .zip(new_write_offsets.iter())
        {
            let file = file.clone();
            futs.push(async move {
                read_write_at(&file, &offset.to_be_bytes(), offset_write_offset).await
            });
        }
        futures::future::try_join_all(futs)
            .await
            .context("write to table offset files")?;

        // 3) write to the keys file
        read_write_at(&self.keys_file, &key.to_be_bytes(), offset_write_offset)
            .await
            .context("write to the keys file")?;

        // 4) create a new length file and rename it onto the old one
        let mut path = self.path.clone();
        path.push("new_length");
        glommio::io::remove(&path).await.ok();
        let mut sink = ImmutableFileBuilder::new(&path)
            .build_sink()
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("build new length file")?;
        sink.write_all(&(self.length + 1).to_be_bytes())
            .await
            .context("write to new length file")?;
        sink.sync()
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("sync new length file to disk")?;
        sink.close()
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("close new length file")?;
        let mut final_path = self.path.clone();
        final_path.push("length");
        glommio::io::rename(&path, &final_path)
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("rename length file")?;

        // 5) update write offsets for future writes
        self.write_offsets = new_write_offsets;

        // 6) update length for future writes
        self.length += 1;

        // 7) write the offsets into the in memory table_offsets
        for (offsets, &offset) in self.table_offsets.iter_mut().zip(self.write_offsets.iter()) {
            offsets.append(&[offset]);
        }

        // 8) write the key into in memory keys
        self.keys.append(&[key]);

        Ok(())
    }

    /// Use to close the underlying file handles explicitly.
    ///
    /// Can be useful in a situation that opens/closes writers rapidly.
    ///
    /// # Panic
    ///
    /// Panics if there is any active write operation on this reader
    pub async fn close(self) -> Result<()> {
        futures::future::try_join_all(
            self.table_files
                .into_iter()
                .map(|f| Rc::try_unwrap(f).expect("unwrap file Rc").close()),
        )
        .await
        .map_err(|e| anyhow!("{}", e))
        .context("close all files")?;

        Ok(())
    }
}

// Utility function for direct_io write.
// Since we need to write a multiple of block size we might need to read some remainder data
// and combine it with our write.
async fn read_write_at(file: &DmaFile, data: &[u8], pos: u64) -> Result<()> {
    let write_pos = file.align_down(pos);
    assert!(write_pos <= pos);

    let extra_read_size = usize::try_from(pos - write_pos).unwrap();

    let bufsize = extra_read_size + data.len() + 8;
    let mut buf = file.alloc_dma_buffer(
        file.align_up(bufsize.try_into().unwrap())
            .try_into()
            .unwrap(),
    );

    if extra_read_size > 0 {
        let read_buf = file
            .read_at(write_pos, extra_read_size)
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("read extra data for alignment")?;

        if read_buf.len() != extra_read_size {
            return Err(anyhow!("failed to read extra data, size mismatch"));
        }

        buf.write_at(0, &*read_buf);
    }

    buf.write_at(extra_read_size, data);
    file.write_at(buf, write_pos)
        .await
        .map_err(|e| anyhow!("{}", e))
        .context("failed to write data")?;
    file.fdatasync()
        .await
        .map_err(|e| anyhow!("{}", e))
        .context("fdatasync file")?;

    Ok(())
}
