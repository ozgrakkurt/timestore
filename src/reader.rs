use std::rc::Rc;

use anyhow::{Context, Result, anyhow};
use futures::{AsyncReadExt, Stream, StreamExt};
use glommio::io::{
    DmaFile, DmaStreamReader, ImmutableFileBuilder, IoVec, MergedBufferLimit,
    ReadAmplificationLimit, ReadResult,
};

#[derive(Clone)]
pub struct Reader {
    pub(crate) keys: caos::Reader<u64>,
    pub(crate) table_offsets: Vec<caos::Reader<u64>>,
    pub(crate) table_names: Vec<String>,
    pub(crate) table_files: Vec<Rc<DmaFile>>,
}

impl Reader {
    pub fn keys(&self) -> caos::Reader<u64> {
        self.keys.clone()
    }

    pub fn table_names(&self) -> &[String] {
        &self.table_names
    }

    /// Use to close the underlying file handles explicitly.
    ///
    /// Can be useful in a situation that opens/closes readers rapidly.
    ///
    /// # Panic
    ///
    /// Panics if there is an active iterator or any other read operation on this reader
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

    pub async fn iter(&self, params: IterParams<'_>) -> Result<Option<Iter>> {
        let pos = match self.keys.next_position(params.from) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let stream_reader = if let Some(table) = params.table {
            let (file, offsets) = self.get_file_and_offsets(table)?;

            let path = file.path().context("get path of table file")?.to_owned();
            let stream_reader = ImmutableFileBuilder::new(&path)
                .with_buffer_size(params.buffer_size)
                .with_sequential_concurrency(params.concurrency)
                .build_existing()
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("open table file")?
                .stream_reader()
                .with_buffer_size(params.buffer_size)
                .with_read_ahead(params.concurrency)
                .build();

            let io_vecs = IoVecIter::from_caos_and_position(offsets, pos);

            Some((stream_reader, io_vecs))
        } else {
            None
        };

        let (current_key, keys) = if pos == 0 {
            (0, self.keys.iter_from(0))
        } else {
            let mut iter = self.keys.iter_from(pos - 1);
            let current_key = iter.next().unwrap();

            (current_key, iter)
        };

        let table_io_vecs = self
            .table_offsets
            .iter()
            .map(|offsets| IoVecIter::from_caos_and_position(offsets.clone(), pos))
            .collect();

        let to = std::cmp::min(params.to, self.keys.last().unwrap_or(0));

        Ok(Some(Iter {
            started: false,
            current_key,
            keys,
            stream_reader,
            table_io_vecs,
            current_table_io_vecs: self.table_names.iter().map(|_| (0, 0)).collect(),
            to,
            table_names: self.table_names.clone(),
            table_files: self.table_files.clone(),
        }))
    }

    pub async fn read_many<V, S>(
        &self,
        table: &str,
        key: u64,
        iovs: S,
        concurrency: usize,
        buffer_limit: MergedBufferLimit,
        read_amp_limit: ReadAmplificationLimit,
    ) -> Result<Option<impl Stream<Item = Result<ReadResult>>>>
    where
        V: IoVec + Unpin,
        S: Stream<Item = V> + Unpin,
    {
        let (file, offsets) = self.get_file_and_offsets(table)?;

        let pos = match self.keys.position(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let base_offset = if pos == 0 {
            0
        } else {
            offsets.iter_from(pos - 1).next().unwrap()
        };

        let iovs = iovs.map(move |iov| (iov.pos() + base_offset, iov.size()));

        Ok(Some(
            file.read_many(iovs, buffer_limit, read_amp_limit)
                .with_concurrency(concurrency)
                .map(|res| match res {
                    Ok((_, buf)) => Ok(buf),
                    Err(e) => Err(anyhow!("{}", e).context("read from file")),
                }),
        ))
    }

    pub async fn read(&self, table: &str, key: u64) -> Result<Option<ReadResult>> {
        let (table_file, table_offsets) = self.get_file_and_offsets(table)?;

        let pos = match self.keys.position(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let (pos, len) = if pos == 0 {
            let len = table_offsets.iter_from(0).next().unwrap();
            (0, len)
        } else {
            let mut iter = table_offsets.iter_from(pos - 1);
            let start = iter.next().unwrap();
            let end = iter.next().unwrap();

            (start, end - start)
        };

        table_file
            .read_at(pos, usize::try_from(len).unwrap())
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("read from file")
            .map(Some)
    }

    fn get_file_and_offsets(&self, table: &str) -> Result<(Rc<DmaFile>, caos::Reader<u64>)> {
        match self.table_names.iter().position(|n| table == n) {
            Some(pos) => Ok((
                self.table_files.get(pos).unwrap().clone(),
                self.table_offsets.get(pos).unwrap().clone(),
            )),
            None => Err(anyhow!("table '{}' not found", table)),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, derive_builder::Builder)]
pub struct IterParams<'input> {
    from: u64,
    to: u64,
    #[builder(default)]
    table: Option<&'input str>,
    #[builder(default = "512 * 1024")]
    buffer_size: usize,
    #[builder(default = "8")]
    concurrency: usize,
}

struct IoVecIter {
    inner: caos::Iter<u64>,
    start: u64,
}

impl IoVecIter {
    fn from_caos_and_position(caos: caos::Reader<u64>, pos: usize) -> Self {
        if pos == 0 {
            Self {
                start: 0,
                inner: caos.iter_from(0),
            }
        } else {
            let mut inner = caos.iter_from(pos - 1);
            let start = inner.next().unwrap();
            Self { start, inner }
        }
    }
}

impl Iterator for IoVecIter {
    type Item = (u64, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let end = self.inner.next()?;

        let iovec = (self.start, usize::try_from(end - self.start).unwrap());

        self.start = end;

        Some(iovec)
    }
}

pub struct Iter {
    started: bool,
    current_key: u64,
    keys: caos::Iter<u64>,
    stream_reader: Option<(DmaStreamReader, IoVecIter)>,
    table_io_vecs: Vec<IoVecIter>,
    current_table_io_vecs: Vec<(u64, usize)>,
    to: u64,
    table_names: Vec<String>,
    table_files: Vec<Rc<DmaFile>>,
}

impl Iter {
    pub async fn next(&mut self) -> Result<Option<((u64, u64), Vec<u8>)>> {
        self.started = true;

        if self.current_key >= self.to {
            return Ok(None);
        }

        let next_key = match self.keys.next() {
            Some(next_key) => next_key,
            None => return Ok(None),
        };

        for (current_io_vec, io_vecs) in self
            .current_table_io_vecs
            .iter_mut()
            .zip(self.table_io_vecs.iter_mut())
        {
            *current_io_vec = io_vecs.next().unwrap();
        }

        let prev_key = self.current_key;
        self.current_key = next_key;

        let buf = if let Some((reader, io_vecs)) = &mut self.stream_reader {
            let (_, len) = io_vecs.next().unwrap();
            let mut buf = vec![0; len];
            reader
                .read_exact(&mut buf)
                .await
                .context("read from file")?;

            buf
        } else {
            Vec::new()
        };

        Ok(Some(((prev_key, self.current_key), buf)))
    }

    pub async fn read(&self, table: &str) -> Result<ReadResult> {
        if !self.started {
            return Err(anyhow!(
                "iter.next has to be called before calling read or read_many"
            ));
        }

        let (file, io_vec) = self.get_file_and_io_vec(table)?;

        file.read_at(io_vec.0, io_vec.1)
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("read from file")
    }

    pub async fn read_many<V, S>(
        &self,
        table: &str,
        iovs: S,
        buffer_limit: MergedBufferLimit,
        read_amp_limit: ReadAmplificationLimit,
    ) -> Result<impl Stream<Item = Result<ReadResult>>>
    where
        V: IoVec + Unpin,
        S: Stream<Item = V> + Unpin,
    {
        if !self.started {
            return Err(anyhow!(
                "iter.next has to be called before calling read or read_many"
            ));
        }

        let (file, base_io_vec) = self.get_file_and_io_vec(table)?;

        let iovs = iovs.map(move |iov| (iov.pos() + base_io_vec.0, iov.size()));

        Ok(file
            .read_many(iovs, buffer_limit, read_amp_limit)
            .map(|res| match res {
                Ok((_, buf)) => Ok(buf),
                Err(e) => Err(anyhow!("{}", e).context("read from file")),
            }))
    }

    fn get_file_and_io_vec(&self, table: &str) -> Result<(Rc<DmaFile>, (u64, usize))> {
        match self.table_names.iter().position(|n| n == table) {
            Some(pos) => Ok((
                self.table_files.get(pos).unwrap().clone(),
                *self.current_table_io_vecs.get(pos).unwrap(),
            )),
            None => Err(anyhow!("table '{}' not found", table)),
        }
    }
}
