use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::{Context, Result, anyhow};
use futures::AsyncReadExt;
use glommio::io::{DmaFile, ImmutableFileBuilder, OpenOptions};

use crate::{Config, Reader, Writer};

// 1) read length file
// 2) open and validate keys file
// 3) open and validate offset files
// 4) open and validate data files
// 5) create writer and reader

pub async fn open(cfg: Config) -> Result<(WriterFactory, ReaderFactory)> {
    if cfg.create_if_not_exists() {
        create_dir_all(cfg.path()).context("create dir if not exists")?;

        let mut path = cfg.path().to_owned();
        path.push("length");
        let file = create_if_not_exists(&path)
            .await
            .context("create length file if not exists")?;
        let size = file
            .file_size()
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("read length file size")?;
        if size == 0 {
            let mut buf = file.alloc_dma_buffer(usize::try_from(file.align_up(8)).unwrap());
            buf.as_bytes_mut().fill(0);
            file.write_at(buf, 0)
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("write zero to length file")?;
        }
        file.close()
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("close length file")?;

        let mut path = cfg.path().to_owned();
        path.push("keys");
        let file = create_if_not_exists(&path)
            .await
            .context("create length file if not exists")?;
        file.close()
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("close keys file")?;

        for name in cfg.tables().iter() {
            let mut path = cfg.path().to_owned();
            path.push(name.as_str());

            create_dir_all(&path).context("create table dir if not exists")?;

            path.push("offsets");
            let file = create_if_not_exists(&path)
                .await
                .context("create offsets file if not exists")?;
            file.close()
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("close offsets file")?;

            path.pop();
            path.push("data");

            let file = create_if_not_exists(&path)
                .await
                .context("create data file if not exists")?;
            file.close()
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("close data file")?;
        }
    }

    let length = {
        let mut path = cfg.path().to_owned();
        path.push("length");
        let buf = read_file(&path, 8).await.context("read length file")?;
        u64::from_be_bytes(buf.try_into().unwrap())
    };
    let len = usize::try_from(length).unwrap();

    let segment_len = usize::try_from(cfg.segment_length()).unwrap();

    let (keys_writer, keys_reader) = {
        let mut keys = caos::new::<u64>(segment_len);

        let mut path = cfg.path().to_owned();
        path.push("keys");

        let vals = load_ordered_u64_file(&path, len)
            .await
            .context("read keys file")?;

        keys.0.append(&vals);

        keys
    };

    let mut table_offset_writers = Vec::with_capacity(cfg.tables().len());
    let mut table_offset_readers = Vec::with_capacity(cfg.tables().len());
    let mut max_offsets = Vec::with_capacity(cfg.tables().len());
    for name in cfg.tables().iter() {
        let mut path = cfg.path().to_owned();
        path.push(name.as_str());
        path.push("offsets");

        let vals = load_ordered_u64_file(&path, len)
            .await
            .with_context(|| format!("failed to load offstes of table '{}'", name.as_str()))?;

        let mut offsets = caos::new::<u64>(segment_len);

        max_offsets.push(vals.last().copied().unwrap_or(0));

        offsets.0.append(&vals);

        table_offset_writers.push(offsets.0);
        table_offset_readers.push(offsets.1);
    }

    for (name, &max_offset) in cfg.tables().iter().zip(max_offsets.iter()) {
        let mut path = cfg.path().to_owned();
        path.push(name.as_str());
        path.push("data");

        let file = ImmutableFileBuilder::new(&path)
            .build_existing()
            .await
            .map_err(|e| anyhow!("{}", e))
            .context("open data file")?;
        if file.file_size() < max_offset {
            return Err(anyhow!(
                "data file size is smaller than maximum offset found in offsets for table '{}'",
                name
            ));
        }
    }

    let writer_factory = WriterFactory {
        path: cfg.path().to_owned(),
        keys: keys_writer,
        table_offsets: table_offset_writers,
        table_names: cfg.tables().to_vec(),
        write_offsets: max_offsets,
        length,
    };

    let reader_factory = ReaderFactory {
        path: cfg.path().to_owned(),
        keys: keys_reader,
        table_offsets: table_offset_readers,
        table_names: cfg.tables().to_vec(),
    };

    Ok((writer_factory, reader_factory))
}

async fn create_if_not_exists(path: &Path) -> Result<DmaFile> {
    let mut opts = OpenOptions::new();
    opts.create(true).read(true).write(true);
    opts.dma_open(path)
        .await
        .map_err(|e| anyhow!("{}", e))
        .context("open file")
}

#[derive(Clone)]
pub struct ReaderFactory {
    path: PathBuf,
    keys: caos::Reader<u64>,
    table_offsets: Vec<caos::Reader<u64>>,
    table_names: Vec<String>,
}

impl ReaderFactory {
    pub async fn make(&self) -> Result<Reader> {
        let mut table_files = Vec::with_capacity(self.table_names.len());

        for name in self.table_names.iter() {
            let mut path = self.path.clone();
            path.push(name.as_str());
            path.push("data");

            let file = DmaFile::open(&path)
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("open data file")?;

            table_files.push(Rc::new(file));
        }

        Ok(Reader {
            keys: self.keys.clone(),
            table_offsets: self.table_offsets.clone(),
            table_names: self.table_names.clone(),
            table_files,
        })
    }
}

pub struct WriterFactory {
    path: PathBuf,
    keys: caos::Writer<u64>,
    table_offsets: Vec<caos::Writer<u64>>,
    table_names: Vec<String>,
    write_offsets: Vec<u64>,
    length: u64,
}

impl WriterFactory {
    pub async fn make(self) -> Result<Writer> {
        let mut opts = OpenOptions::new();
        opts.write(true);
        opts.read(true);

        let keys_file = {
            let mut path = self.path.clone();
            path.push("keys");

            let file = opts
                .dma_open(&path)
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("open keys file")?;

            Rc::new(file)
        };

        let mut table_files = Vec::with_capacity(self.table_names.len());
        let mut table_offsets_files = Vec::with_capacity(self.table_names.len());

        for name in self.table_names.iter() {
            let mut path = self.path.clone();
            path.push(name.as_str());
            path.push("data");

            let file = opts
                .dma_open(&path)
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("open data file")?;

            table_files.push(Rc::new(file));

            let mut path = self.path.clone();
            path.push(name.as_str());
            path.push("offsets");

            let file = opts
                .dma_open(&path)
                .await
                .map_err(|e| anyhow!("{}", e))
                .context("open data file")?;

            table_offsets_files.push(Rc::new(file));
        }

        Ok(Writer {
            path: self.path,
            keys: self.keys,
            keys_file,
            table_offsets: self.table_offsets,
            table_offsets_files,
            table_names: self.table_names,
            table_files,
            write_offsets: self.write_offsets,
            length: self.length,
        })
    }
}

async fn load_ordered_u64_file(path: &Path, len: usize) -> Result<Vec<u64>> {
    let buf = read_file(path, len * 8).await.context("read file")?;

    let mut prev = 0;
    let mut vals = Vec::with_capacity(len);

    for chunk in buf.chunks_exact(8).take(len) {
        let val = u64::from_be_bytes(chunk.try_into().unwrap());

        if prev > val {
            return Err(anyhow!("ordering error found. {} > {}", prev, val));
        }

        prev = val;
        vals.push(val);
    }

    if vals.len() != len {
        return Err(anyhow!("length is invalid"));
    }

    Ok(vals)
}

async fn read_file(path: &Path, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; len];

    let mut file = ImmutableFileBuilder::new(&path)
        .build_existing()
        .await
        .map_err(|e| anyhow!("{}", e))
        .context("open file")?
        .stream_reader()
        .with_buffer_size(512 * 1024)
        .with_read_ahead(8)
        .build();

    file.read_exact(&mut buf).await.context("read contents")?;

    file.close()
        .await
        .map_err(|e| anyhow!("{}", e))
        .context("close file")?;

    Ok(buf)
}
