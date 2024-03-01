use std::path::{Path, PathBuf};

#[derive(Debug, Default, Clone, PartialEq, derive_builder::Builder)]
pub struct Config {
    path: PathBuf,
    create_if_not_exists: bool,
    tables: Vec<String>,
    segment_length: u32,
}

impl Config {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn create_if_not_exists(&self) -> bool {
        self.create_if_not_exists
    }

    pub fn tables(&self) -> &[String] {
        &self.tables
    }

    pub fn segment_length(&self) -> u32 {
        self.segment_length
    }
}
