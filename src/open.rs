use anyhow::Result;

use crate::{Config, Reader, Writer};

pub async fn open(_cfg: &Config) -> Result<(Writer, Reader)> {
    todo!()
}
