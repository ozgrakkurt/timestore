use anyhow::Result;

use crate::{Reader, Writer, Config};

pub async fn open(_cfg: &Config) -> Result<(Writer, Reader)> {
    todo!()
}
