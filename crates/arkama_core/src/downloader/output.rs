use crate::http::HttpMeta;
use eyre::{Context, Result};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use url::Url;

fn infer_filename(url: &Url, meta: &HttpMeta) -> String {
    if let Some(name) = &meta.filename {
        return name.clone();
    }

    let path = url.path();
    if let Some(last) = path.rsplit('/').next()
        && !last.is_empty()
    {
        return last.to_string();
    }

    "download".to_string()
}

pub(crate) fn determine_output(
    url: &Url,
    meta: &HttpMeta,
    output: Option<PathBuf>,
    output_dir: Option<PathBuf>,
) -> Result<PathBuf> {
    if let Some(path) = output {
        if path.is_dir() {
            return Err(eyre::eyre!("output path is a directory"));
        }
        return Ok(path);
    }

    let filename = infer_filename(url, meta);
    if let Some(dir) = output_dir {
        return Ok(dir.join(filename));
    }

    Ok(PathBuf::from(filename))
}

pub(crate) fn prepare_output(path: &Path, total: u64) -> Result<()> {
    ensure_parent_dir(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open output file {path:?}"))?;
    file.set_len(total)
        .with_context(|| format!("failed to allocate output file {path:?}"))?;
    Ok(())
}

pub(crate) fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create output dir {parent:?}"))?;
    Ok(())
}
