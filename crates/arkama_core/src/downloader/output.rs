use crate::http::HttpMeta;
use eyre::{Context, Result};
use fs2::FileExt;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use url::Url;

fn infer_filename(url: &Url, meta: &HttpMeta) -> String {
    if let Some(name) = &meta.filename {
        return name.clone();
    }

    let path = url.path();
    if let Some(last) = path.rsplit('/').next().filter(|last| !last.is_empty()) {
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
    if filename == "." || filename == ".." || filename.contains(['/', '\\', ':']) {
        return Err(eyre::eyre!("server filename is not a plain file name"));
    }
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

pub(crate) fn sidecar_path(output: &Path, suffix: &str) -> Result<PathBuf> {
    let mut name = output
        .file_name()
        .ok_or_else(|| eyre::eyre!("output file name missing"))?
        .to_os_string();
    name.push(suffix);
    Ok(output.with_file_name(name))
}

// The lock file is intentionally never unlinked: all attempts must lock the same inode.
pub(crate) fn lock_output(output: &Path) -> Result<fs::File> {
    let name = output
        .file_name()
        .ok_or_else(|| eyre::eyre!("output file name missing"))?
        .to_string_lossy()
        .to_ascii_lowercase();
    if [".arkama.part", ".arkama.state", ".arkama.lock"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
    {
        return Err(eyre::eyre!("output uses a reserved Arkama artifact suffix"));
    }
    ensure_parent_dir(output)?;
    for suffix in [".arkama.lock", ".arkama.part", ".arkama.part.arkama.state"] {
        let path = sidecar_path(output, suffix)?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(eyre::eyre!(
                    "Arkama artifact is not a regular file: {path:?}"
                ));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    let path = sidecar_path(output, ".arkama.lock")?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.try_lock_exclusive()
        .context("output is already owned by another download")?;
    if output.try_exists()? {
        return Err(eyre::eyre!(
            "output already exists; choose a new output path"
        ));
    }
    Ok(file)
}

pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub(crate) fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_parent(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn publish_output(partial: &Path, output: &Path) -> Result<()> {
    OpenOptions::new().write(true).open(partial)?.sync_all()?;
    // Creating a link is atomic and never replaces an unrelated destination, unlike rename.
    fs::hard_link(partial, output).context("failed to publish output without overwriting")?;
    sync_parent(output)?;
    remove_if_exists(partial)?;
    Ok(())
}
