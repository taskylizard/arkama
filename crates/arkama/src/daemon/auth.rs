//! Single-owner credentials. Never log key contents or include them in errors.

use std::io::{Read, Write};
use std::path::PathBuf;

pub(super) use arkama_data::protect_directory;
use eyre::{Result, eyre};

pub(super) fn key_path() -> Result<PathBuf> {
    Ok(arkama_data::data_dir()?.join("credentials").join("api.key"))
}

pub(super) fn random_id() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| eyre!("secure randomness unavailable"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub(super) fn write_key(value: &str) -> Result<()> {
    let path = key_path()?;
    let parent = path.parent().expect("key parent");
    protect_directory(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(value.as_bytes())?;
    file.as_file().sync_all()?;
    file.persist(&path)
        .map_err(|_| eyre!("cannot publish credential file"))?;
    #[cfg(unix)]
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub(super) fn read_key() -> Result<String> {
    let path = key_path()?;
    let metadata = std::fs::symlink_metadata(&path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(eyre!("invalid credential file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(eyre!("credential file permissions are too broad"));
        }
    }
    let mut value = String::new();
    std::fs::File::open(&path)?
        .take(65)
        .read_to_string(&mut value)?;
    if value != "revoked"
        && (value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err(eyre!("invalid credential file"));
    }
    Ok(value)
}

pub(super) fn initialize() -> Result<()> {
    protect_directory(key_path()?.parent().expect("key parent"))?;
    if !key_path()?.try_exists()? {
        write_key(&random_id()?)?;
    }
    read_key()?;
    Ok(())
}

pub(super) fn authorized(header: &str) -> bool {
    let Ok(key) = read_key() else {
        return false;
    };
    let Some(candidate) = header.strip_prefix("Bearer ") else {
        return false;
    };
    if key == "revoked" || candidate.len() != 64 {
        return false;
    }
    // Do not return early on a mismatched key byte.
    key.bytes()
        .zip(candidate.bytes())
        .fold(0u8, |different, (a, b)| different | (a ^ b))
        == 0
}
