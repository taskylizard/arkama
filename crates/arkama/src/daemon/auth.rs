//! Single-owner credentials. Never log key contents or include them in errors.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use eyre::{Result, eyre};

pub(super) fn key_path() -> Result<PathBuf> {
    Ok(arkama_data::data_dir()?.join("credentials").join("api.key"))
}

pub(super) fn protect_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(eyre!("credential directory must not be a link"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(windows)]
    {
        let reset = std::process::Command::new("icacls")
            .arg(path)
            .arg("/reset")
            .output()?;
        if !reset.status.success() {
            return Err(eyre!("cannot reset credential directory permissions"));
        }
        let user = std::process::Command::new("whoami")
            .args(["/user", "/fo", "csv", "/nh"])
            .output()?;
        if !user.status.success() {
            return Err(eyre!("cannot resolve credential owner"));
        }
        let text = String::from_utf8(user.stdout)?;
        let sid = text
            .trim()
            .rsplit(',')
            .next()
            .unwrap_or_default()
            .trim_matches('"');
        if !sid.starts_with("S-1-")
            || !sid
                .chars()
                .all(|c| c.is_ascii_digit() || c == 'S' || c == '-')
        {
            return Err(eyre!("cannot resolve credential owner"));
        }
        let result = std::process::Command::new("icacls")
            .arg(path)
            .args([
                "/inheritance:r",
                "/grant:r",
                &format!("*{sid}:(OI)(CI)F"),
                "/remove:g",
                "*S-1-1-0",
                "*S-1-5-11",
                "*S-1-5-32-545",
            ])
            .output()?;
        if !result.status.success() {
            return Err(eyre!("cannot protect credential directory"));
        }
    }
    Ok(())
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
