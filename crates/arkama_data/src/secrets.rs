//! Authenticated request storage. A database alone cannot recover its replay credentials.

use std::io::{Read, Write};
use std::path::Path;

use aes_gcm::{Aes256Gcm, KeyInit, Nonce, aead::Aead};
use base64::{Engine, engine::general_purpose::STANDARD};
use eyre::{Result, eyre};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::Zeroizing;

pub(crate) const PREFIX: &str = "arkama:v1:";

pub(crate) struct Secrets {
    key: Zeroizing<[u8; 32]>,
    cipher: Aes256Gcm,
}

impl Secrets {
    pub(crate) fn open(database: &Path, encrypted_exists: bool) -> Result<Self> {
        let directory = database.with_extension("credentials");
        protect_directory(&directory)?;
        let path = directory.join("request.key");
        if !path.try_exists()? {
            if encrypted_exists {
                return Err(eyre!(
                    "request storage key missing; restore it with the database"
                ));
            }
            let mut key = Zeroizing::new([0u8; 32]);
            getrandom::fill(key.as_mut()).map_err(|_| eyre!("secure randomness unavailable"))?;
            let mut file = tempfile::NamedTempFile::new_in(&directory)?;
            file.write_all(key.as_ref())?;
            file.as_file().sync_all()?;
            if let Err(error) = file.persist_noclobber(&path) {
                if error.error.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(eyre!("cannot publish request storage key"));
                }
            }
            #[cfg(unix)]
            std::fs::File::open(&directory)?.sync_all()?;
        }
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != 32 {
            return Err(eyre!("invalid request storage key"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(eyre!("request key permissions are too broad"));
            }
        }
        let mut key = Zeroizing::new([0u8; 32]);
        std::fs::File::open(path)?.read_exact(key.as_mut())?;
        let cipher = Aes256Gcm::new_from_slice(key.as_ref()).expect("32 byte key");
        Ok(Self { key, cipher })
    }

    pub(crate) fn seal(&self, data: &str) -> Result<String> {
        let mut nonce = [0u8; 12];
        getrandom::fill(&mut nonce).map_err(|_| eyre!("secure randomness unavailable"))?;
        let ciphertext = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), data.as_bytes())
            .map_err(|_| eyre!("cannot protect request"))?;
        let mut bytes = nonce.to_vec();
        bytes.extend(ciphertext);
        Ok(format!("{PREFIX}{}", STANDARD.encode(bytes)))
    }

    pub(crate) fn unseal(&self, data: &str) -> Result<Zeroizing<String>> {
        let bytes = STANDARD
            .decode(
                data.strip_prefix(PREFIX)
                    .ok_or_else(|| eyre!("invalid protected request"))?,
            )
            .map_err(|_| eyre!("invalid protected request"))?;
        if bytes.len() < 28 {
            return Err(eyre!("invalid protected request"));
        }
        let plain = self
            .cipher
            .decrypt(Nonce::from_slice(&bytes[..12]), &bytes[12..])
            .map_err(|_| eyre!("request integrity/key check failed"))?;
        Ok(Zeroizing::new(
            String::from_utf8(plain).map_err(|_| eyre!("invalid protected request"))?,
        ))
    }

    pub(crate) fn fingerprint(&self, domain: &str, data: &str) -> String {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(self.key.as_ref()).expect("HMAC key");
        mac.update(domain.as_bytes());
        mac.update(&[0]);
        mac.update(data.as_bytes());
        STANDARD.encode(mac.finalize().into_bytes())
    }
}

/// Owner-only credential directories on Unix/Windows. Trust the owner-controlled filesystem.
pub fn protect_directory(path: &Path) -> Result<()> {
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
