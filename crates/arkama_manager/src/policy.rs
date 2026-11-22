//! Owner-selected filesystem/network policy; never persisted or supplied by client commands.

use std::path::{Component, Path, PathBuf};

use arkama_core::DestinationPolicy;

use crate::{Error, Result};

/// Trusted embedded mode is unrestricted. Server hosts must select `restricted` at startup.
#[derive(Clone, Default)]
pub struct ManagerPolicy {
    pub(crate) roots: Vec<PathBuf>,
    pub(crate) destination: DestinationPolicy,
}

impl ManagerPolicy {
    /// Roots must already exist, be directories, and be outside the store/credential directory.
    pub fn restricted(roots: Vec<PathBuf>, private_origins: &[String]) -> Result<Self> {
        if roots.is_empty() || roots.len() > 16 {
            return Err(Error::new("invalid", "select 1–16 output roots"));
        }
        let mut canonical = Vec::new();
        for root in roots {
            let root = root
                .canonicalize()
                .map_err(|_| Error::new("invalid", "output root must exist"))?;
            if !root.is_dir() {
                return Err(Error::new("invalid", "output root must be a directory"));
            }
            canonical.push(root);
        }
        Ok(Self {
            roots: canonical,
            destination: DestinationPolicy::public_only(private_origins)
                .map_err(|_| Error::new("invalid", "invalid private-source origins"))?,
        })
    }

    pub(crate) fn directory(&self, path: &Path) -> Result<PathBuf> {
        if self.roots.is_empty() {
            return Ok(path.to_path_buf());
        }
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|_| Error::new("storage", "cannot resolve current directory"))?
                .join(path)
        };
        // Canonical Windows roots use verbatim prefixes; ordinary absolute client paths do not.
        #[cfg(windows)]
        let path = {
            let text = path.to_string_lossy();
            if text.starts_with(r"\\?\") {
                path
            } else if let Some(unc) = text.strip_prefix(r"\\") {
                PathBuf::from(format!(r"\\?\UNC\{unc}"))
            } else {
                PathBuf::from(format!(r"\\?\{text}"))
            }
        };
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(Error::new("forbidden", "parent traversal is not allowed"));
        }
        let root = self
            .roots
            .iter()
            .filter(|root| path.starts_with(root))
            .max_by_key(|root| root.components().count())
            .ok_or_else(|| Error::new("forbidden", "output is outside authorized roots"))?;
        let mut current = root.clone();
        for component in path.strip_prefix(root).expect("root prefix").components() {
            if let Component::Normal(name) = component {
                validate_filename(
                    name.to_str()
                        .ok_or_else(|| Error::new("invalid", "path must be UTF-8"))?,
                )?;
                current.push(name);
                match std::fs::symlink_metadata(&current) {
                    Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
                        return Err(Error::new(
                            "forbidden",
                            "output path crosses a link or non-directory",
                        ));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        std::fs::create_dir(&current)
                            .map_err(|_| Error::new("storage", "cannot create output directory"))?
                    }
                    Err(_) => return Err(Error::new("storage", "cannot inspect output directory")),
                }
            }
        }
        let canonical = current
            .canonicalize()
            .map_err(|_| Error::new("storage", "cannot resolve output directory"))?;
        if !canonical.starts_with(root) {
            return Err(Error::new("forbidden", "output escaped authorized root"));
        }
        Ok(canonical)
    }
}

pub(crate) fn validate_filename(name: &str) -> Result<()> {
    let lower = name.to_ascii_lowercase();
    let stem = lower
        .split('.')
        .next()
        .unwrap_or_default()
        .trim_end_matches(' ');
    if name.is_empty()
        || name.len() > 240
        || name == "."
        || name == ".."
        || name.ends_with(['.', ' '])
        || name
            .chars()
            .any(|c| c.is_control() || "/\\:<>\"|?*".contains(c))
        || ["con", "prn", "aux", "nul", "clock$", "conin$", "conout$"].contains(&stem)
        || ["com", "lpt"].iter().any(|prefix| {
            stem.strip_prefix(prefix).is_some_and(|suffix| {
                matches!(
                    suffix,
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
        })
        || [
            ".arkama.part",
            ".arkama.state",
            ".arkama.lock",
            ".manager.lock",
        ]
        .iter()
        .any(|suffix| lower.ends_with(suffix))
    {
        return Err(Error::new(
            "invalid",
            "filename is not a portable plain file name",
        ));
    }
    Ok(())
}
