//! Minimal ordinary-file context. Serialization is sensitive; debugging is redacted.

use std::collections::BTreeMap;
use std::fmt;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};

/// Explicit browser context for replayable GET downloads, including HEAD/range probes.
///
/// Headers accept `authorization`, `accept`, `accept-language`, and `x-*` names. Transport,
/// cookies, referrer, user agent and range headers have dedicated fields or engine ownership.
/// All context headers/cookies/referrer are omitted outside the original origin, even on a
/// redirect back to it. There is no cookie jar or form/body replay. Source is metadata only.
/// `Serialize` contains secrets: use it only for transport/protected persistence, never logs.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
#[derive(Clone)]
pub struct RequestContext {
    pub method: String,
    pub headers: BTreeMap<String, String>,
    pub cookies: Option<String>,
    pub referrer: Option<String>,
    pub filename: Option<String>,
    pub source: Option<String>,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            method: "GET".into(),
            headers: BTreeMap::new(),
            cookies: None,
            referrer: None,
            filename: None,
            source: None,
        }
    }
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestContext")
            .field("method", &"[redacted]")
            .field("headers", &self.headers.len())
            .field("cookies", &self.cookies.is_some())
            .field("referrer", &self.referrer.is_some())
            .field("filename", &self.filename.is_some())
            .field("source", &self.source.is_some())
            .finish()
    }
}

impl RequestContext {
    /// Validates sizes and supported context before any request or durable admission.
    pub fn validate(&self) -> crate::Result<()> {
        let invalid = || crate::Error::InvalidRequest {
            message: "invalid or unsupported request context; only GET handoffs are supported"
                .into(),
        };
        if self.method != "GET"
            || self.headers.len() > 16
            || self
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 8192
            || self.cookies.as_ref().is_some_and(|v| v.len() > 4096)
            || self.referrer.as_ref().is_some_and(|v| v.len() > 8192)
            || self.source.as_ref().is_some_and(|v| v.len() > 8192)
        {
            return Err(invalid());
        }
        let mut names = std::collections::BTreeSet::new();
        for (name, value) in &self.headers {
            let name = name.to_ascii_lowercase();
            if name.len() > 128
                || !(matches!(
                    name.as_str(),
                    "authorization" | "accept" | "accept-language"
                ) || name.starts_with("x-"))
                || !names.insert(name.clone())
                || HeaderName::from_bytes(name.as_bytes()).is_err()
                || HeaderValue::from_str(value).is_err()
            {
                return Err(invalid());
            }
        }
        if self
            .cookies
            .as_ref()
            .is_some_and(|v| HeaderValue::from_str(v).is_err())
        {
            return Err(invalid());
        }
        for value in [&self.referrer, &self.source].into_iter().flatten() {
            let url = url::Url::parse(value).map_err(|_| invalid())?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(invalid());
            }
        }
        if self.filename.as_ref().is_some_and(|name| {
            name.is_empty()
                || name.len() > 240
                || name == "."
                || name == ".."
                || name.contains(['/', '\\', ':'])
                || name.chars().any(char::is_control)
        }) {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in &self.headers {
            let mut value = HeaderValue::from_str(value).expect("validated header");
            value.set_sensitive(true);
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("validated name"),
                value,
            );
        }
        for (name, value) in [("cookie", &self.cookies), ("referer", &self.referrer)] {
            if let Some(value) = value {
                let value = if name == "referer" {
                    let mut url = url::Url::parse(value).expect("validated referrer");
                    url.set_fragment(None);
                    url.to_string()
                } else {
                    value.clone()
                };
                let mut value = HeaderValue::from_str(&value).expect("validated context");
                value.set_sensitive(true);
                headers.insert(HeaderName::from_static(name), value);
            }
        }
        headers
    }

    pub(crate) fn identity(&self, url: &str, user_agent: Option<&str>) -> String {
        let mut hash = Sha256::new();
        // Length framing distinguishes every field; names are ordered by the request's map.
        for value in std::iter::once(url)
            .chain(std::iter::once(user_agent.unwrap_or_default()))
            .chain(
                self.headers
                    .iter()
                    .flat_map(|(k, v)| [k.as_str(), v.as_str()]),
            )
            .chain([
                self.cookies.as_deref().unwrap_or_default(),
                self.referrer.as_deref().unwrap_or_default(),
            ])
        {
            hash.update((value.len() as u64).to_le_bytes());
            hash.update(value.as_bytes());
        }
        format!("{:x}", hash.finalize())
    }
}

/// A display-only origin. Paths, userinfo, queries and fragments are never exposed.
pub fn redact_url(value: &str) -> String {
    url::Url::parse(value)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
        .map(|url| format!("{}/[redacted]", url.origin().ascii_serialization()))
        .unwrap_or_else(|| "[redacted]".into())
}

pub(crate) fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
