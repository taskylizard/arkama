use eyre::{Context, Result};
use reqwest::header::{ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use reqwest::redirect::Policy;
use reqwest::{Client, Response, StatusCode};
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub(crate) struct HttpMeta {
    pub(crate) size: Option<u64>,
    pub(crate) accept_ranges: bool,
    pub(crate) filename: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ClientFactory {
    base: Client,
}

impl ClientFactory {
    pub(crate) fn new(user_agent: Option<&str>, experimental_entropy: bool) -> Result<Self> {
        let base = build_client(user_agent, experimental_entropy)?;
        Ok(Self { base })
    }

    pub(crate) fn client(&self) -> Result<Client> {
        Ok(self.base.clone())
    }
}

fn build_client(user_agent: Option<&str>, experimental_entropy: bool) -> Result<Client> {
    let user_agent = user_agent.unwrap_or("arkama/0.1");
    let mut builder = Client::builder()
        .user_agent(user_agent)
        .redirect(Policy::limited(10));
    if experimental_entropy {
        builder = builder.pool_max_idle_per_host(0);
        builder = builder.pool_idle_timeout(Duration::from_secs(0));
    }
    let client = builder.build().context("failed to build HTTP client")?;
    Ok(client)
}

pub(crate) async fn probe(client: &Client, url: &Url) -> Result<HttpMeta> {
    let mut size = None;
    let mut accept_ranges = false;
    let mut filename = None;

    let head = client.head(url.clone()).send().await;
    let head = head.ok();

    if let Some(resp) = head {
        let headers = resp.headers();
        if let Some(len) = headers.get(CONTENT_LENGTH)
            && let Ok(len) = len.to_str()
            && let Ok(parsed) = len.parse::<u64>()
        {
            size = Some(parsed);
        }
        if let Some(ranges) = headers.get(ACCEPT_RANGES)
            && let Ok(ranges) = ranges.to_str()
            && ranges.to_ascii_lowercase().contains("bytes")
        {
            accept_ranges = true;
        }
        if let Some(name) = filename_from_headers(headers) {
            filename = Some(name);
        }
    }

    if !accept_ranges {
        let probe = client
            .get(url.clone())
            .header(RANGE, "bytes=0-0")
            .send()
            .await;
        let probe = match probe {
            Ok(resp) => resp,
            Err(_) => {
                return Ok(HttpMeta {
                    size,
                    accept_ranges: false,
                    filename,
                });
            }
        };
        if probe.status() == StatusCode::PARTIAL_CONTENT {
            accept_ranges = true;
        }
        if size.is_none() {
            if let Some(total) = total_from_content_range(probe.headers()) {
                size = Some(total);
            } else if let Some(len) = probe.headers().get(CONTENT_LENGTH)
                && let Ok(len) = len.to_str()
                && let Ok(parsed) = len.parse::<u64>()
            {
                size = Some(parsed);
            }
        }
        if filename.is_none()
            && let Some(name) = filename_from_headers(probe.headers())
        {
            filename = Some(name);
        }
    }

    Ok(HttpMeta {
        size,
        accept_ranges,
        filename,
    })
}

pub(crate) async fn get_range(
    client: &Client,
    url: &Url,
    start: u64,
    end: Option<u64>,
) -> Result<Response> {
    let range = match end {
        Some(end) => format!("bytes={start}-{end}"),
        None => format!("bytes={start}-"),
    };
    let resp = client
        .get(url.clone())
        .header(RANGE, range)
        .send()
        .await
        .context("range request failed")?
        .error_for_status()
        .context("range response error")?;
    Ok(resp)
}

pub(crate) async fn get_full(client: &Client, url: &Url) -> Result<Response> {
    let resp = client
        .get(url.clone())
        .send()
        .await
        .context("request failed")?
        .error_for_status()
        .context("response error")?;
    Ok(resp)
}

fn filename_from_headers(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let value = headers.get(CONTENT_DISPOSITION)?;
    let value = value.to_str().ok()?;
    let parts = value.split(';');
    for part in parts {
        let part = part.trim();
        let lower = part.to_ascii_lowercase();
        if lower.starts_with("filename=") {
            let mut name = part.trim_start_matches("filename=");
            if name.starts_with('"') && name.ends_with('"') && name.len() >= 2 {
                name = &name[1..name.len() - 1];
            }
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }
    None
}

fn total_from_content_range(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let value = headers.get(CONTENT_RANGE)?;
    let value = value.to_str().ok()?;
    let total = value.rsplit('/').next()?;
    if total == "*" {
        return None;
    }
    total.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{filename_from_headers, total_from_content_range};
    use reqwest::header::{CONTENT_DISPOSITION, CONTENT_RANGE, HeaderMap, HeaderValue};

    #[test]
    fn test_filename_from_headers_quoted() {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_static("attachment; filename=\"file.bin\"");
        headers.insert(CONTENT_DISPOSITION, value);

        assert_eq!(
            filename_from_headers(&headers),
            Some("file.bin".to_string())
        );
    }

    #[test]
    fn test_filename_from_headers_unquoted() {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_static("inline; filename=report.pdf");
        headers.insert(CONTENT_DISPOSITION, value);

        assert_eq!(
            filename_from_headers(&headers),
            Some("report.pdf".to_string())
        );
    }

    #[test]
    fn test_filename_from_headers_missing() {
        let headers = HeaderMap::new();
        assert_eq!(filename_from_headers(&headers), None);
    }

    #[test]
    fn test_filename_from_headers_empty_filename() {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_static("attachment; filename=");
        headers.insert(CONTENT_DISPOSITION, value);

        assert_eq!(filename_from_headers(&headers), None);
    }

    #[test]
    fn test_total_from_content_range_parses_total() {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_static("bytes 0-0/1234");
        headers.insert(CONTENT_RANGE, value);

        assert_eq!(total_from_content_range(&headers), Some(1234));
    }

    #[test]
    fn test_total_from_content_range_missing_total() {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_static("bytes */1234");
        headers.insert(CONTENT_RANGE, value);

        assert_eq!(total_from_content_range(&headers), Some(1234));
    }

    #[test]
    fn test_total_from_content_range_unknown_total() {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_static("bytes 0-0/*");
        headers.insert(CONTENT_RANGE, value);

        assert_eq!(total_from_content_range(&headers), None);
    }
}
