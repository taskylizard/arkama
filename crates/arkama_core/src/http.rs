use eyre::{Context, Result};
use reqwest::header::{
    ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue, IF_RANGE, LAST_MODIFIED, RANGE,
};
use reqwest::redirect::Policy;
use reqwest::{Client, Response, StatusCode};
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub(crate) struct HttpMeta {
    pub(crate) size: Option<u64>,
    pub(crate) accept_ranges: bool,
    pub(crate) filename: Option<String>,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<String>,
    pub(crate) mime_type: Option<String>,
    pub(crate) final_url: String,
}

impl HttpMeta {
    pub(crate) fn strong_etag(&self) -> Option<&str> {
        self.etag.as_deref().filter(|tag| {
            tag.len() >= 2
                && tag.starts_with('"')
                && tag.ends_with('"')
                && tag[1..tag.len() - 1]
                    .bytes()
                    .all(|b| b == 0x21 || (0x23..=0x7e).contains(&b) || b >= 0x80)
        })
    }
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
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    let mut builder = Client::builder()
        .user_agent(user_agent)
        .default_headers(headers)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
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
    let mut etag = None;
    let mut last_modified = None;
    let mut mime_type = None;
    let mut final_url = url.to_string();

    let head = client.head(url.clone()).send().await;
    let head = head.ok().filter(|response| response.status().is_success());

    if let Some(resp) = head {
        validate_identity_encoding(&resp)?;
        final_url = resp.url().to_string();
        let headers = resp.headers();
        size = header_string(headers.get(CONTENT_LENGTH)).and_then(|len| len.parse().ok());
        accept_ranges = header_string(headers.get(ACCEPT_RANGES))
            .is_some_and(|ranges| ranges.eq_ignore_ascii_case("bytes"));
        if let Some(name) = filename_from_headers(headers) {
            filename = Some(name);
        }
        etag = header_string(headers.get(ETAG));
        last_modified = header_string(headers.get(LAST_MODIFIED));
        mime_type = header_string(headers.get(CONTENT_TYPE));
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
                    etag,
                    last_modified,
                    mime_type,
                    final_url,
                });
            }
        };
        validate_identity_encoding(&probe)?;
        final_url = probe.url().to_string();
        if probe.status() == StatusCode::PARTIAL_CONTENT {
            accept_ranges = true;
        }
        if probe.status() == StatusCode::PARTIAL_CONTENT {
            size = total_from_content_range(probe.headers());
        } else if probe.status() == StatusCode::OK {
            size = probe.content_length().or(size);
        }
        if filename.is_none() {
            filename = filename_from_headers(probe.headers());
        }
        etag = header_string(probe.headers().get(ETAG));
        last_modified = header_string(probe.headers().get(LAST_MODIFIED));
        if mime_type.is_none() {
            mime_type = header_string(probe.headers().get(CONTENT_TYPE));
        }
    }

    Ok(HttpMeta {
        size,
        accept_ranges,
        filename,
        etag,
        last_modified,
        mime_type,
        final_url,
    })
}

pub(crate) async fn get_range(
    client: &Client,
    url: &Url,
    start: u64,
    end: Option<u64>,
    if_range: Option<&str>,
) -> Result<Response> {
    let range = match end {
        Some(end) => format!("bytes={start}-{end}"),
        None => format!("bytes={start}-"),
    };
    let mut request = client.get(url.clone()).header(RANGE, range);
    if let Some(if_range) = if_range {
        request = request.header(IF_RANGE, HeaderValue::from_str(if_range)?);
    }
    let resp = request
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

pub(crate) fn validate_identity_encoding(response: &Response) -> Result<()> {
    if let Some(value) = response.headers().get(CONTENT_ENCODING) {
        if !value.to_str()?.trim().eq_ignore_ascii_case("identity") {
            return Err(eyre::eyre!(
                "encoded responses are not supported; requested identity encoding"
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_range(
    response: &Response,
    start: u64,
    end: u64,
    meta: &HttpMeta,
) -> Result<()> {
    validate_identity_encoding(response)?;
    let value = response
        .headers()
        .get(CONTENT_RANGE)
        .ok_or_else(|| eyre::eyre!("missing Content-Range"))?
        .to_str()?;
    let (range, total) = value
        .strip_prefix("bytes ")
        .and_then(|value| value.split_once('/'))
        .ok_or_else(|| eyre::eyre!("invalid Content-Range"))?;
    let (returned_start, returned_end) = range
        .split_once('-')
        .ok_or_else(|| eyre::eyre!("invalid Content-Range bounds"))?;
    if returned_start.parse::<u64>()? != start
        || returned_end.parse::<u64>()? != end
        || Some(total.parse::<u64>()?) != meta.size
        || response
            .content_length()
            .is_some_and(|len| len != end - start + 1)
    {
        return Err(eyre::eyre!(
            "response range does not match requested bounds and total"
        ));
    }
    if response.url().as_str() != meta.final_url
        || header_string(response.headers().get(ETAG)).as_deref() != meta.strong_etag()
    {
        return Err(eyre::eyre!(
            "resource identity changed during ranged download"
        ));
    }
    Ok(())
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

fn header_string(value: Option<&HeaderValue>) -> Option<String> {
    let value = value?.to_str().ok()?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
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
