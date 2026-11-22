use crate::RecoveryPolicy;
use crate::recovery::{RecoveryReason, RetryBudget};
use eyre::{Context, Result};
use futures_util::{Stream, StreamExt};
use reqwest::header::{
    ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue, IF_RANGE, LAST_MODIFIED, RANGE,
    RETRY_AFTER,
};
use reqwest::redirect::Policy;
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
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
    pub(crate) recovery: RecoveryPolicy,
    budget: Arc<RetryBudget>,
    pub(crate) origin: String,
}

impl ClientFactory {
    pub(crate) fn new(
        user_agent: Option<&str>,
        experimental_entropy: bool,
        recovery: RecoveryPolicy,
        budget: Arc<RetryBudget>,
        url: &Url,
    ) -> Result<Self> {
        let base = build_client(user_agent, experimental_entropy, &recovery)?;
        Ok(Self {
            base,
            recovery,
            budget,
            origin: url.origin().ascii_serialization(),
        })
    }

    async fn send(&self, request: RequestBuilder) -> Result<Response> {
        tokio::time::timeout(
            Duration::from_millis(self.recovery.setup_timeout_ms),
            request.send(),
        )
        .await
        .map_err(|_| HttpFailure::SetupTimeout)?
        .map_err(|error| error.without_url().into())
    }

    pub(crate) fn retry_delay(
        &self,
        attempt: u32,
        error: eyre::Report,
    ) -> Result<(RecoveryReason, Duration)> {
        let (reason, server_wait, origin) =
            if let Some(failure) = error.downcast_ref::<HttpFailure>() {
                let Some(reason) = failure.reason() else {
                    return Err(error);
                };
                match failure {
                    HttpFailure::Status {
                        retry_after,
                        origin,
                        ..
                    } => (reason, *retry_after, origin.as_str()),
                    _ => (reason, None, self.origin.as_str()),
                }
            } else if let Some(failure) = error.downcast_ref::<reqwest::Error>() {
                // Reqwest classifies premature HTTP body EOF as Decode, even with decoding off.
                let reason = if failure.is_connect() || failure.is_timeout() {
                    RecoveryReason::Connection
                } else if failure.is_body() || failure.is_decode() {
                    RecoveryReason::Body
                } else {
                    return Err(error);
                };
                (reason, None, self.origin.as_str())
            } else {
                return Err(error);
            };
        if attempt > self.recovery.max_retries {
            return Err(error.wrap_err("retry limit exhausted"));
        }
        let delay = self
            .recovery
            .delay(attempt)
            .max(server_wait.unwrap_or_default());
        if delay > Duration::from_millis(self.recovery.max_delay_ms) {
            return Err(error.wrap_err("Retry-After exceeds maximum wait; retry later"));
        }
        self.budget
            .reserve(origin, &self.recovery)
            .map_err(|limit| error.wrap_err(limit))?;
        Ok((reason, delay))
    }
}

#[derive(Debug)]
pub(crate) enum HttpFailure {
    SetupTimeout,
    ReadIdle,
    ShortBody,
    Status {
        status: StatusCode,
        retry_after: Option<Duration>,
        origin: String,
    },
}

impl HttpFailure {
    fn reason(&self) -> Option<RecoveryReason> {
        match self {
            Self::SetupTimeout => Some(RecoveryReason::SetupTimeout),
            Self::ReadIdle => Some(RecoveryReason::ReadIdle),
            Self::ShortBody => Some(RecoveryReason::ShortBody),
            Self::Status { status, .. } => match status.as_u16() {
                429 => Some(RecoveryReason::Throttled),
                408 | 500 | 502 | 503 | 504 => Some(RecoveryReason::TemporaryServer),
                _ => None,
            },
        }
    }
}

impl fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SetupTimeout => f.write_str("connection/header deadline exceeded"),
            Self::ReadIdle => f.write_str("network read idle deadline exceeded"),
            Self::ShortBody => f.write_str("HTTP body ended early"),
            Self::Status { status, .. } => {
                write!(f, "HTTP {status}")?;
                if matches!(status.as_u16(), 401 | 403 | 404) {
                    f.write_str("; check credentials or link")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for HttpFailure {}

fn check_status(response: Response) -> Result<Response> {
    let status = response.status();
    if status.is_client_error() || status.is_server_error() {
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|header| header.to_str().ok())
            .and_then(|value| {
                value
                    .parse::<u64>()
                    .ok()
                    .map(Duration::from_secs)
                    .or_else(|| {
                        httpdate::parse_http_date(value)
                            .ok()
                            .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
                    })
            });
        return Err(HttpFailure::Status {
            status,
            retry_after,
            origin: response.url().origin().ascii_serialization(),
        }
        .into());
    }
    Ok(response)
}

pub(crate) async fn next_chunk<S, T>(stream: &mut S, idle_ms: u64) -> Option<Result<T>>
where
    S: Stream<Item = std::result::Result<T, reqwest::Error>> + Unpin,
{
    match tokio::time::timeout(Duration::from_millis(idle_ms), stream.next()).await {
        Ok(next) => next.map(|result| result.map_err(|error| error.without_url().into())),
        Err(_) => Some(Err(HttpFailure::ReadIdle.into())),
    }
}

fn build_client(
    user_agent: Option<&str>,
    experimental_entropy: bool,
    recovery: &RecoveryPolicy,
) -> Result<Client> {
    let user_agent = user_agent.unwrap_or("arkama/0.1");
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    let mut builder = Client::builder()
        .user_agent(user_agent)
        .connect_timeout(Duration::from_millis(recovery.connect_timeout_ms))
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

pub(crate) async fn probe(factory: &ClientFactory, url: &Url) -> Result<HttpMeta> {
    let mut size = None;
    let mut accept_ranges = false;
    let mut filename = None;
    let mut etag = None;
    let mut last_modified = None;
    let mut mime_type = None;
    let mut final_url = url.to_string();

    let head = factory.send(factory.base.head(url.clone())).await?;
    // HEAD may be unsupported even when GET works; temporary failures must not be bypassed.
    let head = if matches!(head.status().as_u16(), 408 | 429 | 500 | 502 | 503 | 504) {
        check_status(head)?
    } else {
        head
    };
    let head = head.status().is_success().then_some(head);

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
        let probe = check_status(
            factory
                .send(factory.base.get(url.clone()).header(RANGE, "bytes=0-0"))
                .await?,
        )?;
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
    factory: &ClientFactory,
    url: &Url,
    start: u64,
    end: Option<u64>,
    if_range: Option<&str>,
) -> Result<Response> {
    let range = match end {
        Some(end) => format!("bytes={start}-{end}"),
        None => format!("bytes={start}-"),
    };
    let mut request = factory.base.get(url.clone()).header(RANGE, range);
    if let Some(if_range) = if_range {
        request = request.header(IF_RANGE, HeaderValue::from_str(if_range)?);
    }
    check_status(factory.send(request).await?)
}

pub(crate) async fn get_full(factory: &ClientFactory, url: &Url) -> Result<Response> {
    check_status(factory.send(factory.base.get(url.clone())).await?)
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
