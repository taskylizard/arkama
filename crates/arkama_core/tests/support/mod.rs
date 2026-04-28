use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub(crate) struct FlakyServerConfig {
    pub(crate) data: Vec<u8>,
    pub(crate) accept_ranges: bool,
    pub(crate) chunk_size: usize,
    pub(crate) chunk_delay: Duration,
    pub(crate) interrupt_full_gets: usize,
    pub(crate) interrupt_range_gets: usize,
    pub(crate) interrupt_after_bytes: usize,
}

impl FlakyServerConfig {
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            accept_ranges: true,
            chunk_size: 64 * 1024,
            chunk_delay: Duration::ZERO,
            interrupt_full_gets: 0,
            interrupt_range_gets: 0,
            interrupt_after_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpMethod {
    Head,
    Get,
}

#[derive(Debug, Clone, Copy)]
struct ByteRange {
    start: u64,
    end: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct HttpRequest {
    method: HttpMethod,
    range: Option<ByteRange>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestStats {
    pub(crate) head_requests: usize,
    pub(crate) full_get_requests: usize,
    pub(crate) range_get_requests: usize,
}

struct SharedState {
    config: FlakyServerConfig,
    head_requests: AtomicUsize,
    full_get_requests: AtomicUsize,
    range_get_requests: AtomicUsize,
}

pub(crate) struct FlakyServer {
    addr: SocketAddr,
    state: Arc<SharedState>,
    handle: JoinHandle<()>,
}

impl FlakyServer {
    pub(crate) async fn spawn(config: FlakyServerConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let state = Arc::new(SharedState {
            config,
            head_requests: AtomicUsize::new(0),
            full_get_requests: AtomicUsize::new(0),
            range_get_requests: AtomicUsize::new(0),
        });
        let task_state = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let task_state = Arc::clone(&task_state);
                tokio::spawn(async move {
                    let _ = handle_connection(stream, task_state).await;
                });
            }
        });

        Self {
            addr,
            state,
            handle,
        }
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}/file.bin", self.addr)
    }

    pub(crate) fn stats(&self) -> RequestStats {
        RequestStats {
            head_requests: self.state.head_requests.load(Ordering::Relaxed),
            full_get_requests: self.state.full_get_requests.load(Ordering::Relaxed),
            range_get_requests: self.state.range_get_requests.load(Ordering::Relaxed),
        }
    }
}

impl Drop for FlakyServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn handle_connection(mut stream: TcpStream, state: Arc<SharedState>) -> io::Result<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };

    let mut full_get_attempt = None;
    let mut range_get_attempt = None;

    match request.method {
        HttpMethod::Head => {
            state.head_requests.fetch_add(1, Ordering::Relaxed);
        }
        HttpMethod::Get => match request.range {
            Some(_) => {
                let attempt = state.range_get_requests.fetch_add(1, Ordering::Relaxed) + 1;
                range_get_attempt = Some(attempt);
            }
            None => {
                let attempt = state.full_get_requests.fetch_add(1, Ordering::Relaxed) + 1;
                full_get_attempt = Some(attempt);
            }
        },
    }

    let data = &state.config.data;
    let body_range = select_body_range(data.len(), request.range, state.config.accept_ranges);
    let response = response_headers(body_range, state.config.accept_ranges);
    stream.write_all(response.as_bytes()).await?;

    if request.method == HttpMethod::Head || body_range.length == 0 {
        return Ok(());
    }

    let should_interrupt = match request.range {
        Some(_) => range_get_attempt.expect("range attempt") <= state.config.interrupt_range_gets,
        None => full_get_attempt.expect("full attempt") <= state.config.interrupt_full_gets,
    };

    let body = &data[body_range.start..body_range.end];
    let send_limit = if should_interrupt {
        state.config.interrupt_after_bytes.min(body.len())
    } else {
        body.len()
    };

    let mut sent = 0usize;
    while sent < send_limit {
        let chunk_end = (sent + state.config.chunk_size).min(send_limit);
        stream.write_all(&body[sent..chunk_end]).await?;
        sent = chunk_end;
        if state.config.chunk_delay > Duration::ZERO && sent < send_limit {
            tokio::time::sleep(state.config.chunk_delay).await;
        }
    }
    stream.flush().await?;

    if should_interrupt && send_limit < body.len() {
        return Ok(());
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct BodyRange {
    status: u16,
    start: usize,
    end: usize,
    length: usize,
    content_range: Option<(usize, usize, usize)>,
}

fn select_body_range(total: usize, range: Option<ByteRange>, accept_ranges: bool) -> BodyRange {
    if !accept_ranges {
        return BodyRange {
            status: 200,
            start: 0,
            end: total,
            length: total,
            content_range: None,
        };
    }

    let Some(ByteRange { start, end }) = range else {
        return BodyRange {
            status: 200,
            start: 0,
            end: total,
            length: total,
            content_range: None,
        };
    };
    let start = usize::try_from(start).unwrap_or(total);
    if start >= total {
        return BodyRange {
            status: 416,
            start: 0,
            end: 0,
            length: 0,
            content_range: Some((0, 0, total)),
        };
    }

    let end = end
        .and_then(|end| usize::try_from(end).ok())
        .map(|end| end.saturating_add(1))
        .unwrap_or(total)
        .min(total);

    BodyRange {
        status: 206,
        start,
        end,
        length: end.saturating_sub(start),
        content_range: Some((start, end.saturating_sub(1), total)),
    }
}

fn response_headers(body_range: BodyRange, accept_ranges: bool) -> String {
    let status_line = match body_range.status {
        200 => "HTTP/1.1 200 OK",
        206 => "HTTP/1.1 206 Partial Content",
        416 => "HTTP/1.1 416 Range Not Satisfiable",
        _ => unreachable!(),
    };
    let mut response = format!(
        "{status_line}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body_range.length
    );
    if accept_ranges {
        response.push_str("Accept-Ranges: bytes\r\n");
    }
    if let Some((start, end, total)) = body_range.content_range {
        if body_range.status == 416 {
            response.push_str(&format!("Content-Range: bytes */{total}\r\n"));
        } else {
            response.push_str(&format!("Content-Range: bytes {start}-{end}/{total}\r\n"));
        }
    }
    response.push_str("\r\n");
    response
}

async fn read_request(stream: &mut TcpStream) -> io::Result<Option<HttpRequest>> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = find_headers_end(&buffer) {
            buffer.truncate(end);
            break;
        }
        if buffer.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request too large",
            ));
        }
    }

    parse_request(&buffer).map(Some)
}

fn parse_request(buffer: &[u8]) -> io::Result<HttpRequest> {
    let request = String::from_utf8_lossy(buffer);
    let mut lines = request.split("\r\n");
    let Some(line) = lines.next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing request line",
        ));
    };
    let mut parts = line.split_whitespace();
    let Some(method) = parts.next() else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "missing method"));
    };
    let method = match method {
        "HEAD" => HttpMethod::Head,
        "GET" => HttpMethod::Get,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported method",
            ));
        }
    };

    let mut range = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("range") {
            range = parse_range(value.trim());
        }
    }

    Ok(HttpRequest { method, range })
}

fn parse_range(value: &str) -> Option<ByteRange> {
    let value = value.strip_prefix("bytes=")?;
    let (start, end) = value.split_once('-')?;
    let start = start.parse().ok()?;
    let end = if end.is_empty() {
        None
    } else {
        end.parse().ok()
    };
    Some(ByteRange { start, end })
}

fn find_headers_end(buffer: &[u8]) -> Option<usize> {
    for (index, window) in buffer.windows(4).enumerate() {
        if window == b"\r\n\r\n" {
            return Some(index);
        }
    }
    None
}
