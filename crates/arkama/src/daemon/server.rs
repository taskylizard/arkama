//! Bounded HTTP/JSON transport; no transfer or SQLite ownership lives here.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arkama_manager::{Command, Manager, Response};
use eyre::{Result, eyre};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::protocol::DaemonRequest;
use super::{
    AddrFileGuard, ServerOptions, adapt_request, auth, daemon_bind_addr, write_daemon_addr,
};

struct State {
    manager: Manager,
    no_auth: bool,
    hosts: Vec<String>,
    origins: Vec<String>,
    instance: String,
}

pub(super) async fn serve(manager: Manager, options: ServerOptions) -> Result<()> {
    let bind = daemon_bind_addr()?;
    if !bind.ip().is_loopback() && !(options.no_auth && options.allow_remote_no_auth) {
        return Err(eyre!(
            "authenticated remote access requires a TLS proxy to the loopback listener; wider no-auth needs --allow-remote-no-auth"
        ));
    }
    if options.no_auth {
        eprintln!(
            "WARNING: credentials disabled; reachable callers can control downloads. Other boundaries remain enabled."
        );
    }
    if !options.no_auth {
        auth::initialize()?;
    }
    let listener = TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let mut hosts = vec![address.to_string()];
    let mut origins = Vec::new();
    if options.origins.len() > 16 {
        return Err(eyre!("too many browser origins"));
    }
    for origin in options.origins {
        let url = url::Url::parse(&origin)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(eyre!("browser origins must be exact HTTPS origins"));
        }
        origins.push(url.origin().ascii_serialization());
        hosts.push(url[url::Position::BeforeHost..url::Position::AfterPort].to_owned());
    }
    let state = Arc::new(State {
        manager: manager.clone(),
        no_auth: options.no_auth,
        hosts,
        origins,
        instance: auth::random_id()?,
    });
    let address_path = arkama_data::daemon_addr_path()?;
    write_daemon_addr(&address_path, address)?;
    let _guard = AddrFileGuard { path: address_path };
    let slots = Arc::new(Semaphore::new(32));
    let mut connections = JoinSet::new();
    let mut window = Instant::now();
    let mut accepted_count = 0usize;
    tracing::info!("HTTP API v1 listening on {address}");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                if window.elapsed() >= Duration::from_secs(1) { window = Instant::now(); accepted_count = 0; }
                if accepted_count >= 64 { continue; }
                accepted_count += 1;
                let Ok(slot) = slots.clone().try_acquire_owned() else { continue; };
                let state = state.clone();
                connections.spawn(async move {
                    let _slot = slot;
                    let service = service_fn(move |request| handle(request, state.clone()));
                    let _ = tokio::time::timeout(Duration::from_secs(30), http1::Builder::new()
                        .timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5))
                        .max_buf_size(32 * 1024).keep_alive(false)
                        .serve_connection(TokioIo::new(stream), service)).await;
                });
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
            _ = manager.closed() => break,
            signal = tokio::signal::ctrl_c() => { signal?; manager.shutdown().await?; break; }
        }
    }
    drop(listener);
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    Ok(())
}

type HttpResponse = hyper::Response<Full<Bytes>>;

fn response(status: StatusCode, value: Value) -> HttpResponse {
    let mut bytes = serde_json::to_vec(&value).expect("JSON value");
    let mut status = status;
    if bytes.len() > 2 * 1024 * 1024 {
        status = StatusCode::INTERNAL_SERVER_ERROR;
        bytes = br#"{"error":{"code":"limit","message":"reply exceeds limit; use smaller pages"}}"#
            .to_vec();
    }
    hyper::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-store")
        .header("X-Content-Type-Options", "nosniff")
        .body(Full::new(Bytes::from(bytes)))
        .expect("HTTP response")
}

fn error(status: StatusCode, code: &str, message: &str) -> HttpResponse {
    response(status, json!({"error":{"code":code,"message":message}}))
}

async fn handle(
    mut request: Request<Incoming>,
    state: Arc<State>,
) -> Result<HttpResponse, Infallible> {
    let host = request
        .headers()
        .get("Host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let origin = request
        .headers()
        .get("Origin")
        .and_then(|value| value.to_str().ok());
    if !state.hosts.iter().any(|allowed| allowed == host)
        || (request.headers().contains_key("Origin")
            && !origin.is_some_and(|origin| state.origins.iter().any(|allowed| allowed == origin)))
    {
        return Ok(error(
            StatusCode::FORBIDDEN,
            "origin",
            "host or origin is not authorized",
        ));
    }
    // Narrow preflight grants no data or command access. Every actual route authenticates below.
    if request.method() == Method::OPTIONS && origin.is_some() {
        let mut reply = response(StatusCode::NO_CONTENT, Value::Null);
        cors(&mut reply, origin);
        reply.headers_mut().insert(
            "Access-Control-Allow-Methods",
            "GET, POST, OPTIONS".parse().unwrap(),
        );
        reply.headers_mut().insert(
            "Access-Control-Allow-Headers",
            "Authorization, Content-Type".parse().unwrap(),
        );
        return Ok(reply);
    }
    let origin = origin.map(str::to_owned);
    if !state.no_auth {
        let header = request
            .headers()
            .get("Authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let valid = tokio::task::spawn_blocking(move || auth::authorized(&header))
            .await
            .unwrap_or(false);
        if !valid {
            return Ok(error(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "a current API key is required",
            ));
        }
    }
    let path = request.uri().path().to_owned();
    let query = request.uri().query().unwrap_or_default().to_owned();
    let result = if request.method() == Method::GET && path == "/v1/info" {
        response(
            StatusCode::OK,
            json!({"api_version":1,"instance":state.instance,"authentication":if state.no_auth {"none"} else {"api_key"},"capabilities":["jobs","controls","settings","history","diagnostics","updates"],"limits":{"body_bytes":65536,"reply_bytes":2097152,"connections":32,"connections_per_second":64,"page_size":100}}),
        )
    } else if request.method() == Method::GET && path == "/v1/updates" {
        updates(&state, &query).await
    } else if request.method() == Method::GET && path == "/v1/diagnostics" {
        let fields: Vec<_> = url::form_urlencoded::parse(query.as_bytes()).collect();
        match fields.as_slice() {
            [(key, value)] if key == "id" => match value.parse::<i64>() {
                Ok(id) => match state.manager.execute(Command::Diagnostics { id }).await {
                    Ok(reply) => response(StatusCode::OK, serde_json::to_value(reply).unwrap()),
                    Err(e) => manager_error(e),
                },
                Err(_) => error(StatusCode::BAD_REQUEST, "invalid", "invalid job ID"),
            },
            _ => error(
                StatusCode::BAD_REQUEST,
                "invalid",
                "diagnostics needs one job ID",
            ),
        }
    } else if request.method() == Method::POST
        && matches!(
            path.as_str(),
            "/v1/commands" | "/v1/legacy" | "/v1/shutdown"
        )
    {
        if request
            .headers()
            .get("Content-Type")
            .and_then(|value| value.to_str().ok())
            != Some("application/json")
        {
            error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "content_type",
                "expected application/json",
            )
        } else {
            match tokio::time::timeout(Duration::from_secs(5), read_body(request.body_mut())).await
            {
                Ok(Ok(body)) => {
                    if path == "/v1/shutdown" {
                        if body != b"{}" {
                            error(
                                StatusCode::BAD_REQUEST,
                                "invalid",
                                "shutdown expects an empty object",
                            )
                        } else {
                            match state.manager.shutdown().await {
                                Ok(()) => response(StatusCode::OK, json!({"type":"ok"})),
                                Err(e) => manager_error(e),
                            }
                        }
                    } else if path == "/v1/legacy" {
                        match serde_json::from_slice::<DaemonRequest>(&body) {
                            Ok(command) => response(
                                StatusCode::OK,
                                serde_json::to_value(adapt_request(&state.manager, command).await)
                                    .unwrap(),
                            ),
                            Err(_) => error(
                                StatusCode::BAD_REQUEST,
                                "invalid",
                                "invalid v1 legacy command",
                            ),
                        }
                    } else {
                        match serde_json::from_slice::<Command>(&body) {
                            Ok(command) => match state.manager.execute(command).await {
                                Ok(reply) => {
                                    response(StatusCode::OK, serde_json::to_value(reply).unwrap())
                                }
                                Err(e) => manager_error(e),
                            },
                            Err(_) => {
                                error(StatusCode::BAD_REQUEST, "invalid", "invalid v1 command")
                            }
                        }
                    }
                }
                Ok(Err(())) => error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "body_limit",
                    "invalid or oversized request body",
                ),
                Err(_) => error(
                    StatusCode::REQUEST_TIMEOUT,
                    "deadline",
                    "request body deadline exceeded",
                ),
            }
        }
    } else {
        error(StatusCode::NOT_FOUND, "route", "unknown v1 route or method")
    };
    let mut result = result;
    cors(&mut result, origin.as_deref());
    Ok(result)
}

fn cors(response: &mut HttpResponse, origin: Option<&str>) {
    if let Some(origin) = origin {
        response.headers_mut().insert(
            "Access-Control-Allow-Origin",
            origin.parse().expect("validated origin"),
        );
        response
            .headers_mut()
            .insert("Vary", "Origin".parse().unwrap());
    }
}

async fn read_body(body: &mut Incoming) -> Result<Vec<u8>, ()> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| ())?;
        if let Some(data) = frame.data_ref() {
            if bytes.len() + data.len() > 65_536 {
                return Err(());
            }
            bytes.extend_from_slice(data);
        }
    }
    Ok(bytes)
}

fn manager_error(error: arkama_manager::Error) -> HttpResponse {
    let status = match error.code.as_str() {
        "invalid" => StatusCode::BAD_REQUEST,
        "forbidden" => StatusCode::FORBIDDEN,
        "not_found" => StatusCode::NOT_FOUND,
        "conflict" => StatusCode::CONFLICT,
        "busy" | "stopped" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    response(status, json!({"error":error}))
}

async fn updates(state: &State, query: &str) -> HttpResponse {
    let fields: Vec<_> = url::form_urlencoded::parse(query.as_bytes()).collect();
    if fields.len() != 2
        || !fields.iter().any(|(key, _)| key == "instance")
        || !fields.iter().any(|(key, _)| key == "revision")
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid",
            "updates needs instance and revision",
        );
    }
    let instance = fields
        .iter()
        .find(|(key, _)| key == "instance")
        .unwrap()
        .1
        .as_ref();
    let Ok(revision) = fields
        .iter()
        .find(|(key, _)| key == "revision")
        .unwrap()
        .1
        .parse::<u64>()
    else {
        return error(StatusCode::BAD_REQUEST, "invalid", "invalid revision");
    };
    let started = Instant::now();
    loop {
        match state.manager.execute(Command::Status).await {
            Ok(Response::Status(status)) => {
                if instance != state.instance
                    || status.revision != revision
                    || started.elapsed() >= Duration::from_secs(20)
                {
                    return response(
                        StatusCode::OK,
                        json!({"instance":state.instance,"revision":status.revision,"changed":instance != state.instance || status.revision != revision,"resync":instance != state.instance || status.revision != revision,"status":status}),
                    );
                }
            }
            Err(e) => return manager_error(e),
            _ => unreachable!("status response"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
