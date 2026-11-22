//! Real-process HTTP, credential, destination and overload contracts.

use std::path::Path;
use std::time::Duration;

use arkama_core::{DestinationPolicy, DownloadRequest, DownloadSession};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "../../arkama_core/tests/support/mod.rs"]
mod support;
use support::{FlakyServer, FlakyServerConfig};

struct Server {
    child: tokio::process::Child,
    addr: String,
    key: Option<String>,
    client: Client,
}

impl Server {
    async fn start(dir: &Path, options: &[String]) -> Self {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_arkama"))
            .args(["daemon", "run", "--download-root"])
            .arg(dir.join("out"))
            .args(options)
            .env("ARKAMA_DATA_DIR", dir.join("state"))
            .env_remove("ARKAMA_DAEMON_ADDR")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let addr = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "host exited before ready"
                );
                match std::fs::read_to_string(dir.join("state/daemon.addr")) {
                    Ok(addr) if TcpStream::connect(&addr).await.is_ok() => break addr,
                    _ => {}
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        Self {
            child,
            addr,
            key: std::fs::read_to_string(dir.join("state/credentials/api.key")).ok(),
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(25))
                .build()
                .unwrap(),
        }
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.authenticate(self.client.get(format!("http://{}{path}", self.addr)))
    }

    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.key {
            Some(key) => request.bearer_auth(key),
            None => request,
        }
    }

    async fn command(&self, command: Value) -> (StatusCode, Value) {
        let reply = self
            .authenticate(
                self.client
                    .post(format!("http://{}/v1/commands", self.addr)),
            )
            .json(&command)
            .send()
            .await
            .unwrap();
        (reply.status(), reply.json().await.unwrap())
    }

    async fn stop(&mut self) {
        self.authenticate(
            self.client
                .post(format!("http://{}/v1/shutdown", self.addr)),
        )
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }

    async fn finished(&self, id: i64) -> Value {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let (status, reply) = self.command(json!({"op":"show","id":id})).await;
                assert_eq!(status, StatusCode::OK);
                if reply["record"]["status"] == "finished" {
                    break reply;
                }
                assert_ne!(reply["record"]["status"], "failed", "{reply}");
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        })
        .await
        .unwrap()
    }
}

async fn rotate(dir: &Path, action: &str) {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_arkama"))
        .args(["daemon", "key", action])
        .env("ARKAMA_DATA_DIR", dir.join("state"))
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keys_origins_limits_and_reconnect_follow_one_policy() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(
        dir.path(),
        &["--allow-origin".into(), "https://client.example".into()],
    )
    .await;
    for path in [
        "/v1/info",
        "/v1/updates?instance=old&revision=0",
        "/v1/diagnostics?id=1",
        "/v1/commands",
        "/v1/legacy",
        "/v1/shutdown",
    ] {
        let request =
            if path.contains("info") || path.contains("updates") || path.contains("diagnostics") {
                server.client.get(format!("http://{}{path}", server.addr))
            } else {
                server
                    .client
                    .post(format!("http://{}{path}", server.addr))
                    .json(&json!({"op":"status"}))
            };
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        server
            .client
            .get(format!("http://{}/v1/info", server.addr))
            .bearer_auth("wrong")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server
            .get("/v1/info")
            .header("Host", "evil.example")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        server
            .get("/v1/info")
            .header("Origin", "https://evil.example")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let allowed = server
        .get("/v1/info")
        .header("Origin", "https://client.example")
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    assert_eq!(
        allowed.headers()["Access-Control-Allow-Origin"],
        "https://client.example"
    );
    let info: Value = allowed.json().await.unwrap();
    assert_eq!(info["api_version"], 1);
    assert!(!info.to_string().contains(server.key.as_ref().unwrap()));
    let revision = server.command(json!({"op":"status"})).await.1["revision"]
        .as_u64()
        .unwrap();
    assert_eq!(
        server
            .command(json!({"op":"set_setting","key":"max_concurrent","value":"2"}))
            .await
            .0,
        StatusCode::OK
    );
    let update: Value = server
        .get(&format!(
            "/v1/updates?instance={}&revision={revision}",
            info["instance"].as_str().unwrap()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(update["resync"], true);
    assert!(update["revision"].as_u64().unwrap() > revision);
    let restarted: Value = server
        .get("/v1/updates?instance=old&revision=9999")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(restarted["resync"], true);
    let large = server
        .authenticate(
            server
                .client
                .post(format!("http://{}/v1/commands", server.addr)),
        )
        .header("Content-Type", "application/json")
        .body(vec![b' '; 65_537])
        .send()
        .await
        .unwrap();
    assert_eq!(large.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let mut slow = TcpStream::connect(&server.addr).await.unwrap();
    slow.write_all(format!("POST /v1/commands HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{{", server.addr, server.key.as_ref().unwrap()).as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(8), slow.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes).starts_with("HTTP/1.1 408"));
    rotate(dir.path(), "rotate").await;
    assert_eq!(
        server.get("/v1/info").send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    server.key = std::fs::read_to_string(dir.path().join("state/credentials/api.key")).ok();
    assert_eq!(
        server.get("/v1/info").send().await.unwrap().status(),
        StatusCode::OK
    );
    rotate(dir.path(), "revoke").await;
    assert_eq!(
        server.get("/v1/info").send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    server.child.kill().await.unwrap();
    server = Server::start(dir.path(), &[]).await;
    assert_eq!(
        server.get("/v1/info").send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    rotate(dir.path(), "rotate").await;
    server.key = std::fs::read_to_string(dir.path().join("state/credentials/api.key")).ok();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(dir.path().join("state/credentials/api.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let cli = tokio::process::Command::new(env!("CARGO_BIN_EXE_arkama"))
        .args(["manage", "{\"op\":\"status\"}"])
        .env("ARKAMA_DATA_DIR", dir.path().join("state"))
        .output()
        .await
        .unwrap();
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credential_free_mode_cannot_relax_destination_or_path_boundaries() {
    let data: Vec<u8> = (0..65_539)
        .map(|i| ((i * 37 + i / 251) % 256) as u8)
        .collect();
    let source = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;
    let target = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path(), &["--no-auth".into()]).await;
    let path = dir.path().join("out/file.bin");
    let request = DownloadRequest::new(source.url()).output(&path);
    assert_eq!(
        server
            .command(json!({"op":"enqueue","request":request}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    // Hostnames are checked at actual resolution, including localhost and IPv4-mapped IPv6.
    let policy = DestinationPolicy::public_only(&[]).unwrap();
    let session = DownloadSession::default().destination_policy(policy.clone());
    assert!(
        session
            .start(DownloadRequest::new("http://[::ffff:127.0.0.1]/file"))
            .is_err()
    );
    for address in [
        "http://10.1.2.3/",
        "http://169.254.169.254/",
        "http://100.64.0.1/",
        "http://[fc00::1]/",
        "http://[2001:db8::1]/",
    ] {
        assert!(policy.validate(&url::Url::parse(address).unwrap()).is_err());
    }
    assert!(
        policy
            .validate(&url::Url::parse("https://8.8.8.8/").unwrap())
            .is_ok()
    );
    let dns = source.url().replace("127.0.0.1", "localhost");
    let (_, accepted) = server.command(json!({"op":"enqueue","request":DownloadRequest::new(dns).output(dir.path().join("out/dns.bin"))})).await;
    let id = accepted["id"].as_i64().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let reply = server.command(json!({"op":"show","id":id})).await.1;
            if reply["record"]["status"] == "failed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(source.stats().head_requests, 0);
    assert_eq!(source.stats().full_get_requests, 0);
    assert_eq!(source.stats().range_get_requests, 0);
    server.stop().await;
    let options = vec![
        "--no-auth".into(),
        "--allow-private-source".into(),
        url::Url::parse(&source.url())
            .unwrap()
            .origin()
            .ascii_serialization(),
    ];
    // An owner-selected root can have an aliased parent (like macOS /var). Client paths
    // using that spelling must work, without granting permission to links below the root.
    #[cfg(unix)]
    let (options, request) = {
        let mut options = options;
        let alias = dir.path().join("root-alias");
        std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
        options.extend([
            "--download-root".into(),
            alias.join("out").to_str().unwrap().into(),
        ]);
        (options, request.output(alias.join("out/file.bin")))
    };
    server = Server::start(dir.path(), &options).await;
    for bad in [
        dir.path().join("outside.bin"),
        dir.path().join("out/../escape.bin"),
        dir.path().join("out/CON.bin"),
        dir.path().join("out/name.ARKAMA.PART"),
        dir.path().join("out/name:stream"),
        dir.path().join("out/name."),
    ] {
        assert!(
            server
                .command(json!({"op":"enqueue","request":request.clone().output(bad)}))
                .await
                .0
                .is_client_error()
        );
    }
    assert_eq!(
        server
            .command(
                json!({"op":"set_setting","key":"download_dir","value":dir.path().join("state")})
            )
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(dir.path().join("state"), dir.path().join("out/link")).unwrap();
        for bad in [
            "out/link/escaped.bin",
            "root-alias/out/link/escaped.bin",
            "root-alias/out/../escape.bin",
        ] {
            assert_eq!(server.command(json!({"op":"enqueue","request":request.clone().output(dir.path().join(bad))})).await.0, StatusCode::FORBIDDEN);
        }
    }
    let mut unsupported = serde_json::to_value(&request).unwrap();
    unsupported["method"] = json!("POST");
    assert_eq!(
        server
            .command(json!({"op":"enqueue","request":unsupported}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (status, accepted) = server
        .command(json!({"op":"enqueue","request":request}))
        .await;
    assert_eq!(status, StatusCode::OK);
    server.finished(accepted["id"].as_i64().unwrap()).await;
    assert_eq!(std::fs::read(&path).unwrap(), data);
    let mut redirect = FlakyServerConfig::new(data);
    redirect.redirect = Some(target.url());
    source.configure(redirect);
    let (_, accepted) = server.command(json!({"op":"enqueue","request":DownloadRequest::new(source.url()).output(dir.path().join("out/redirect.bin"))})).await;
    let id = accepted["id"].as_i64().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let reply = server.command(json!({"op":"show","id":id})).await.1;
            if reply["record"]["status"] == "failed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(target.stats().head_requests, 0);
    assert_eq!(target.stats().full_get_requests, 0);
    assert_eq!(target.stats().range_get_requests, 0);
    assert!(!dir.path().join("out/redirect.bin").exists());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_client_admission_cannot_back_up_downloads() {
    let data: Vec<u8> = (0..262_151)
        .map(|i| ((i * 31 + i / 257) % 256) as u8)
        .collect();
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 4096;
    config.chunk_delay = Duration::from_millis(20);
    let source = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(
        dir.path(),
        &[
            "--allow-private-source".into(),
            url::Url::parse(&source.url())
                .unwrap()
                .origin()
                .ascii_serialization(),
        ],
    )
    .await;
    let path = dir.path().join("out/overload.bin");
    let (_, accepted) = server.command(json!({"op":"enqueue","request":DownloadRequest::new(source.url()).output(&path).connections(1)})).await;
    let id = accepted["id"].as_i64().unwrap();
    let diagnostics = server
        .get(&format!("/v1/diagnostics?id={id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(diagnostics.status(), StatusCode::OK);
    let mut clients = Vec::new();
    for _ in 0..80 {
        if let Ok(mut stream) = TcpStream::connect(&server.addr).await {
            let _ = stream.write_all(b"GET /v1/info HTTP/1.1\r\n").await;
            clients.push(stream);
        }
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), data);
    drop(clients);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let finished = server.finished(accepted["id"].as_i64().unwrap()).await;
    assert_eq!(finished["record"]["downloaded_bytes"], 262_151);
    let diagnostics: Value = server
        .get(&format!("/v1/diagnostics?id={id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        diagnostics["snapshot"]["observation"]["outcome"],
        "complete"
    );
    let history = diagnostics["snapshot"]["history"].as_array().unwrap();
    assert!(!history.is_empty() && history.len() <= 120);
    server.stop().await;
    server = Server::start(dir.path(), &[]).await;
    let retained: Value = server
        .get(&format!("/v1/diagnostics?id={id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retained, diagnostics);
    server.stop().await;
}
