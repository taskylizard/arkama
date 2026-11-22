#!/usr/bin/env python3
"""Headless v1 client example. Read commands (including download secrets) from stdin."""

import argparse
import http.client
import ipaddress
import json
from pathlib import Path
import ssl
import sys
from urllib.parse import urlencode, urlsplit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("server", help="Loopback HTTP or certificate-verified HTTPS origin")
    auth = parser.add_mutually_exclusive_group(required=True)
    auth.add_argument("--key-file", type=Path, help="Owner-readable control API key file")
    auth.add_argument("--no-auth", action="store_true", help="Explicit credential-free loopback use")
    parser.add_argument("--watch", action="store_true", help="Resnapshot a submitted/shown job until stopped")
    args = parser.parse_args()
    origin = urlsplit(args.server)
    if origin.username or origin.password or origin.path not in ("", "/") or origin.query or origin.fragment:
        raise ValueError("server must be an origin without credentials")
    try:
        loopback = ipaddress.ip_address(origin.hostname).is_loopback
    except ValueError:
        loopback = origin.hostname == "localhost"
    if origin.scheme not in ("http", "https") or (origin.scheme == "http" and not loopback):
        raise ValueError("remote clients require certificate-verified HTTPS")
    if args.no_auth and not loopback:
        raise ValueError("this example limits credential-free use to loopback")
    headers = {"Content-Type": "application/json"}
    if args.key_file:
        headers["Authorization"] = "Bearer " + args.key_file.read_text().strip()

    def call(method, path, command=None):
        body = None if command is None else json.dumps(command).encode()
        if body is not None and len(body) > 65536:
            raise ValueError("command exceeds 64 KiB")
        connection_type = http.client.HTTPSConnection if origin.scheme == "https" else http.client.HTTPConnection
        options = {"context": ssl.create_default_context()} if origin.scheme == "https" else {}
        connection = connection_type(origin.hostname, origin.port, timeout=30, **options)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read(2097153)
            if len(data) > 2097152:
                raise ValueError("reply exceeds 2 MiB")
            if response.status != 200:
                # Do not print URLs, raw bodies or authentication headers on failure.
                raise ValueError(f"server refused request (HTTP {response.status})")
            return json.loads(data)
        finally:
            connection.close()

    data = sys.stdin.buffer.read(65537)
    if len(data) > 65536:
        raise ValueError("command exceeds 64 KiB")
    command = json.loads(data)
    info = call("GET", "/v1/info")
    if info["api_version"] != 1:
        raise ValueError("unsupported API version")
    if command.get("op") == "submit" and not command["submission"].get("handoff_id"):
        raise ValueError("use a stable handoff_id so ambiguous acceptance can be reconciled")
    reply = call("POST", "/v1/commands", command)
    print(json.dumps(reply), flush=True)
    if not args.watch:
        return
    job_id = reply.get("id") if reply["type"] == "accepted" else command.get("id")
    if job_id is None:
        raise ValueError("watch requires a submitted or selected job")
    while True:
        # Capture revision before reading the job so a concurrent change is not lost.
        status = call("POST", "/v1/commands", {"op": "status"})
        job = call("POST", "/v1/commands", {"op": "show", "id": job_id})
        record = job["record"]
        print(json.dumps({"id": job_id, "status": record["status"], "downloaded_bytes": record["downloaded_bytes"]}), flush=True)
        if record["status"] in ("finished", "failed", "cancelled", "paused"):
            return
        query = urlencode({"instance": info["instance"], "revision": status["revision"]})
        update = call("GET", "/v1/updates?" + query)
        info["instance"] = update["instance"]
        # Updates are invalidations, not a log; always fetch authoritative snapshots again.


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, http.client.HTTPException):
        sys.exit("Client failed; check access/input and resnapshot before retrying. Keep the same handoff ID.")
