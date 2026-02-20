#!/usr/bin/env python3
import argparse
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional, Tuple


def parse_range(value: str, total: int) -> Optional[Tuple[int, int]]:
    if not value or not value.startswith("bytes="):
        return None
    value = value[len("bytes=") :]
    if "," in value:
        return None
    start_str, sep, end_str = value.partition("-")
    if sep != "-":
        return None
    if start_str == "":
        try:
            suffix = int(end_str)
        except ValueError:
            return None
        if suffix <= 0:
            return None
        start = max(total - suffix, 0)
        end = total - 1
        return (start, end)
    try:
        start = int(start_str)
    except ValueError:
        return None
    if end_str == "":
        end = total - 1
    else:
        try:
            end = int(end_str)
        except ValueError:
            return None
    if start > end or start < 0:
        return None
    return (start, min(end, total - 1))


class FileHandler(BaseHTTPRequestHandler):
    file_path: Path = Path()
    file_size: int = 0

    def do_HEAD(self) -> None:  # noqa: N802
        self._send_headers(200, self.file_size, None)

    def do_GET(self) -> None:  # noqa: N802
        total = self.file_size
        range_header = self.headers.get("Range")
        byte_range = parse_range(range_header or "", total)
        if byte_range is None:
            self._send_headers(200, total, None)
            self._send_body(0, total)
            return

        start, end = byte_range
        if start >= total:
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{total}")
            self.end_headers()
            return

        length = end - start + 1
        self._send_headers(206, length, (start, end, total))
        self._send_body(start, length)

    def _send_headers(self, status: int, length: int, content_range: Optional[Tuple[int, int, int]]) -> None:
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        if content_range is not None:
            start, end, total = content_range
            self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
        self.end_headers()

    def _send_body(self, offset: int, length: int) -> None:
        chunk_size = 1024 * 1024
        remaining = length
        with open(self.file_path, "rb") as handle:
            handle.seek(offset)
            while remaining > 0:
                to_read = min(chunk_size, remaining)
                data = handle.read(to_read)
                if not data:
                    break
                self.wfile.write(data)
                remaining -= len(data)

    def log_message(self, format: str, *args: object) -> None:  # noqa: N802
        return


def ensure_file(path: Path, size: int) -> None:
    if path.exists() and path.stat().st_size == size:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        chunk = os.urandom(1024 * 1024)
        remaining = size
        while remaining > 0:
            to_write = min(len(chunk), remaining)
            handle.write(chunk[:to_write])
            remaining -= to_write


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve a large file with Range support for arkama_cli tests.")
    parser.add_argument("--path", default="tmp/large.bin", help="File path to serve/create.")
    parser.add_argument("--size-mb", type=int, default=256, help="File size to create if missing.")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on.")
    args = parser.parse_args()

    file_path = Path(args.path)
    ensure_file(file_path, args.size_mb * 1024 * 1024)

    FileHandler.file_path = file_path
    FileHandler.file_size = file_path.stat().st_size

    server = ThreadingHTTPServer(("127.0.0.1", args.port), FileHandler)
    print(f"Serving {file_path} ({FileHandler.file_size} bytes) at http://127.0.0.1:{args.port}/file.bin")
    server.serve_forever()


if __name__ == "__main__":
    main()
