#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional, Tuple


RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def build_file(path: Path, size_bytes: int) -> None:
    if path.exists() and path.stat().st_size == size_bytes:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    chunk = b"0123456789abcdef" * 4096  # 64 KiB
    remaining = size_bytes
    with path.open("wb") as f:
        while remaining > 0:
            take = min(len(chunk), remaining)
            f.write(chunk[:take])
            remaining -= take


def ensure_files(root: Path) -> None:
    build_file(root / "small.txt", 1024)
    build_file(root / "1mb.bin", 1 * 1024 * 1024)
    build_file(root / "5mb.bin", 5 * 1024 * 1024)


class RangeHandler(SimpleHTTPRequestHandler):
    server_version = "arkamaTestHTTP/1.0"

    def _parse_range(self, size: int) -> Optional[Tuple[int, int]]:
        header = self.headers.get("Range")
        if not header:
            return None
        match = RANGE_RE.match(header.strip())
        if not match:
            return None
        start_s, end_s = match.groups()
        if start_s == "" and end_s == "":
            return None
        if start_s == "":
            # suffix bytes
            suffix = int(end_s)
            if suffix <= 0:
                return None
            start = max(0, size - suffix)
            end = size - 1
            return (start, end)
        start = int(start_s)
        end = int(end_s) if end_s != "" else size - 1
        if start >= size or end < start:
            return None
        end = min(end, size - 1)
        return (start, end)

    def send_head(self):  # noqa: ANN001
        path = self.translate_path(self.path)
        if os.path.isdir(path):
            return super().send_head()
        try:
            f = open(path, "rb")
        except OSError:
            self.send_error(404, "File not found")
            return None
        st = os.fstat(f.fileno())
        size = st.st_size
        ctype = self.guess_type(path)
        byte_range = self._parse_range(size)
        if byte_range is None:
            self.send_response(200)
            self.send_header("Content-type", ctype)
            self.send_header("Content-Length", str(size))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            return (f, None)

        start, end = byte_range
        length = end - start + 1
        self.send_response(206)
        self.send_header("Content-type", ctype)
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        return (f, (start, length))

    def do_HEAD(self) -> None:  # noqa: N802
        result = self.send_head()
        if result is None:
            return
        f, _ = result
        f.close()

    def do_GET(self) -> None:  # noqa: N802
        result = self.send_head()
        if result is None:
            return
        f, range_info = result
        try:
            if range_info is None:
                self.copyfile(f, self.wfile)
                return
            start, length = range_info
            f.seek(start)
            remaining = length
            bufsize = 64 * 1024
            while remaining > 0:
                chunk = f.read(min(bufsize, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)
        finally:
            f.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local file server with Range support for arkama testing.")
    parser.add_argument("--bind", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8088, help="Port (default: 8088)")
    parser.add_argument("--dir", default="testdata", help="Directory to serve (default: ./testdata)")
    parser.add_argument("--no-generate", action="store_true", help="Do not generate sample files")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.dir).resolve()
    if not args.no_generate:
        ensure_files(root)
    os.chdir(root)
    httpd = ThreadingHTTPServer((args.bind, args.port), RangeHandler)
    print(f"Serving {root} on http://{args.bind}:{args.port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
