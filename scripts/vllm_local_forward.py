"""Bridge a remote vLLM endpoint (Kaggle/Colab tunnel) onto the local port
every other part of the stack already expects.

``ports.template`` documents ``LAB28_VLLM_PORT=8001`` and ``compose.yaml``
hardcodes the API's default ``LAB28_VLLM_BASE_URL`` to
``http://host.docker.internal:8001/v1``. Prometheus's ``lab28-vllm-optional``
scrape job in ``monitoring/prometheus.yml`` targets the same
``host.docker.internal:8001``. Both assume vLLM answers on the host at 8001.

A tunnel (Cloudflare quick tunnel, ngrok, Kaggle's own forwarding) gives you a
public HTTPS URL instead, on a hostname that changes every session. Pointing
``LAB28_VLLM_BASE_URL`` straight at that URL works for the API's own chat
calls, but leaves Prometheus scraping a dead ``host.docker.internal:8001`` —
Prometheus's static config has no way to follow a URL that changes per run.

This script is the missing piece: it listens on ``127.0.0.1:8001`` and
forwards every request to the tunnel URL, so the API, the readiness probe and
Prometheus all keep talking to the one address the rest of the repo was
already written for. Stop it and pointing ``LAB28_VLLM_BASE_URL`` at the
tunnel URL directly still works for everything except the Prometheus scrape.

Usage::

    uv run python scripts/vllm_local_forward.py https://xxxx.trycloudflare.com

Then leave ``LAB28_VLLM_BASE_URL`` at its compose default
(``http://host.docker.internal:8001/v1``) — do not override it.
"""

from __future__ import annotations

import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8001
#: Headers that describe *this* hop, not the one being forwarded; passing them
#: through would send a stale Content-Length or double up Host.
_HOP_BY_HOP = {"host", "content-length", "connection", "transfer-encoding"}


class ForwardingHandler(BaseHTTPRequestHandler):
    target: str = ""  # set by main() before the server starts

    def _forward(self) -> None:
        length = int(self.headers.get("Content-Length", 0) or 0)
        body = self.rfile.read(length) if length else None
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in _HOP_BY_HOP
        }
        request = urllib.request.Request(
            f"{self.target}{self.path}", data=body, headers=headers, method=self.command
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                self._relay(response.status, response.getheaders(), response.read())
        except urllib.error.HTTPError as error:
            self._relay(error.code, error.headers.items(), error.read())
        except Exception as error:  # tunnel down, DNS failure, timeout
            payload = f"vllm_local_forward: upstream unreachable: {error}".encode()
            self._relay(502, [("Content-Type", "text/plain")], payload)

    def _relay(self, status: int, headers: object, body: bytes) -> None:
        self.send_response(status)
        for key, value in headers:
            if key.lower() not in _HOP_BY_HOP:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = do_DELETE = _forward

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write(f"{self.address_string()} {self.command} {self.path} -> "
                          f"{format % args}\n")


def main() -> None:
    if len(sys.argv) != 2 or not sys.argv[1].startswith("http"):
        raise SystemExit(f"usage: {sys.argv[0]} https://<tunnel-host>")
    ForwardingHandler.target = sys.argv[1].rstrip("/")
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), ForwardingHandler)
    print(
        f"forwarding http://{LISTEN_HOST}:{LISTEN_PORT} -> {ForwardingHandler.target}\n"
        "leave LAB28_VLLM_BASE_URL at its compose default; Ctrl+C to stop"
    )
    import contextlib

    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
