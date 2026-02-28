from __future__ import annotations

import threading
import unittest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from converter.adapters.storage_http import StorageHTTPRepository


class _DeleteHandler(BaseHTTPRequestHandler):
    server: "_DeleteServer"

    def do_DELETE(self) -> None:  # noqa: N802
        self.server.paths.append(self.path)
        self.server.auth_headers.append((self.headers.get("Authorization") or "").strip())
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def log_message(self, fmt: str, *args: object) -> None:
        return


class _DeleteServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address, handler_cls) -> None:
        super().__init__(server_address, handler_cls)
        self.paths: list[str] = []
        self.auth_headers: list[str] = []


class StorageHTTPRepositoryTests(unittest.TestCase):
    def test_delete_images_skips_foreign_urls_and_deduplicates(self) -> None:
        server = _DeleteServer(("127.0.0.1", 0), _DeleteHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host, port = server.server_address
            base_url = f"http://{host}:{port}"
            repo = StorageHTTPRepository(
                base_url=base_url,
                api_token="test-token",
                timeout_seconds=2.0,
                fail_on_error=True,
            )
            repo.delete_images(
                [
                    f"{base_url}/images/a.webp",
                    f"{base_url}/images/a.webp",
                    f"{base_url}/images/b.webp",
                    "http://other-host/images/c.webp",
                    "https://example.org/remote.webp",
                ]
            )

            self.assertEqual(server.paths, ["/api/images/a.webp", "/api/images/b.webp"])
            self.assertEqual(server.auth_headers, ["Bearer test-token", "Bearer test-token"])
        finally:
            server.shutdown()
            thread.join(timeout=2.0)
            server.server_close()


if __name__ == "__main__":
    unittest.main()
