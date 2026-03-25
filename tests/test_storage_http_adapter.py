from __future__ import annotations

import threading
import unittest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from converter.adapters.storage_http import StorageHTTPRepository


class _DeleteHandler(BaseHTTPRequestHandler):
    server: "_StorageServer"

    def do_DELETE(self) -> None:  # noqa: N802
        self.server.paths.append(self.path)
        self.server.auth_headers.append((self.headers.get("Authorization") or "").strip())
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        self.server.paths.append(self.path)
        self.server.auth_headers.append((self.headers.get("Authorization") or "").strip())
        if self.path.startswith("/api/images/missing.webp/persist"):
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()
            return
        if self.path.startswith("/api/images/") and self.path.endswith("/persist"):
            image_name = self.path.removeprefix("/api/images/").removesuffix("/persist")
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", f"/images-permanent/{image_name}")
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def do_HEAD(self) -> None:  # noqa: N802
        self.server.paths.append(self.path)
        if self.path == "/images/a.webp":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Length", "123")
            self.end_headers()
            return
        if self.path == "/images-permanent/a.webp":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Length", "321")
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def log_message(self, fmt: str, *args: object) -> None:
        return


class _StorageServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address, handler_cls) -> None:
        super().__init__(server_address, handler_cls)
        self.paths: list[str] = []
        self.auth_headers: list[str] = []


class StorageHTTPRepositoryTests(unittest.TestCase):
    def test_delete_images_skips_foreign_urls_and_deduplicates(self) -> None:
        server = _StorageServer(("127.0.0.1", 0), _DeleteHandler)
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
                    f"{base_url}/images-permanent/a.webp",
                    f"{base_url}/images/b.webp",
                    "http://other-host/images/c.webp",
                    "https://example.org/remote.webp",
                ]
            )

            self.assertEqual(
                server.paths,
                ["/api/images/a.webp?scope=both", "/api/images/b.webp?scope=both"],
            )
            self.assertEqual(server.auth_headers, ["Bearer test-token", "Bearer test-token"])
        finally:
            server.shutdown()
            thread.join(timeout=2.0)
            server.server_close()

    def test_persist_images_rewrites_url_with_location(self) -> None:
        server = _StorageServer(("127.0.0.1", 0), _DeleteHandler)
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

            persisted = repo.persist_images(
                [
                    f"{base_url}/images/a.webp",
                    f"{base_url}/images/a.webp",
                    f"{base_url}/images-permanent/a.webp",
                    "https://example.org/remote.webp",
                ]
            )

            self.assertEqual(
                persisted,
                [
                    f"{base_url}/images-permanent/a.webp",
                    f"{base_url}/images-permanent/a.webp",
                    f"{base_url}/images-permanent/a.webp",
                    "https://example.org/remote.webp",
                ],
            )
            self.assertEqual(server.paths, ["/api/images/a.webp/persist"])
        finally:
            server.shutdown()
            thread.join(timeout=2.0)
            server.server_close()

    def test_persist_images_best_effort_keeps_original_on_not_found(self) -> None:
        server = _StorageServer(("127.0.0.1", 0), _DeleteHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host, port = server.server_address
            base_url = f"http://{host}:{port}"
            repo = StorageHTTPRepository(
                base_url=base_url,
                api_token="test-token",
                timeout_seconds=2.0,
                fail_on_error=False,
            )
            source = f"{base_url}/images/missing.webp"
            persisted = repo.persist_images([source])
            self.assertEqual(persisted, [source])
        finally:
            server.shutdown()
            thread.join(timeout=2.0)
            server.server_close()

    def test_get_image_sizes_uses_head_content_length(self) -> None:
        server = _StorageServer(("127.0.0.1", 0), _DeleteHandler)
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

            sizes = repo.get_image_sizes(
                [
                    f"{base_url}/images/a.webp",
                    f"{base_url}/images/a.webp",
                    f"{base_url}/images-permanent/a.webp",
                    f"{base_url}/images_permanent/a.webp",
                    "https://example.org/remote.webp",
                ]
            )

            self.assertEqual(sizes, [123, 123, 321, 321, None])
            self.assertEqual(
                server.paths,
                [
                    "/images/a.webp",
                    "/images-permanent/a.webp",
                ],
            )
        finally:
            server.shutdown()
            thread.join(timeout=2.0)
            server.server_close()


if __name__ == "__main__":
    unittest.main()
