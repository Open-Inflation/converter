from __future__ import annotations

import argparse
import logging

from converter.daemon import ConverterDaemon, ConverterDaemonHTTPServer, ConverterDaemonRequestHandler


LOGGER = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run converter daemon with queue and HTTP trigger API")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--port", type=int, default=8090, help="HTTP bind port")

    parser.add_argument("--receiver-db", default="", help="Default receiver DB path or DSN")
    parser.add_argument("--catalog-db", default="", help="Default catalog DB path or DSN")
    parser.add_argument("--parser-name", default="fixprice", help="Default parser_name")

    parser.add_argument("--receiver-fetch-size", type=int, default=2000, help="Default receiver fetch size")
    parser.add_argument("--write-chunk-size", type=int, default=1000, help="Default atomic write chunk size")
    parser.add_argument("--sync-version", choices=("v2",), default="v2", help="Default sync version")
    parser.add_argument("--writer-mode", choices=("mysql_v2",), default="mysql_v2", help="Default writer mode")
    parser.add_argument("--max-batches", type=int, default=0, help="Default max batches per queue job")
    parser.add_argument("--max-queue-size", type=int, default=100, help="Max queued jobs")

    parser.add_argument("--auth-token", default="", help="Optional bearer token for /trigger")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )

    daemon = ConverterDaemon(max_queue_size=args.max_queue_size)
    daemon.start()

    server = ConverterDaemonHTTPServer(
        (args.host, int(args.port)),
        ConverterDaemonRequestHandler,
        daemon=daemon,
        default_receiver_db=(args.receiver_db or "").strip() or None,
        default_catalog_db=(args.catalog_db or "").strip() or None,
        default_parser_name=args.parser_name,
        default_receiver_fetch_size=args.receiver_fetch_size,
        default_write_chunk_size=args.write_chunk_size,
        default_sync_version=args.sync_version,
        default_writer_mode=args.writer_mode,
        default_max_batches=args.max_batches,
        auth_token=(args.auth_token or "").strip() or None,
    )

    LOGGER.info(
        "Converter daemon started: listen=%s:%s default_receiver_db=%s default_catalog_db=%s default_parser=%s sync_version=%s writer_mode=%s receiver_fetch_size=%s write_chunk_size=%s max_batches=%s auth_enabled=%s",
        args.host,
        int(args.port),
        bool((args.receiver_db or "").strip()),
        bool((args.catalog_db or "").strip()),
        args.parser_name,
        args.sync_version,
        args.writer_mode,
        int(args.receiver_fetch_size),
        int(args.write_chunk_size),
        int(args.max_batches),
        bool((args.auth_token or "").strip()),
    )

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        LOGGER.info("Converter daemon interrupted by keyboard signal")
    finally:
        LOGGER.info("Converter daemon shutting down")
        server.server_close()
        daemon.stop()
        LOGGER.info("Converter daemon stopped")


if __name__ == "__main__":
    main()
