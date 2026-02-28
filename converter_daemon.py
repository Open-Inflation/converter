from __future__ import annotations

import argparse
import logging

from converter.daemon import ConverterDaemon, ConverterDaemonHTTPServer, ConverterDaemonRequestHandler


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run converter daemon with queue and HTTP trigger API")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--port", type=int, default=8090, help="HTTP bind port")

    parser.add_argument("--receiver-db", default="", help="Default receiver DB path or DSN")
    parser.add_argument("--catalog-db", default="", help="Default catalog DB path or DSN")
    parser.add_argument("--parser-name", default="fixprice", help="Default parser_name")

    parser.add_argument("--batch-size", type=int, default=250, help="Default batch size per queue job")
    parser.add_argument("--max-batches", type=int, default=0, help="Default max batches per queue job")
    parser.add_argument("--max-queue-size", type=int, default=100, help="Max queued jobs")

    parser.add_argument("--auth-token", default="", help="Optional bearer token for /trigger")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
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
        default_batch_size=args.batch_size,
        default_max_batches=args.max_batches,
        auth_token=(args.auth_token or "").strip() or None,
    )

    print(
        "Converter daemon started:",
        f"listen={args.host}:{args.port}",
        f"default_receiver_db={bool((args.receiver_db or '').strip())}",
        f"default_catalog_db={bool((args.catalog_db or '').strip())}",
        f"default_parser={args.parser_name}",
    )

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        daemon.stop()


if __name__ == "__main__":
    main()
