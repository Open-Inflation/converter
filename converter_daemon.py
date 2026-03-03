from __future__ import annotations

import argparse
import logging
from time import sleep
from itertools import cycle

from converter.daemon import ConverterDaemon, PollingJob


LOGGER = logging.getLogger(__name__)

AVAILABLE_PARSERS = ("fixprice", "chizhik", "perekrestok")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run converter daemon in polling mode")

    parser.add_argument("--receiver-db", required=True, help="Receiver DB path or DSN")
    parser.add_argument("--catalog-db", required=True, help="Catalog DB path or DSN")
    parser.add_argument("--parser-name", default="all", help="Parser name")

    parser.add_argument("--receiver-fetch-size", type=int, default=2000, help="Receiver fetch size")
    parser.add_argument("--write-chunk-size", type=int, default=1000, help="Atomic write chunk size")
    parser.add_argument("--sync-version", choices=("v2",), default="v2", help="Sync version")
    parser.add_argument("--writer-mode", choices=("mysql_v2",), default="mysql_v2", help="Writer mode")
    parser.add_argument("--max-batches", type=int, default=0, help="Max batches per polling cycle")
    parser.add_argument("--poll-interval-sec", type=float, default=5.0, help="Polling interval in seconds")

    parser.add_argument("--log-level", default="INFO", help="Log level")
    return parser


def build_job(args, parser_name: str) -> PollingJob:
    return PollingJob(
        receiver_db=args.receiver_db,
        catalog_db=args.catalog_db,
        parser_name=parser_name,
        receiver_fetch_size=max(1, int(args.receiver_fetch_size)),
        write_chunk_size=max(1, int(args.write_chunk_size)),
        sync_version=args.sync_version,
        writer_mode=args.writer_mode,
        max_batches=max(0, int(args.max_batches)),
    )


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )

    poll_interval = max(0.1, float(args.poll_interval_sec))

    if args.parser_name == "all":
        parser_cycle = cycle(AVAILABLE_PARSERS)
    else:
        parser_cycle = cycle((args.parser_name,))

    daemon: ConverterDaemon | None = None

    try:
        while True:
            parser_name = next(parser_cycle)

            if daemon is not None:
                daemon.stop()

            job = build_job(args, parser_name)
            daemon = ConverterDaemon(job=job, poll_interval_sec=poll_interval)
            daemon.start()

            LOGGER.info(
                "Converter daemon started with parser=%s poll_interval_sec=%.2f",
                parser_name,
                poll_interval,
            )

            sleep(poll_interval)

    except KeyboardInterrupt:
        LOGGER.info("Converter daemon interrupted by keyboard signal")
    finally:
        if daemon is not None:
            LOGGER.info("Converter daemon shutting down")
            daemon.stop()
            LOGGER.info("Converter daemon stopped")


if __name__ == "__main__":
    main()