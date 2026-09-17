"""Entrypoint for the Nimiq Staking Rewards Tracker.

Starts a read-only HTTP API thread on 0.0.0.0:8649 and a scheduler loop that
runs the four jobs (snapshot, rewards ingest, cycle close, verify) every
POLL_SECONDS (default 60). Handles SIGTERM for graceful shutdown.
"""

import signal
import sys
import threading
import time

from tracker.api import serve
from tracker.config import load
from tracker.db import DB
from tracker.jobs import Jobs


def _run_scheduler(jobs, db, stop_event, interval):
    while not stop_event.is_set():
        try:
            jobs.run_snapshot()
            jobs.run_ingest()
            jobs.run_cycle_close()
            jobs.run_verify()
        except Exception as exc:  # noqa: BLE001
            print("scheduler error: %s" % exc, file=sys.stderr)
        stop_event.wait(interval)


def main():
    cfg = load()
    db = DB(cfg.db_path())

    server = serve(db, cfg)
    server.started_at_ms = int(time.time() * 1000)

    stop_event = threading.Event()

    def shutdown(signum, frame):  # noqa: ARG001
        print("received signal %s, shutting down" % signum)
        stop_event.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.5}, daemon=True
    )
    thread.start()

    jobs = Jobs(db, cfg)
    scheduler = threading.Thread(
        target=_run_scheduler,
        args=(jobs, db, stop_event, cfg.poll_seconds),
        daemon=True,
    )
    scheduler.start()

    print("api listening on 0.0.0.0:%s (db=%s)" % (cfg.port, cfg.db_path()))
    scheduler.join()


if __name__ == "__main__":
    main()
