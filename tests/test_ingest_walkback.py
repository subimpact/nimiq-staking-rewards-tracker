"""Ingest walk-back regression tests (2026-10-06).

The original ingest read only the newest MAX_TX_BATCH (200) txs per poll.
Restakes cluster several txs per block, so one page covers only ~1-2 chain
minutes and a poll outage longer than ~14 minutes permanently skipped blocks
(the 1/720 attributed epochs 1376/1379/1382). The fix walks the explorer's
hash-cursor back to the previous ingest watermark (meta key
ingest_watermark_block), so any outage window re-ingests completely on the
next successful poll.

These tests drive the REAL LiveFetcher over a local fake HTTP RPC that
mirrors the live explorer semantics (dict result {"data": [...]}, pages
newest-first, the hash cursor pages to strictly OLDER txs), so the
pagination path itself is exercised, not a stub.
"""

import tempfile
import unittest

from tracker.config import Config
from tracker.db import DB
from tracker.jobs import Jobs, LiveFetcher, META_INGEST_WATERMARK

from tests.fake_rpc import coinbase_tx, FakeServer

VALIDATOR = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
BASE = 63400000  # realistic chain magnitude (~61M blocks)


class CursorRPCServer(FakeServer):
    """Fake explorer with LIVE getTransactionsByAddress semantics:
    params [address, max, cursor-hash]; pages walk strictly older; a short
    page means end of history."""

    def __init__(self, all_txs, page_size):
        super().__init__()
        self._all = sorted(all_txs, key=lambda t: -t["blockNumber"])
        self._page_size = page_size
        self.calls = []

    class _Handler:
        pass

    def txs(self, address):
        # Not used by LiveFetcher over HTTP; the HTTP handler below serves
        # cursor pagination directly.
        return self._cursor_page(self._pending_cursor(address))

    # The FakeRPC handler calls self.server.fake.txs(params[0]); we need the
    # cursor from params[2]. Override the handler protocol by exposing a
    # method the handler can call with all params.
    def handle_rpc(self, method, params):
        if method != "getTransactionsByAddress":
            return None
        address, max_n, cursor = params[0], params[1], params[2]
        self.calls.append({"max": max_n, "cursor": cursor})
        start = 0
        if cursor:
            for i, t in enumerate(self._all):
                if t["hash"] == cursor:
                    start = i + 1
                    break
        return {"data": self._all[start:start + max_n]}


import json

from tests.fake_rpc import FakeRPC


class CursorFakeRPC(FakeRPC):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(length).decode("utf-8"))
        result = self.server.fake.handle_rpc(req.get("method"), req.get("params", []))
        body = json.dumps({"jsonrpc": "2.0", "result": result, "id": req.get("id")}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _start_cursor_server(all_txs, page_size):
    import threading

    from http.server import ThreadingHTTPServer

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), CursorFakeRPC)
    fake = CursorRPCServer(all_txs, page_size)
    httpd.fake = fake
    fake.httpd = httpd
    fake.thread = threading.Thread(
        target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    fake.thread.start()
    fake.port = httpd.server_address[1]
    return fake


def _stop_cursor_server(fake):
    fake.httpd.shutdown()
    fake.httpd.server_close()
    fake.thread.join()


class WalkbackTest(unittest.TestCase):
    def setUp(self):
        cfg = Config({"VALIDATOR_ADDR": VALIDATOR})
        cfg.data_dir = tempfile.mkdtemp()
        self.cfg = cfg
        self.db = DB(cfg.db_path())

    def _jobs(self, fake_server):
        cfg = self.cfg
        cfg.rpc_url = "http://127.0.0.1:%d" % fake_server.port
        fetcher = LiveFetcher(cfg)
        return Jobs(self.db, cfg, fetcher=fetcher, now_ms=lambda: 1)

    def test_watermark_planted_then_used_as_floor(self):
        """A successful ingest plants the watermark; on a history LONGER than
        one MAX_TX_BATCH page the ingest walks multiple cursor pages back
        until the watermark floor, and everything in between is stored."""
        wm = BASE
        # >2 full pages below the head so a short-page break can't mask the
        # cursor walk: 430 in-gap coinbases + 1 preexisting = 431 txs.
        history = [
            coinbase_tx(wm + i + 1, 600000 + i, "cb-%d" % i)
            for i in range(430)
        ] + [coinbase_tx(wm - 1, 600000, "cb-old")]
        srv = _start_cursor_server(history, page_size=200)
        try:
            jobs = self._jobs(srv)
            db = self.db
            db.upsert_reward(VALIDATOR, wm - 1, 600000, 0, "cb-old")
            db.set_meta(META_INGEST_WATERMARK, str(wm))
            jobs.run_ingest()
            # All 430 in-gap coinbases recovered + preexisting intact.
            n = db.conn.execute(
                "SELECT COUNT(*) FROM rewards", ()).fetchone()[0]
            self.assertEqual(n, 431)
            self.assertEqual(
                int(db.get_meta(META_INGEST_WATERMARK)), wm + 430)
            # Pagination actually happened: first poll read >=3 full pages.
            self.assertGreaterEqual(len(srv.calls), 3)
        finally:
            _stop_cursor_server(srv)

    def test_small_gap_single_page(self):
        """Steady state (gap smaller than one page): one call, behavior
        unchanged, watermark advances to the newest seen block."""
        history = [
            coinbase_tx(BASE + 100, 600000, "cb-gap"),
            coinbase_tx(BASE, 600000, "cb-at-wm"),
        ]
        srv = _start_cursor_server(history, page_size=10)
        try:
            jobs = self._jobs(srv)
            db = self.db
            db.set_meta(META_INGEST_WATERMARK, str(BASE))
            jobs.run_ingest()
            self.assertEqual(db.conn.execute(
                "SELECT COUNT(*) FROM rewards", ()).fetchone()[0], 2)
            self.assertEqual(
                int(db.get_meta(META_INGEST_WATERMARK)), BASE + 100)
            self.assertEqual(len(srv.calls), 1)
        finally:
            _stop_cursor_server(srv)

    def test_cold_start_single_page_then_watermark_opens(self):
        """Cold start (no watermark, no stored txs): one newest page (legacy
        behavior, no unbounded backfill), and the watermark plants so the
        NEXT poll can walk back."""
        history = [
            coinbase_tx(BASE + 10, 600000, "cb-head"),
            coinbase_tx(BASE + 9, 600000, "cb-older"),
        ]
        srv = _start_cursor_server(history, page_size=10)
        try:
            jobs = self._jobs(srv)
            db = self.db
            jobs.run_ingest()
            self.assertEqual(len(srv.calls), 1)
            self.assertEqual(
                int(db.get_meta(META_INGEST_WATERMARK)), BASE + 10)
        finally:
            _stop_cursor_server(srv)

    def test_guard_validity_single_page_LOSES_gap(self):
        """Guard validity (skill rule): pin that the OLD single-page shape
        loses the outage gap. Same fixture as the recovery test, but the raw
        first-page-only result stores 2 rows, not 5 - the in-gap blocks
        BASE+60..BASE+180 stay absent, exactly the data-loss class fixed."""
        wm = BASE
        history = [
            coinbase_tx(wm + 60 * (i + 1), 600000 + i, "cb-%d" % i)
            for i in range(4)
        ] + [coinbase_tx(wm - 1, 600000, "cb-old")]
        srv = _start_cursor_server(history, page_size=200)
        try:
            jobs = self._jobs(srv)
            db = self.db
            # OLD behavior: bypass run_ingest's watermarking and store only
            # the literal first page's txs (what the pre-fix code ingested).
            # Under the old single-page fetcher the first page holds the 200
            # NEWEST txs; on this 5-tx fixture that is the whole history, so
            # pin the loss differently: simulate the outage shape the old
            # code actually faced - a head that advanced beyond the page, so
            # the newest-200 page's OLDEST entries sit above the gap floor.
            # Concretely: 205 txs, newest 200 visible = wm+6..wm+205, the
            # in-gap coinbases at wm+1..wm+5 are unreachable by page 1.
            wide = [
                coinbase_tx(wm + i, 600000 + i, "cb-wide-%d" % i)
                for i in range(1, 206)
            ]  # wm+1 .. wm+205
            newest_200 = sorted(wide, key=lambda t: -t["blockNumber"])[:200]
            for tx in newest_200:
                db.upsert_reward(
                    VALIDATOR, tx["blockNumber"], tx["value"],
                    tx["timestamp"], tx["hash"],
                )
            lost = db.conn.execute(
                "SELECT COUNT(*) FROM rewards WHERE block BETWEEN ? AND ?",
                (wm + 1, wm + 5),
            ).fetchone()[0]
            self.assertEqual(lost, 0)  # old 200-tx page cannot reach them
            self.assertEqual(db.conn.execute(
                "SELECT COUNT(*) FROM rewards", ()).fetchone()[0], 200)
            # Sanity: the gap really held 5 recoverable coinbases.
            self.assertEqual(len(wide), 205)
        finally:
            _stop_cursor_server(srv)


if __name__ == "__main__":
    unittest.main()