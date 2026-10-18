"""Tests for ingest gaps backfill and CLI gap registration (Task 1)."""

import json
import sqlite3
import tempfile
import unittest

from tracker.config import Config
from tracker.db import DB
from tracker.jobs import Jobs, LiveFetcher, META_INGEST_WATERMARK, META_INGEST_GAPS
from tools.register_ingest_gaps import register_ingest_gaps
from tests.test_ingest_walkback import (
    _start_cursor_server,
    _stop_cursor_server,
    coinbase_tx,
    BASE,
    VALIDATOR,
)


class IngestGapsTest(unittest.TestCase):
    def setUp(self):
        cfg = Config({"VALIDATOR_ADDR": VALIDATOR})
        cfg.data_dir = tempfile.mkdtemp()
        self.cfg = cfg
        self.db = DB(cfg.db_path())

    def tearDown(self):
        self.db.close()

    def _jobs(self, fake_server):
        cfg = self.cfg
        cfg.rpc_url = "http://127.0.0.1:%d" % fake_server.port
        fetcher = LiveFetcher(cfg)
        return Jobs(self.db, cfg, fetcher=fetcher, now_ms=lambda: 1)

    def test_800_gap_drained_and_calls_bounded(self):
        """800-tx gap above the watermark, page 200: after enough polls ALL 800
        in-gap rows are stored and ingest_gaps is empty; assert calls per poll <= 6."""
        wm = BASE
        # 800 in-gap coinbases + 1 preexisting coinbase
        history = [
            coinbase_tx(wm + i + 1, 600000 + i, "cb-%d" % i)
            for i in range(800)
        ] + [coinbase_tx(wm - 1, 600000, "cb-old")]

        srv = _start_cursor_server(history, page_size=200)
        try:
            jobs = self._jobs(srv)
            db = self.db
            db.upsert_reward(VALIDATOR, wm - 1, 600000, 0, "cb-old")
            db.set_meta(META_INGEST_WATERMARK, str(wm))

            polls = 0
            while True:
                calls_before = len(srv.calls)
                jobs.run_ingest()
                calls_this_poll = len(srv.calls) - calls_before
                self.assertLessEqual(calls_this_poll, 6)
                polls += 1

                raw_gaps = db.get_meta(META_INGEST_GAPS)
                gaps = json.loads(raw_gaps) if raw_gaps else []
                if not gaps:
                    break
                if polls > 10:
                    self.fail("Gap did not drain within 10 polls")

            # All 800 in-gap rows + 1 preexisting stored
            total = db.conn.execute("SELECT COUNT(*) FROM rewards").fetchone()[0]
            self.assertEqual(total, 801)
            raw_gaps = db.get_meta(META_INGEST_GAPS)
            gaps = json.loads(raw_gaps) if raw_gaps else []
            self.assertEqual(gaps, [])
        finally:
            _stop_cursor_server(srv)

    def test_head_txs_and_gap_draining(self):
        """New txs arriving at the head between polls are also stored,
        and the gap still drains."""
        wm = BASE
        # 1200 in-gap coinbases: head walk takes 3 pages (600), backfill takes 3 pages (600),
        # but let's configure backfill_pages_per_poll = 1 so it takes multiple polls.
        self.cfg.backfill_pages_per_poll = 1
        history = [
            coinbase_tx(wm + i + 1, 600000 + i, "cb-%d" % i)
            for i in range(800)
        ] + [coinbase_tx(wm - 1, 600000, "cb-old")]

        srv = _start_cursor_server(history, page_size=200)
        try:
            jobs = self._jobs(srv)
            db = self.db
            db.upsert_reward(VALIDATOR, wm - 1, 600000, 0, "cb-old")
            db.set_meta(META_INGEST_WATERMARK, str(wm))

            # Poll 1: head walk (600) + 1 page backfill (200). Gap of 200 remains.
            jobs.run_ingest()
            raw_gaps = db.get_meta(META_INGEST_GAPS)
            gaps = json.loads(raw_gaps) if raw_gaps else []
            self.assertTrue(len(gaps) > 0, "Expected pending gap after poll 1")

            # New txs arrive at the head between polls
            new_head_txs = [
                coinbase_tx(wm + 800 + i + 1, 700000 + i, "cb-new-%d" % i)
                for i in range(50)
            ]
            srv._all = sorted(srv._all + new_head_txs, key=lambda t: -t["blockNumber"])

            # Poll 2: head walk covers new head txs, then backfill drains remaining gap
            jobs.run_ingest()

            # Verify new head txs stored
            new_stored = db.conn.execute(
                "SELECT COUNT(*) FROM rewards WHERE block > ?", (wm + 800,)
            ).fetchone()[0]
            self.assertEqual(new_stored, 50)

            # Drain any remaining gaps
            polls = 0
            while True:
                raw_gaps = db.get_meta(META_INGEST_GAPS)
                gaps = json.loads(raw_gaps) if raw_gaps else []
                if not gaps:
                    break
                jobs.run_ingest()
                polls += 1
                if polls > 10:
                    self.fail("Gap did not drain")

            # All 800 in-gap + 50 new + 1 preexisting stored
            total = db.conn.execute("SELECT COUNT(*) FROM rewards").fetchone()[0]
            self.assertEqual(total, 851)
        finally:
            _stop_cursor_server(srv)

    def test_429_mid_walk_keeps_gap_and_resumes(self):
        """429 mid-walk (make the fake answer HTTP 429 on one call) keeps the gap
        and resumes next poll without loss."""
        wm = BASE
        history = [
            coinbase_tx(wm + i + 1, 600000 + i, "cb-%d" % i)
            for i in range(800)
        ] + [coinbase_tx(wm - 1, 600000, "cb-old")]

        srv = _start_cursor_server(history, page_size=200)
        try:
            jobs = self._jobs(srv)
            db = self.db
            db.upsert_reward(VALIDATOR, wm - 1, 600000, 0, "cb-old")
            db.set_meta(META_INGEST_WATERMARK, str(wm))

            # Calls 1, 2, 3 are head walk. Call 4 (first backfill call) fails with 429.
            srv.fail_429_on_call = 4

            jobs.run_ingest()

            # Gap must still be present
            raw_gaps = db.get_meta(META_INGEST_GAPS)
            gaps = json.loads(raw_gaps) if raw_gaps else []
            self.assertEqual(len(gaps), 1)

            # Clear 429 so next poll succeeds
            srv.fail_429_on_call = None

            # Poll 2 resumes backfill without loss
            jobs.run_ingest()

            raw_gaps = db.get_meta(META_INGEST_GAPS)
            gaps = json.loads(raw_gaps) if raw_gaps else []
            self.assertEqual(gaps, [])

            total = db.conn.execute("SELECT COUNT(*) FROM rewards").fetchone()[0]
            self.assertEqual(total, 801)
        finally:
            _stop_cursor_server(srv)

    def test_register_ingest_gaps_cli_tool(self):
        """Test tools/register_ingest_gaps.py with a temp DB."""
        db_path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "CREATE TABLE rewards (validator TEXT, block INTEGER, amount_luna INTEGER, ts_ms INTEGER, tx_hash TEXT, PRIMARY KEY (validator, block))"
            )
            conn.execute("INSERT INTO rewards VALUES(?, ?, ?, ?, ?)", (VALIDATOR, 100, 1000, 0, "h-100"))
            conn.execute("INSERT INTO rewards VALUES(?, ?, ?, ?, ?)", (VALIDATOR, 200, 1000, 0, "h-200"))  # gap = 100 > 60
            conn.execute("INSERT INTO rewards VALUES(?, ?, ?, ?, ?)", (VALIDATOR, 240, 1000, 0, "h-240"))  # gap = 40 <= 60
            conn.execute("INSERT INTO rewards VALUES(?, ?, ?, ?, ?)", (VALIDATOR, 400, 1000, 0, "h-400"))  # gap = 160 > 60
            conn.commit()
        finally:
            conn.close()

        # Dry run does not write meta
        dry_gaps = register_ingest_gaps(db_path, dry_run=True)
        self.assertEqual(len(dry_gaps), 2)
        self.assertEqual(dry_gaps[0], {"cursor": "h-200", "floor": 100})
        self.assertEqual(dry_gaps[1], {"cursor": "h-400", "floor": 240})

        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute("SELECT v FROM meta WHERE k='ingest_gaps'").fetchone()
            self.assertIsNone(row)
        finally:
            conn.close()

        # Real run registers gaps
        real_gaps = register_ingest_gaps(db_path, dry_run=False)
        self.assertEqual(len(real_gaps), 2)

        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute("SELECT v FROM meta WHERE k='ingest_gaps'").fetchone()
            self.assertIsNotNone(row)
            stored = json.loads(row[0])
            self.assertEqual(stored, [
                {"cursor": "h-200", "floor": 100},
                {"cursor": "h-400", "floor": 240},
            ])
        finally:
            conn.close()

        # Second run skips already registered floors
        second_gaps = register_ingest_gaps(db_path, dry_run=False)
        self.assertEqual(len(second_gaps), 0)


if __name__ == "__main__":
    unittest.main()
