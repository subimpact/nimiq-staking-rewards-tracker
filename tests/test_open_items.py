"""Regression tests for the 2026-10-19 open-items pass:

- F8  per-thread SQLite connections (API reads never see the writer's
      uncommitted transaction)
- F10 generality: stakers URL keyed on the validator, reward wallet excluded
      from staker attribution, operator fee + deposit-retaining denominator,
      ledger rows carry the validator
- F12 a partial staker list never tombstones live stakers
- F14 opt-in snapshot downsampling
- F16 API: 404 for unknown/foreign cycles, decoded query values, no internal
      error text in 500 bodies
"""

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request

from tracker.api import serve
from tracker.config import Config, STAKING_CONTRACT_ADDR
from tracker.db import DB
from tracker.jobs import Jobs, _staker_from_related

from tests.fake_rpc import (
    FakeServer,
    STAKER_A,
    STAKER_B,
    STAKER_C,
    VALIDATOR,
    coinbase_tx,
    staker_row,
)
from tests.test_verify import (
    _FakeFetcher,
    closed_cycle,
    closing_restakes_attributed,
    run_two_pass,
    stage_open_close,
)

OTHER = "NQ55 OTHE RVAL IDAT ORXX XXXX XXXX XXXX XXXX"
REWARD_WALLET = "NQ66 REWA RDWA LLET XXXX XXXX XXXX XXXX XXXX"
COINBASE = 2000000


def _env(fake, **extra):
    env = {
        "RPC_URL": fake.url(),
        "STAKERS_URL_TEMPLATE": fake.url() + "/api/stakers/{address}",
        "VALIDATORS_URL_TEMPLATE": fake.url() + "/api/validators",
        "VALIDATOR_ADDR": VALIDATOR,
        "REWARD_ADDR": VALIDATOR,
        "DATA_DIR": fake.data_dir,
    }
    env.update(extra)
    return Config(env)


def _jobs(fake, total, **extra):
    fake.set_validator(total, 3)
    cfg = _env(fake, **extra)
    db = DB(os.path.join(fake.data_dir, "tracker.db"))
    counter = {"ms": 1_700_000_000_000}

    def now_ms():
        counter["ms"] += 1
        return counter["ms"]

    return cfg, db, Jobs(db, cfg, fetcher=_FakeFetcher(cfg, fake), now_ms=now_ms)


def _run_window(fake, jobs, amounts):
    stakers = [STAKER_A, STAKER_B, STAKER_C]
    stage_open_close(
        fake,
        [staker_row(STAKER_A, 40000000), staker_row(STAKER_B, 30000000),
         staker_row(STAKER_C, 30000000)],
        [staker_row(STAKER_A, 40000000 + amounts[0]),
         staker_row(STAKER_B, 30000000 + amounts[1]),
         staker_row(STAKER_C, 30000000 + amounts[2])],
    )
    fake.set_staged_txs([
        [coinbase_tx(50, COINBASE, "tx-cb-0")],
        closing_restakes_attributed(amounts, stakers),
    ])
    run_two_pass(jobs)


class DistributionRuleTest(unittest.TestCase):
    def test_fee_ppm_verifies_fee_charging_pool(self):
        # 10% fee: distributable 1.8M of a 2M coinbase -> 720k/540k/540k.
        fake = FakeServer()
        try:
            cfg, db, jobs = _jobs(fake, 100000000, VALIDATOR_FEE_PPM="100000")
            _run_window(fake, jobs, [720000, 540000, 540000])
            c = closed_cycle(db)[0]
            self.assertEqual(c["status"], "VERIFIED")
            exp = {s["staker_address"]: s["expected_luna"] for s in db.cycle_shares(c["id"])}
            self.assertEqual(exp[STAKER_A], 720000)
            db.close()
        finally:
            fake.close()

    def test_default_rule_flags_the_same_fee_payout(self):
        # Same payout under the default 0% rule is a 10% underpayment. Since
        # M1 a single short window is carry (paid next cycle) unless the
        # carry limit is 0, so this one-window test runs the strict limit;
        # tests/test_carry.py covers the default limit over many windows.
        fake = FakeServer()
        try:
            cfg, db, jobs = _jobs(fake, 100000000, CARRY_MAX_BATCHES="0")
            _run_window(fake, jobs, [720000, 540000, 540000])
            self.assertEqual(closed_cycle(db)[0]["status"], "MISMATCH")
            db.close()
        finally:
            fake.close()

    def test_denominator_total_keeps_deposit_slice(self):
        # Validator total 200M (100M deposit) -> stakers get half the coinbase.
        fake = FakeServer()
        try:
            cfg, db, jobs = _jobs(fake, 200000000, DENOMINATOR_MODE="total")
            _run_window(fake, jobs, [400000, 300000, 300000])
            c = closed_cycle(db)[0]
            self.assertEqual(c["status"], "VERIFIED")
            db.close()
        finally:
            fake.close()

    def test_bad_denominator_mode_rejected(self):
        with self.assertRaises(ValueError):
            Config({"DENOMINATOR_MODE": "deposit"})

    def test_ledger_rows_carry_validator(self):
        fake = FakeServer()
        try:
            cfg, db, jobs = _jobs(fake, 100000000)
            _run_window(fake, jobs, [800000, 600000, 600000])
            vals = {r[0] for r in db.conn.execute("SELECT validator FROM staker_rewards")}
            self.assertEqual(vals, {VALIDATOR})
            db.credit_staker_reward(STAKER_A, 999, 5, "h-other", 1, validator=OTHER)
            rows = db.staker_rewards(VALIDATOR, STAKER_A, 50)
            self.assertNotIn(999, [r["block"] for r in rows])
            self.assertIn(999, [r["block"] for r in db.staker_rewards(OTHER, STAKER_A, 50)])
            db.close()
        finally:
            fake.close()


class AddressingTest(unittest.TestCase):
    def test_stakers_url_uses_validator_not_reward_wallet(self):
        cfg = Config({"VALIDATOR_ADDR": VALIDATOR, "REWARD_ADDR": REWARD_WALLET,
                      "STAKERS_URL_TEMPLATE": "http://x/api/stakers/{address}"})
        self.assertIn(urllib.parse.quote(VALIDATOR), cfg.stakers_url())
        self.assertNotIn(urllib.parse.quote(REWARD_WALLET), cfg.stakers_url())

    def test_reward_wallet_is_never_the_credited_staker(self):
        tx = {"relatedAddresses": [REWARD_WALLET, STAKING_CONTRACT_ADDR, STAKER_B]}
        self.assertEqual(
            _staker_from_related(tx, VALIDATOR, STAKING_CONTRACT_ADDR, REWARD_WALLET),
            STAKER_B,
        )


class PartialStakerListTest(unittest.TestCase):
    def test_partial_staker_list_never_tombstones(self):
        fake = FakeServer()
        try:
            cfg, db, jobs = _jobs(fake, 100000000)   # record says 3 stakers
            fake.set_staged_stakers([
                [staker_row(STAKER_A, 40000000), staker_row(STAKER_B, 30000000),
                 staker_row(STAKER_C, 30000000)],
                [staker_row(STAKER_B, 30000000), staker_row(STAKER_C, 30000000)],
            ])
            jobs.run_snapshot()
            jobs.run_snapshot()
            latest = {r["address"]: r["balance_luna"] for r in db.latest_staker_rows(VALIDATOR)}
            self.assertEqual(latest[STAKER_A], 40000000)
            db.close()
        finally:
            fake.close()

    def test_complete_list_tombstones_departure(self):
        fake = FakeServer()
        try:
            cfg, db, jobs = _jobs(fake, 100000000)
            fake.set_staged_stakers([
                [staker_row(STAKER_A, 40000000), staker_row(STAKER_B, 30000000),
                 staker_row(STAKER_C, 30000000)],
                [staker_row(STAKER_B, 30000000), staker_row(STAKER_C, 30000000)],
            ])
            jobs.run_snapshot()
            fake.set_validator(60000000, 2)          # record follows the departure
            jobs.run_snapshot()
            latest = {r["address"]: r["balance_luna"] for r in db.latest_staker_rows(VALIDATOR)}
            self.assertEqual(latest[STAKER_A], 0)
            db.close()
        finally:
            fake.close()


class RetentionTest(unittest.TestCase):
    def test_downsample_keeps_one_per_hour_for_old_rows(self):
        db = DB(os.path.join(tempfile.mkdtemp(), "t.db"))
        hour = 3600000
        base = 100 * hour
        for m in range(180):                         # 3 hours of minute snapshots
            db.upsert_staker_snapshot(VALIDATOR, STAKER_A, base + m * 60000, m, 0)
        db.upsert_staker_snapshot(VALIDATOR, STAKER_A, base + 10 * hour, 1, 0)  # recent
        deleted = db.downsample_staker_snapshots(base + 5 * hour)
        self.assertEqual(deleted, 177)
        left = [r[0] for r in db.conn.execute(
            "SELECT fetched_at_ms FROM staker_snapshots ORDER BY 1")]
        self.assertEqual(left, [base, base + hour, base + 2 * hour, base + 10 * hour])
        db.close()

    def test_retention_off_by_default(self):
        self.assertEqual(Config({}).snapshot_retention_days, 0)


class ApiHardeningTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.cfg = Config({"VALIDATOR_ADDR": VALIDATOR, "DATA_DIR": self.dir, "PORT": "0"})
        self.db = DB(self.cfg.db_path())
        self.db.upsert_reward(VALIDATOR, 100, 2000000, 1000, "tx-cb")
        cid = self.db.create_cycle(VALIDATOR, 100, 1000, 100000000, 0)
        self.db.close_cycle(cid, 200, 2000, 2000000, 0, "VERIFIED")
        self.cid = cid
        for b in (300, 400, 500):
            self.db.credit_staker_reward(STAKER_A, b, 10, "h%d" % b, b, validator=VALIDATOR)
        self.server = serve(self.db, self.cfg, bind="127.0.0.1", port=0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.db.close()

    def _get(self, path):
        url = "http://127.0.0.1:%s%s" % (self.port, path)
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_unknown_cycle_is_404(self):
        status, body = self._get("/api/validators/%s/cycles/99999/shares"
                                 % urllib.parse.quote(VALIDATOR))
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    def test_foreign_validator_cycle_is_404(self):
        status, _ = self._get("/api/validators/%s/cycles/%d/shares"
                              % (urllib.parse.quote(OTHER), self.cid))
        self.assertEqual(status, 404)

    def test_query_values_are_decoded(self):
        status, body = self._get("/api/validators/%s/stakers/%s/rewards?limit=%%32"
                                 % (urllib.parse.quote(VALIDATOR), urllib.parse.quote(STAKER_A)))
        self.assertEqual(status, 200)
        self.assertEqual(len(body["rewards"]), 2)

    def test_500_hides_internal_detail(self):
        def boom(*a, **k):
            raise RuntimeError("secret path /data/tracker.db detail")
        self.db.recent_cycles_before = boom
        status, body = self._get("/api/verify")
        self.assertEqual(status, 500)
        self.assertEqual(body, {"error": "internal error"})

    def test_api_read_does_not_see_uncommitted_writer_txn(self):
        # The scheduler thread (here: the test thread) holds an open write
        # transaction. An API request on its own thread must neither see the
        # uncommitted row nor block on it.
        self.db.conn.execute(
            "INSERT INTO rewards (validator, block, amount_luna, ts_ms, tx_hash) "
            "VALUES (?, 101, 5, 1001, 'uncommitted')", (VALIDATOR,))
        status, body = self._get("/api/validators/%s/summary" % urllib.parse.quote(VALIDATOR))
        self.assertEqual(status, 200)
        self.assertEqual(body["rewards"]["count"], 1)
        self.db.conn.commit()
        status, body = self._get("/api/validators/%s/summary" % urllib.parse.quote(VALIDATOR))
        self.assertEqual(body["rewards"]["count"], 2)


if __name__ == "__main__":
    unittest.main()
