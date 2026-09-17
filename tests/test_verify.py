"""End-to-end tests for the verify (G2) job against a fake RPC server.

Covers a fully VERIFIED cycle with 3 stakers and a MISMATCH cycle, plus the
integer-luna expected-share math, dust floor and rounding tolerance from SPEC.
"""

import os
import tempfile
import unittest

from tracker.config import Config
from tracker.db import DB
from tracker.jobs import Jobs

from tests.fake_rpc import (
    FakeServer,
    STAKER_A,
    STAKER_B,
    STAKER_C,
    VALIDATOR,
    coinbase_tx,
    restake_tx,
    staker_row,
)

TOTAL_LUNA = 100000000      # 1000 NIM total validator stake
RESERVE = 100000            # RESERVE_LUNA default
COINBASE_AMOUNT = 2000000   # 20 NIM block reward
MIN_SHARE = 500             # MIN_SHARE_LUNA default


def build_config(fake):
    environ = {
        "RPC_URL": fake.url(),
        "STAKERS_URL_TEMPLATE": fake.url() + "/api/stakers/{address}",
        "VALIDATORS_URL_TEMPLATE": fake.url() + "/api/validators",
        "VALIDATOR_ADDR": VALIDATOR,
        "REWARD_ADDR": VALIDATOR,
        "MIN_TRIGGER_LUNA": "2000",
        "MIN_SHARE_LUNA": str(MIN_SHARE),
        "RESERVE_LUNA": str(RESERVE),
        "DATA_DIR": fake.data_dir,
        "PORT": "8649",
    }
    return Config(environ)


def base_env():
    fake = FakeServer()
    fake.set_stakers([
        staker_row(STAKER_A, 40000000),
        staker_row(STAKER_B, 30000000),
        staker_row(STAKER_C, 30000000),
    ])
    fake.set_validator(TOTAL_LUNA, 3)
    cfg = build_config(fake)
    db = DB(os.path.join(fake.data_dir, "tracker.db"))
    jobs = Jobs(db, cfg, fetcher=_FakeFetcher(cfg, fake))
    return fake, db, cfg, jobs


class _FakeFetcher:
    """Wires Jobs to the FakeServer's handlers without real networking."""

    def __init__(self, cfg, fake):
        self.cfg = cfg
        self.fake = fake

    def get_transactions(self, address):
        return self.fake.txs(address)

    def get_stakers(self):
        return self.fake.stakers_payload

    def get_validators(self):
        return self.fake.validators_payload


def run_two_pass(jobs):
    """Run all four jobs twice so a staged fixture's coinbase is followed by a
    closing restake batch."""
    for _ in range(2):
        jobs.run_snapshot()
        jobs.run_ingest()
        jobs.run_cycle_close()
        jobs.run_verify()


def run_pass(jobs):
    jobs.run_snapshot()
    jobs.run_ingest()
    jobs.run_cycle_close()
    jobs.run_verify()


def verified_fixture_txs(c_actual=None):
    """Staged fixture: pass 1 earns a coinbase, pass 2 adds the closing
    restake batch that distributes the previous window's reward."""
    if c_actual is None:
        c_actual = 570000
    return [
        [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
        [
            restake_tx(100, STAKER_A, 760000, "tx-a"),
            restake_tx(100, STAKER_B, 570000, "tx-b"),
            restake_tx(100, STAKER_C, c_actual, "tx-c"),
        ],
    ]


class VerifyTest(unittest.TestCase):
    def test_verified_cycle_three_stakers(self):
        fake, db, cfg, jobs = base_env()
        try:
            fake.set_staged_txs(verified_fixture_txs())
            run_two_pass(jobs)

            cycles = db.recent_cycles(VALIDATOR, 10)
            closed = [c for c in cycles if c["status"] != "PENDING"]
            self.assertEqual(len(closed), 1)
            cycle = closed[0]
            self.assertEqual(cycle["status"], "VERIFIED")
            self.assertEqual(cycle["available_luna"], COINBASE_AMOUNT - RESERVE)

            shares = db.cycle_shares(cycle["id"])
            self.assertEqual(len(shares), 3)
            by_addr = {s["staker_address"]: s for s in shares}

            self.assertEqual(by_addr[STAKER_A]["expected_luna"], 760000)
            self.assertEqual(by_addr[STAKER_A]["actual_luna"], 760000)
            self.assertEqual(by_addr[STAKER_A]["ok"], 1)

            self.assertEqual(by_addr[STAKER_B]["expected_luna"], 570000)
            self.assertEqual(by_addr[STAKER_B]["ok"], 1)
            self.assertEqual(by_addr[STAKER_C]["expected_luna"], 570000)
            self.assertEqual(by_addr[STAKER_C]["ok"], 1)

            # Sum of expected equals available exactly (no dust here).
            total_expected = sum(
                s["expected_luna"] for s in shares
            )
            self.assertEqual(total_expected, cycle["available_luna"])

            # G1 ledger populated for verified stakers.
            for addr in (STAKER_A, STAKER_B, STAKER_C):
                rewards = db.staker_rewards(VALIDATOR, addr, 50)
                self.assertEqual(len(rewards), 1)
        finally:
            db.close()
            fake.close()

    def test_mismatch_cycle(self):
        fake, db, cfg, jobs = base_env()
        try:
            # C's restake is short by 5000 lunas -> mismatch on C only.
            fake.set_staged_txs(verified_fixture_txs(c_actual=565000))
            run_two_pass(jobs)

            cycles = db.recent_cycles(VALIDATOR, 10)
            closed = [c for c in cycles if c["status"] != "PENDING"]
            cycle = closed[0]
            self.assertEqual(cycle["status"], "MISMATCH")

            shares = db.cycle_shares(cycle["id"])
            by_addr = {s["staker_address"]: s for s in shares}
            self.assertEqual(by_addr[STAKER_A]["ok"], 1)
            self.assertEqual(by_addr[STAKER_B]["ok"], 1)
            self.assertEqual(by_addr[STAKER_C]["ok"], 0)
            self.assertEqual(by_addr[STAKER_C]["expected_luna"], 570000)
            self.assertEqual(by_addr[STAKER_C]["actual_luna"], 565000)

            # Mismatched staker must NOT be credited to the G1 ledger.
            self.assertEqual(len(db.staker_rewards(VALIDATOR, STAKER_C, 50)), 0)
            self.assertEqual(len(db.staker_rewards(VALIDATOR, STAKER_A, 50)), 1)
        finally:
            db.close()
            fake.close()

    def test_expected_math_is_floor_integer(self):
        fake, db, cfg, jobs = base_env()
        try:
            # Balance that produces a fractional expected share to prove floor.
            fake.set_stakers([
                staker_row(STAKER_A, 99999999),
                staker_row(STAKER_B, 1),
            ])
            fake.set_validator(100000000, 2)
            # available = 2000000 - 100000 = 1900000
            # A expected = floor(99999999*1900000/100000000)
            expected_a = (99999999 * 1900000) // 100000000
            expected_b = (1 * 1900000) // 100000000
            self.assertEqual(expected_a, 1899999)
            self.assertEqual(expected_b, 0)

            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                [
                    restake_tx(100, STAKER_A, expected_a, "tx-a"),
                    restake_tx(100, STAKER_B, 0, "tx-b"),
                ],
            ])
            run_two_pass(jobs)

            cycles = [c for c in db.recent_cycles(VALIDATOR, 10)
                      if c["status"] != "PENDING"]
            cycle = cycles[0]
            shares = db.cycle_shares(cycle["id"])
            by_addr = {s["staker_address"]: s for s in shares}
            self.assertEqual(by_addr[STAKER_A]["expected_luna"], expected_a)
            # B's expected (0) is below MIN_SHARE and actual is 0 -> dust ok.
            self.assertEqual(by_addr[STAKER_B]["expected_luna"], 0)
            self.assertEqual(by_addr[STAKER_B]["actual_luna"], 0)
            self.assertEqual(by_addr[STAKER_B]["ok"], 1)
            self.assertEqual(cycle["status"], "VERIFIED")
        finally:
            db.close()
            fake.close()

    def test_rounding_tolerance_plus_minus_one(self):
        fake, db, cfg, jobs = base_env()
        try:
            # actual == expected + 1 is still OK (rounding tolerance).
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                [
                    restake_tx(100, STAKER_A, 760001, "tx-a"),   # +1
                    restake_tx(100, STAKER_B, 569999, "tx-b"),   # -1
                    restake_tx(100, STAKER_C, 570000, "tx-c"),
                ],
            ])
            run_two_pass(jobs)
            cycles = [c for c in db.recent_cycles(VALIDATOR, 10)
                      if c["status"] != "PENDING"]
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "VERIFIED")
            shares = db.cycle_shares(cycle["id"])
            self.assertTrue(all(s["ok"] == 1 for s in shares))
        finally:
            db.close()
            fake.close()


if __name__ == "__main__":
    unittest.main()
