"""End-to-end tests for the verify (G2) job against a fake RPC server.

Covers a fully VERIFIED cycle with 3 stakers, a MISMATCH cycle, the integer-luna
expected-share math, dust floor, rounding tolerance, the insufficient-snapshot
PENDING guard, and the unaccounted-restake-value consistency check.

Credit attribution is Option A: a staker's actual credit is the active-balance
delta between the snapshot at/before the window opened and the first snapshot
strictly after the window opened.
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
    STAKER_D,
    VALIDATOR,
    coinbase_tx,
    spread_staking_txs,
    staking_tx,
    staker_row,
)

TOTAL_LUNA = 100000000      # 1000 NIM total validator stake
RESERVE = 100000            # RESERVE_LUNA default (wallet floor, NOT subtracted)
COINBASE_AMOUNT = 2000000   # 20 NIM block reward
MIN_SHARE = 500             # MIN_SHARE_LUNA default
DUST_TOLERANCE = 500        # DUST_TOLERANCE_LUNA default
AVAILABLE = COINBASE_AMOUNT  # full coinbase sum; no reserve carve

# Expected shares for open balances A=40M, B=30M, C=30M.
EXP_A = (40000000 * AVAILABLE) // TOTAL_LUNA  # 800000
EXP_B = (30000000 * AVAILABLE) // TOTAL_LUNA  # 600000
EXP_C = (30000000 * AVAILABLE) // TOTAL_LUNA  # 600000


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
    fake.set_validator(TOTAL_LUNA, 3)
    cfg = build_config(fake)
    db = DB(os.path.join(fake.data_dir, "tracker.db"))
    # Monotonic clock: each timestamp is strictly increasing, so the
    # strict-after snapshot lookup can never miss due to same-ms writes.
    counter = {"ms": 1_700_000_000_000}

    def now_ms():
        counter["ms"] += 1
        return counter["ms"]

    jobs = Jobs(db, cfg, fetcher=_FakeFetcher(cfg, fake), now_ms=now_ms)
    return fake, db, cfg, jobs


class _FakeFetcher:
    """Wires Jobs to the FakeServer's handlers without real networking."""

    def __init__(self, cfg, fake):
        self.cfg = cfg
        self.fake = fake

    def get_transactions(self, address):
        return self.fake.txs(address)

    def get_stakers(self):
        return self.fake.stakers_payload_now()

    def get_validators(self):
        return self.fake.validators_payload_now()


def run_pass(jobs):
    jobs.run_snapshot()
    jobs.run_ingest()
    jobs.run_cycle_close()
    jobs.run_verify()


def run_two_pass(jobs):
    """Pass 1 snapshots the open balances + earns a coinbase; pass 2 snapshots
    the close balances + adds the closing restake batch."""
    run_pass(jobs)
    run_pass(jobs)


def stage_open_close(fake, open_rows, close_rows):
    fake.set_staged_stakers([open_rows, close_rows])


def closed_cycle(db):
    return [c for c in db.recent_cycles(VALIDATOR, 10)
            if c["status"] != "PENDING"]


def closing_restakes(amounts):
    """Closing restake batch: all AddStake txs to the staking contract."""
    return [staking_tx(100, v, "tx-%d" % i) for i, v in enumerate(amounts)]


class VerifyTest(unittest.TestCase):
    def test_verified_cycle_three_stakers(self):
        fake, db, cfg, jobs = base_env()
        try:
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_A, 40000000 + EXP_A),
                    staker_row(STAKER_B, 30000000 + EXP_B),
                    staker_row(STAKER_C, 30000000 + EXP_C),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([EXP_A, EXP_B, EXP_C]),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 1)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "VERIFIED")
            self.assertEqual(cycle["available_luna"], AVAILABLE)

            shares = db.cycle_shares(cycle["id"])
            self.assertEqual(len(shares), 3)
            by_addr = {s["staker_address"]: s for s in shares}

            self.assertEqual(by_addr[STAKER_A]["expected_luna"], EXP_A)
            self.assertEqual(by_addr[STAKER_A]["actual_luna"], EXP_A)
            self.assertEqual(by_addr[STAKER_A]["ok"], 1)
            self.assertEqual(by_addr[STAKER_B]["actual_luna"], EXP_B)
            self.assertEqual(by_addr[STAKER_B]["ok"], 1)
            self.assertEqual(by_addr[STAKER_C]["actual_luna"], EXP_C)
            self.assertEqual(by_addr[STAKER_C]["ok"], 1)
            self.assertEqual(sum(s["expected_luna"] for s in shares), AVAILABLE)

            # G1 ledger populated for verified stakers, credited with the
            # closing batch's last tx hash.
            boundary = db.next_restake_batch(VALIDATOR, 0, cfg.batch_gap_blocks)
            batch_hash = boundary[1][-1]["tx_hash"]
            for addr in (STAKER_A, STAKER_B, STAKER_C):
                rewards = db.staker_rewards(VALIDATOR, addr, 50)
                self.assertEqual(len(rewards), 1)
                self.assertEqual(rewards[0]["tx_hash"], batch_hash)
                self.assertEqual(rewards[0]["block"], cycle["closed_block"])
        finally:
            db.close()
            fake.close()

    def test_mismatch_cycle(self):
        fake, db, cfg, jobs = base_env()
        try:
            # C's close balance exceeds expected by 5000 -> ok=False.
            c_close = 30000000 + EXP_C + 5000
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_A, 40000000 + EXP_A),
                    staker_row(STAKER_B, 30000000 + EXP_B),
                    staker_row(STAKER_C, c_close),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([EXP_A, EXP_B, EXP_C + 5000]),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "MISMATCH")

            by_addr = {s["staker_address"]: s
                       for s in db.cycle_shares(cycle["id"])}
            self.assertEqual(by_addr[STAKER_A]["ok"], 1)
            self.assertEqual(by_addr[STAKER_B]["ok"], 1)
            self.assertEqual(by_addr[STAKER_C]["ok"], 0)
            self.assertEqual(by_addr[STAKER_C]["actual_luna"], EXP_C + 5000)
            self.assertIn("credit exceeds expected share",
                          by_addr[STAKER_C]["reason"])

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
            fake.set_validator(100000000, 2)
            expected_a = (99999999 * AVAILABLE) // 100000000
            expected_b = (1 * AVAILABLE) // 100000000
            self.assertEqual(expected_a, 1999999)
            self.assertEqual(expected_b, 0)

            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 99999999),
                    staker_row(STAKER_B, 1),
                ],
                [
                    staker_row(STAKER_A, 99999999 + expected_a),
                    staker_row(STAKER_B, 1 + expected_b),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([expected_a, expected_b]),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            cycle = cycles[0]
            by_addr = {s["staker_address"]: s
                       for s in db.cycle_shares(cycle["id"])}
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
            # actual == expected +- 1 is still OK (rounding tolerance).
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_A, 40000000 + EXP_A + 1),
                    staker_row(STAKER_B, 30000000 + EXP_B - 1),
                    staker_row(STAKER_C, 30000000 + EXP_C),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([EXP_A + 1, EXP_B - 1, EXP_C]),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "VERIFIED")
            shares = db.cycle_shares(cycle["id"])
            self.assertTrue(all(s["ok"] == 1 for s in shares))
        finally:
            db.close()
            fake.close()

    def test_insufficient_snapshots_stays_pending(self):
        fake, db, cfg, jobs = base_env()
        try:
            # Only open balances staged: no snapshot strictly after the window
            # opened -> the cycle must remain PENDING.
            fake.set_staged_stakers([
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                ],
            ])
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([EXP_A, EXP_B]),
            ])
            run_two_pass(jobs)

            cycles = db.recent_cycles(VALIDATOR, 10)
            self.assertTrue(cycles)
            self.assertEqual(len(closed_cycle(db)), 0)
            self.assertTrue(all(c["status"] == "PENDING" for c in cycles))
        finally:
            db.close()
            fake.close()

    def test_unaccounted_restake_value(self):
        fake, db, cfg, jobs = base_env()
        try:
            # All stakers match expected, but the on-chain restake value exceeds
            # the attributed deltas by > 1 luna -> MISMATCH via drift.
            extra = 10000
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_A, 40000000 + EXP_A),
                    staker_row(STAKER_B, 30000000 + EXP_B),
                    staker_row(STAKER_C, 30000000 + EXP_C),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([EXP_A, EXP_B, EXP_C, extra]),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "MISMATCH")
            # Per-staker all ok; the drift surfaces on the shares' reason.
            shares = db.cycle_shares(cycle["id"])
            self.assertTrue(all(s["ok"] == 1 for s in shares))
            self.assertTrue(all(
                s["reason"] == "unaccounted restake value (possible timing drift)"
                for s in shares
            ))
        finally:
            db.close()
            fake.close()


    def test_skim_detected(self):
        # The bot distributes only 1,700,000 of a 2,000,000 coinbase, but the
        # batch deltas match that batch proportionally (all stakers internally
        # consistent). Guard 4a (drift) passes; guard 4b (honesty vs the cycle's
        # reward income) fires -> MISMATCH with the skim reason on every share.
        fake, db, cfg, jobs = base_env()
        try:
            skim_batch_total = 1700000
            s_a = (40000000 * skim_batch_total) // TOTAL_LUNA
            s_b = (30000000 * skim_batch_total) // TOTAL_LUNA
            s_c = (30000000 * skim_batch_total) // TOTAL_LUNA
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_A, 40000000 + s_a),
                    staker_row(STAKER_B, 30000000 + s_b),
                    staker_row(STAKER_C, 30000000 + s_c),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([s_a, s_b, s_c]),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "MISMATCH")
            shares = db.cycle_shares(cycle["id"])
            self.assertTrue(all(
                s["reason"]
                == "distribution does not match cycle rewards (possible skim)"
                for s in shares
            ))
        finally:
            db.close()
            fake.close()

    def test_dust_carry_ok(self):
        # The batch carries 33 lunas of dust beyond the coinbase (2,000,000 +
        # 33 = 2,000,033), well inside DUST_TOLERANCE_LUNA (500). A/B/C get
        # exactly their expected share of the coinbase, and the 33 lunas of
        # swept dust goes to a new mid-cycle staker that lands in unaccounted.
        # Guard 4a (internal) and 4b (honesty vs the coinbase) both pass and no
        # staker over-credits -> VERIFIED.
        fake, db, cfg, jobs = base_env()
        try:
            dust = 33
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_A, 40000000 + EXP_A),
                    staker_row(STAKER_B, 30000000 + EXP_B),
                    staker_row(STAKER_C, 30000000 + EXP_C),
                    staker_row(STAKER_D, dust),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes([EXP_A, EXP_B, EXP_C, dust]),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "VERIFIED")
            shares = db.cycle_shares(cycle["id"])
            self.assertTrue(all(s["ok"] == 1 for s in shares))
        finally:
            db.close()
            fake.close()

    def test_batch_aware_close_verified(self):
        # Regression for the ImpactZero cycle-60 bug. The bot sends a burst of
        # AddStake txs (blocks 100, 101, 102) inside one distribution event. The
        # old code closed on the FIRST restake (block 100), capturing a partial
        # batch -> deltas exceed expected + unaccounted -> false MISMATCH and an
        # empty G1 ledger. Batch-aware close must end at block 102.
        fake, db, cfg, jobs = base_env()
        try:
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_A, 40000000 + EXP_A),
                    staker_row(STAKER_B, 30000000 + EXP_B),
                    staker_row(STAKER_C, 30000000 + EXP_C),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                spread_staking_txs([EXP_A, EXP_B, EXP_C], 100, gap=1),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "VERIFIED")
            # The batch ends at block 102 (last of the 3-tx burst), not 100.
            self.assertEqual(cycle["closed_block"], 102)

            shares = db.cycle_shares(cycle["id"])
            by_addr = {s["staker_address"]: s for s in shares}
            self.assertEqual(by_addr[STAKER_A]["actual_luna"], EXP_A)
            self.assertEqual(by_addr[STAKER_B]["actual_luna"], EXP_B)
            self.assertEqual(by_addr[STAKER_C]["actual_luna"], EXP_C)
            self.assertTrue(all(s["ok"] == 1 for s in shares))

            # The batch's LAST tx hash (block 102) stands in for the credits.
            boundary = db.next_restake_batch(VALIDATOR, 0, cfg.batch_gap_blocks)
            last_hash = boundary[1][-1]["tx_hash"]
            self.assertIn("102", last_hash)
            for addr in (STAKER_A, STAKER_B, STAKER_C):
                rewards = db.staker_rewards(VALIDATOR, addr, 50)
                self.assertEqual(len(rewards), 1)
                self.assertEqual(rewards[0]["tx_hash"], last_hash)
                self.assertEqual(rewards[0]["block"], 102)
        finally:
            db.close()
            fake.close()

    def test_batch_gap_splits_batches(self):
        # Two bursts separated by more than BATCH_GAP_BLOCKS form two separate
        # cycles, each closing on its own batch end.
        fake, db, cfg, jobs = base_env()
        try:
            # Cycle 1: open base balances, close with batch ending at block 102.
            # Cycle 2: opened on cycle 1's close, paid from a second burst
            # (blocks 200-202) well past the 10-block gap.
            close1_a = 40000000 + EXP_A
            close1_b = 30000000 + EXP_B
            close1_c = 30000000 + EXP_C
            total2 = close1_a + close1_b + close1_c  # validator total grew
            # Cycle 2 expected shares are recomputed from the grown balances and
            # the grown validator total.
            exp2_a = (close1_a * AVAILABLE) // total2
            exp2_b = (close1_b * AVAILABLE) // total2
            exp2_c = (close1_c * AVAILABLE) // total2
            fake.set_staged_stakers([
                # snapshot 1: cycle 1 open
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                # snapshot 2: cycle 1 close == cycle 2 open
                [
                    staker_row(STAKER_A, close1_a),
                    staker_row(STAKER_B, close1_b),
                    staker_row(STAKER_C, close1_c),
                ],
                # snapshot 3: cycle 2 close (strictly after cycle 2 opened)
                [
                    staker_row(STAKER_A, close1_a + exp2_a),
                    staker_row(STAKER_B, close1_b + exp2_b),
                    staker_row(STAKER_C, close1_c + exp2_c),
                ],
            ])
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                # first distribution event (closes cycle 1 at block 102)
                spread_staking_txs([EXP_A, EXP_B, EXP_C], 100, gap=1),
                # second distribution event: new coinbase + a fresh burst far
                # past the 10-block gap (closes cycle 2 at block 202)
                [coinbase_tx(150, COINBASE_AMOUNT, "tx-cb-1")]
                + spread_staking_txs([exp2_a, exp2_b, exp2_c], 200, gap=1),
            ])
            fake.set_staged_validators([
                {"data": [{"total": TOTAL_LUNA, "numStakers": 3}]},
                {"data": [{"total": total2, "numStakers": 3}]},
                {"data": [{"total": total2, "numStakers": 3}]},
            ])
            # Pass 1 opens cycle 1; pass 2 closes it with the first burst and
            # opens cycle 2; pass 3 closes cycle 2 with the second burst.
            run_pass(jobs)
            run_pass(jobs)
            run_pass(jobs)

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 2)
            # Oldest first.
            c1, c2 = cycles[::-1]
            self.assertEqual(c1["closed_block"], 102)
            self.assertEqual(c2["closed_block"], 202)
            self.assertEqual(c1["status"], "VERIFIED")
            self.assertEqual(c2["status"], "VERIFIED")

            # Each cycle attributed its own full batch.
            c1_by = {s["staker_address"]: s for s in db.cycle_shares(c1["id"])}
            c2_by = {s["staker_address"]: s for s in db.cycle_shares(c2["id"])}
            self.assertEqual(c1_by[STAKER_A]["actual_luna"], EXP_A)
            self.assertEqual(c1_by[STAKER_B]["actual_luna"], EXP_B)
            self.assertEqual(c1_by[STAKER_C]["actual_luna"], EXP_C)
            self.assertEqual(c2_by[STAKER_A]["actual_luna"], exp2_a)
            self.assertEqual(c2_by[STAKER_B]["actual_luna"], exp2_b)
            self.assertEqual(c2_by[STAKER_C]["actual_luna"], exp2_c)
        finally:
            db.close()
            fake.close()


if __name__ == "__main__":
    unittest.main()
