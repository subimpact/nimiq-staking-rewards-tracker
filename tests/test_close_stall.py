"""Regression tests for the 2026-09-18..09-30 close-stall incident.

Root cause: a staker that fully unstaked (balance 0 at the cycle open, then
dropped from the staker list) made Guard 1 block the cycle close forever - the
close silently re-PENDed every poll, so NO cycle closed Sep 18 to Sep 30 and
/verify froze on a Sep-18 PENDING cycle.

Pin three behaviors:
1. a departed zero-balance staker no longer blocks the close (excluded from
   attribution; the staked stakers still verify),
2. a staked staker with no post-open snapshot still blocks (hard Guard 1
   stays fail-closed),
3. the successor cycle chains from the last closed cycle's closed_block so a
   catch-up never skips the backlog of historic batches.
"""

import os
import unittest

from tracker.config import Config
from tracker.db import DB
from tracker.jobs import Jobs

from tests.fake_rpc import (
    FakeServer,
    STAKER_A,
    STAKER_B,
    STAKER_C,
    STAKER_Z,
    VALIDATOR,
    coinbase_tx,
    spread_staking_txs,
    staking_tx,
    staker_row,
)
from tracker.config import STAKING_CONTRACT_ADDR


class _F:
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

TOTAL_LUNA = 100000000
COINBASE_AMOUNT = 2000000
EXP_A = (40000000 * COINBASE_AMOUNT) // TOTAL_LUNA  # 800000
EXP_B = (30000000 * COINBASE_AMOUNT) // TOTAL_LUNA  # 600000
EXP_C = (30000000 * COINBASE_AMOUNT) // TOTAL_LUNA  # 600000
ZERO = 0


def build_config(fake):
    environ = {
        "RPC_URL": fake.url(),
        "STAKERS_URL_TEMPLATE": fake.url() + "/api/stakers/{address}",
        "VALIDATORS_URL_TEMPLATE": fake.url() + "/api/validators",
        "VALIDATOR_ADDR": VALIDATOR,
        "REWARD_ADDR": VALIDATOR,
        "MIN_TRIGGER_LUNA": "2000",
        "MIN_SHARE_LUNA": "500",
        "RESERVE_LUNA": "100000",
        "DATA_DIR": fake.data_dir,
        "PORT": "8649",
    }
    return Config(environ)


class CatchUpTest(unittest.TestCase):
    def _base(self):
        fake = FakeServer()
        fake.set_validator(TOTAL_LUNA, 3)
        cfg = build_config(fake)
        db = DB(os.path.join(fake.data_dir, "tracker.db"))
        counter = {"ms": 1_700_000_000_000}

        def now_ms():
            counter["ms"] += 1
            return counter["ms"]

        jobs = Jobs(db, cfg, fetcher=_F(cfg, fake), now_ms=now_ms)
        return fake, db, cfg, jobs

    def _pass(self, jobs):
        jobs.run_snapshot()
        jobs.run_ingest()
        jobs.run_cycle_close()
        jobs.run_verify()

    # ---- 1: departed zero-balance staker must not wedge closes ----------

    def test_departed_zero_staker_does_not_block_close(self):
        fake, db, cfg, jobs = self._base()
        try:
            # Open snapshot includes STAKER_Z at balance 0 (still listed at
            # open, mirrors Z having fully unstaked before the cycle opened).
            fake.set_staged_stakers([
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                    staker_row(STAKER_Z, ZERO),
                ],
                # Close snapshot: Z is GONE from the list (departed), the
                # others earned their shares.
                [
                    staker_row(STAKER_A, 40000000 + EXP_A),
                    staker_row(STAKER_B, 30000000 + EXP_B),
                    staker_row(STAKER_C, 30000000 + EXP_C),
                ],
            ])
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                spread_staking_txs([EXP_A, EXP_B, EXP_C], 100, gap=1),
            ])
            self._pass(jobs)
            self._pass(jobs)

            cycles = [c for c in db.recent_cycles(VALIDATOR, 10)
                      if c["status"] != "PENDING"]
            self.assertEqual(len(cycles), 1)
            cycle = cycles[0]
            self.assertEqual(cycle["status"], "VERIFIED")

            # Z holds no stake and expects no share: not in cycle_shares.
            shares = db.cycle_shares(cycle["id"])
            self.assertNotIn(
                STAKER_Z, {s["staker_address"] for s in shares}
            )
            by_addr = {s["staker_address"]: s for s in shares}
            self.assertEqual(by_addr[STAKER_A]["actual_luna"], EXP_A)
            self.assertEqual(by_addr[STAKER_B]["actual_luna"], EXP_B)
            self.assertEqual(by_addr[STAKER_C]["actual_luna"], EXP_C)
        finally:
            db.close()
            fake.close()

    # ---- 2: staked-staker departure resolves honestly (tombstone) -------

    def test_staked_staker_departure_tombstone_resolves_not_wedges(self):
        """The nonzero variant of the 2026-09-30 wedge: a staker with stake at
        open fully unstakes mid-window and drops off the staker list. The
        snapshot job's tombstone rows the departure to balance 0, so the
        window closes as an honest Guard-2 MISMATCH ("unstaked during
        cycle") instead of wedging every later cycle; and the departed
        staker is excluded from subsequent windows (failed pre-fix: Guard 1
        blocked the close forever)."""
        fake, db, cfg, jobs = self._base()
        try:
            # Stage 1: three staked stakers at open. Stage 2: A gone (fully
            # unstaked mid-window), B/C earned shares.
            fake.set_staged_stakers([
                [
                    staker_row(STAKER_A, 40000000),
                    staker_row(STAKER_B, 30000000),
                    staker_row(STAKER_C, 30000000),
                ],
                [
                    staker_row(STAKER_B, 30000000 + EXP_B),
                    staker_row(STAKER_C, 30000000 + EXP_C),
                ],
            ])
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                spread_staking_txs([EXP_A, EXP_B, EXP_C], 100, gap=1),
            ])
            self._pass(jobs)
            self._pass(jobs)

            cycles = [c for c in db.recent_cycles(VALIDATOR, 10)]
            self.assertEqual(len(closed_cycles(db)), 1)
            cycle = closed_cycles(db)[0]
            # Honest MISMATCH: the window cannot prove A was paid their
            # missing stake honestly (delta went negative; unaccounted).
            self.assertEqual(cycle["status"], "MISMATCH")

            shares = db.cycle_shares(cycle["id"])
            by_addr = {s["staker_address"]: s for s in shares}
            # A's tombstoned delta (0 - 40M) is Guard-2 flagged.
            self.assertIn(STAKER_A, by_addr)
            self.assertEqual(by_addr[STAKER_A]["ok"], 0)
            self.assertEqual(
                by_addr[STAKER_A]["reason"], "unstaked during cycle"
            )
            # B/C verified normally.
            self.assertEqual(by_addr[STAKER_B]["ok"], 1)
            self.assertEqual(by_addr[STAKER_C]["ok"], 1)
            self.assertEqual(by_addr[STAKER_B]["actual_luna"], EXP_B)
            self.assertEqual(by_addr[STAKER_C]["actual_luna"], EXP_C)
        finally:
            db.close()
            fake.close()

    # ---- 3: catch-up chains from the last closed block -------------------

    def test_backlog_drains_and_successor_chains_from_last_closed_block(self):
        """The 2026-09-30 catch-up scenario: a wedged close left many already-
        ingested batches unverified. One scheduler pass must drain the whole
        backlog (up to max_close_per_tick), and every successor cycle must
        open at the PREVIOUS cycle's closed_block - never jump to the latest
        ingested batch (which would swallow the backlog unverified)."""
        fake, db, cfg, jobs = self._base()
        try:
            V = VALIDATOR
            # Snapshot ladder seeded directly (fetched_at_ms controlled):
            # balances grow by one share set per batch, exactly like the
            # production minute-snapshots interleaving with payout batches.
            balances = [40000000, 30000000, 30000000]  # A, B, C base
            addrs = (STAKER_A, STAKER_B, STAKER_C)
            T = {
                "open":   1_700_000_000_000,
                "s1":     1_700_000_000_800,   # after batch 1 (txs at +500..502)
                "s2":     1_700_000_001_800,   # after batch 2 (txs at +1500..502)
                "s3":     1_700_000_002_800,   # after batch 3 (txs at +2500..502)
                "s4":     1_700_000_003_800,   # final
            }
            # Ladder row 0: base balances before any batch.
            for a, b in zip(addrs, balances):
                db.upsert_staker_snapshot(V, a, T["open"], b, 0)
            # Batches 1..3 at blocks 100, 200, 300 (last tx of each batch ends
            # at 102 / 202 / 302), each paying one EXP-share set.
            batch_blocks = [100, 200, 300]
            snap_after = [T["s1"], T["s2"], T["s3"]]
            ladder = [T["s4"]]  # final post-cycle-3 row: after batch 3 close
            for n, (start, snap_ts) in enumerate(zip(batch_blocks, snap_after)):
                run_start = start
                run_end = start + len(addrs) - 1
                # Income: one coinbase per batch window.
                db.upsert_reward(V, start - 5, COINBASE_AMOUNT,
                                 T["open"] + n * 1000, "cb-%d" % n)
                # Snapshot row AFTER this batch's payouts (balance grows).
                for a, b in zip(addrs, balances):
                    per = EXP_A if a == STAKER_A else (EXP_B if a == STAKER_B else EXP_C)
                    db.upsert_staker_snapshot(V, a, snap_ts,
                                              b + (n + 1) * per, 0)
                # The batch tx itself (per-staker amounts = EXP each).
                for i, a in enumerate(addrs):
                    db.upsert_restake(V, run_start + i, STAKING_CONTRACT_ADDR,
                                      [EXP_A, EXP_B, EXP_C][i],
                                      T["open"] + n * 1000 + 500 + i,
                                      "tx-%d-%d" % (start, i),
                                      staker_address=a)
            # Final ladder row (strictly after cycle-3's open/close window).
            for a, b in zip(addrs, balances):
                per = EXP_A if a == STAKER_A else (EXP_B if a == STAKER_B else EXP_C)
                db.upsert_staker_snapshot(V, a, ladder[0], b + 3 * per, 0)

            # One pass: the pre-seeded DB supplies everything; staged payloads
            # stay empty so the test exercises ONLY close/catch-up chaining.
            fake.set_staged_txs([[]] * 4)
            # Production state at fix-ship time: the wedged PENDING cycle
            # (1177 in prod) opened BEFORE every backlog batch.
            db.create_cycle(V, 0, T["open"], TOTAL_LUNA, 100000)
            self._pass(jobs)

            closed = closed_cycles(db)
            # Three batches -> three closed cycles, all in one pass.
            self.assertEqual(len(closed), 3)
            self.assertEqual(len(db.recent_cycles(V, 10)), 4)  # + 1 new open
            oldest_newest = closed[::-1]
            self.assertEqual(oldest_newest[0]["opened_block"], 0)
            self.assertEqual(oldest_newest[0]["closed_block"], 102)
            for prev, cur in zip(oldest_newest, oldest_newest[1:]):
                # THE chaining assertion: successor opens where predecessor
                # closed - never at max_restake_block (302-ish).
                self.assertEqual(cur["opened_block"], prev["closed_block"])
            self.assertEqual(oldest_newest[-1]["closed_block"], 302)
            self.assertEqual(len(db.recent_cycles(V, 10)), 4)  # 3 + 1 new open
            for c in oldest_newest:
                self.assertEqual(c["status"], "VERIFIED")
                self.assertEqual(c["available_luna"], COINBASE_AMOUNT)
                # Every cycle attributed its own full share set.
                by_addr = {s["staker_address"]: s
                           for s in db.cycle_shares(c["id"])}
                self.assertEqual(by_addr[STAKER_A]["actual_luna"], EXP_A)
                self.assertEqual(by_addr[STAKER_B]["actual_luna"], EXP_B)
                self.assertEqual(by_addr[STAKER_C]["actual_luna"], EXP_C)
        finally:
            db.close()
            fake.close()


def closed_cycles(db):
    return [c for c in db.recent_cycles(VALIDATOR, 10)
            if c["status"] != "PENDING"]


if __name__ == "__main__":
    unittest.main()