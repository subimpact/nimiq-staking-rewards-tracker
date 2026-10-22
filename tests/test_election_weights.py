"""Optional election weighting (ELECTION_FROM_EPOCH, payout basis "Option 1").

When enabled for an epoch, each staker's weight is
min(epoch-start stake, live balance at cycle open): a mid-epoch joiner earns
0 until the next epoch, a mid-epoch top-up only earns on its epoch-start
balance, and a deactivation still stops earning at once (open balance 0 ->
weight 0). Epoch-start data comes from the snapshot ladder near the epoch's
first reward/restake timestamp; when unavailable the cycle is SKIPPED.

DB-level harness (see test_carry.py), with realistic blocks around
64.9M (epoch 1423: blocks 64,886,401..64,929,600) and chain-timed
snapshots: 1 block = 1 s, so the payout sits ~15.7 min after the epoch
anchor and its own snapshots fall OUTSIDE the 15-min epoch-start window.
"""

import os
import tempfile
import unittest

from tracker.config import Config, STAKING_CONTRACT_ADDR
from tracker.db import DB
from tracker.jobs import Jobs

from tests.fake_rpc import STAKER_A, STAKER_B, STAKER_C, VALIDATOR

COINBASE = 1188480                    # a live-sized coinbase (~11.9 NIM)
BAL = {STAKER_A: 55000000000, STAKER_B: 30000000000, STAKER_C: 15000000000}

# The epoch the tests live in (block ~64.9M -> epoch 1423).
EPOCH = 1423
EPOCH_FIRST_BLOCK = (EPOCH + 79) * 43200 + 1        # 64,886,401
NEXT_EPOCH_FIRST_BLOCK = (EPOCH + 80) * 43200 + 1   # 64,929,601

# Epoch anchor as the verifier computes it: the ts of the earliest reward in
# the epoch, here the first coinbase one batch (60 blocks) after the seed close
# at the epoch's first block (1 block = 1 s of chain time).
EPOCH_ANCHOR_MS = (EPOCH_FIRST_BLOCK + 60) * 1000
# The bot takes its epoch-start snapshot seconds after the anchor.
EPOCH_LADDER_MS = EPOCH_ANCHOR_MS + 1000
# The payout batch sits ~940 s (15.7 min) after the anchor: its own "open"
# snapshots (anchor + 939 s) fall OUTSIDE the default 15-min election window.
PAYOUT_FIRST_BLOCK = EPOCH_FIRST_BLOCK + 1000


def split(amount, weights):
    """Floor-split `amount` over the weight map; zero-weight payees omitted."""
    s = sum(weights.values())
    out = {}
    for a, w in sorted(weights.items()):
        v = (w * amount) // s
        if v > 0:
            out[a] = v
    return out


class ElectionHarness:
    """One cycle window inside epoch 1423, with explicit snapshot times."""

    def __init__(self, **env):
        self.dir = tempfile.mkdtemp()
        base = {"VALIDATOR_ADDR": VALIDATOR, "REWARD_ADDR": VALIDATOR,
                "DATA_DIR": self.dir}
        base.update({k: str(v) for k, v in env.items()})
        self.cfg = Config(base)
        self.db = DB(os.path.join(self.dir, "tracker.db"))
        self.ms = {"v": 1_800_000_000_000}

        def now_ms():
            self.ms["v"] += 1
            return self.ms["v"]

        self.jobs = Jobs(self.db, self.cfg, fetcher=object(), now_ms=now_ms)
        self.block = EPOCH_FIRST_BLOCK
        self.n = 0
        self.nwin = 0
        # A predecessor cycle closed AT the epoch's first block, carrying
        # nothing: the carry chain starts at 0.
        cid = self.db.create_cycle(VALIDATOR, self.block - 100, 1,
                                   sum(BAL.values()), 0)
        self.db.close_cycle(cid, self.block, 2, 0, 0, "VERIFIED",
                            attribution="tx", carry_luna=0, fee_carry_luna=0)

    def ladder(self, balances, t=None):
        """The epoch-start snapshot ladder. Default: 1 s after the epoch
        anchor (inside the 15-min election window)."""
        if t is None:
            t = EPOCH_LADDER_MS
        for a, b in balances.items():
            self.db.upsert_staker_snapshot(VALIDATOR, a, t, b, 0)

    def open_snapshot(self, addr, bal):
        """A live snapshot just before the payout (cycle-open balance)."""
        self.db.upsert_staker_snapshot(
            VALIDATOR, addr, PAYOUT_FIRST_BLOCK * 1000 - 1000, bal, 0)

    def window(self, incomes, payouts):
        """Coinbases (every 60 blocks from the previous close) then one payout
        batch ~1000 blocks into the epoch (later windows sit further in)."""
        if self.block < EPOCH_FIRST_BLOCK:
            self.block = EPOCH_FIRST_BLOCK
        for amt in incomes:
            self.block += 60
            self.db.upsert_reward(VALIDATOR, self.block, amt, self.block * 1000,
                                  "cb-%d" % self.block)
        self.block = max(self.block + 10,
                         PAYOUT_FIRST_BLOCK + self.nwin * 1600)
        for i, (staker, amt) in enumerate(sorted(payouts.items())):
            self.n += 1
            self.db.upsert_restake(
                VALIDATOR, self.block + i, STAKING_CONTRACT_ADDR, amt,
                (self.block + i) * 1000, "tx-%d" % self.n, staker)
        self.nwin += 1

    def close_all(self):
        """Trailing batch far ahead makes the last real batch complete."""
        self.n += 1
        tail = self.block + 500
        self.db.upsert_restake(VALIDATOR, tail, STAKING_CONTRACT_ADDR, 1,
                               tail * 1000, "tail-%d" % self.n, STAKER_A)
        self.block = tail
        if self.db.latest_pending_cycle(VALIDATOR) is None:
            self.jobs._open_pending_cycle()
        self.jobs.run_cycle_close()
        return [c for c in reversed(self.db.recent_cycles(VALIDATOR, 500))
                if c["status"] != "PENDING" and
                c["closed_block"] > EPOCH_FIRST_BLOCK]

    def shares(self, cycle):
        return {s["staker_address"]: s for s in self.db.cycle_shares(cycle["id"])}


class ElectionTest(unittest.TestCase):
    def tearDown(self):
        if getattr(self, "h", None) is not None:
            self.h.db.close()

    # a -------------------------------------------------------------------

    def test_joiner_paid_zero_under_election(self):
        # B joins mid-epoch: absent from the epoch-start ladder, big live
        # balance at open. Election pays B 0; A and C by epoch-start stake.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH,
                                     ELECTION_SNAPSHOT_WINDOW_MS=900000)
        h.ladder({STAKER_A: BAL[STAKER_A], STAKER_C: BAL[STAKER_C]})
        h.open_snapshot(STAKER_B, 60000000000)
        pot = split(COINBASE, {STAKER_A: 55000000000, STAKER_C: 15000000000})
        h.window([COINBASE], pot)
        cyc = h.close_all()[0]
        self.assertEqual(cyc["status"], "VERIFIED")
        sh = h.shares(cyc)
        self.assertEqual(sh[STAKER_B]["actual_luna"], 0)
        self.assertEqual(sh[STAKER_B]["expected_luna"], 0)
        for a in (STAKER_A, STAKER_C):
            self.assertEqual(sh[a]["ok"], 1)

    def test_same_data_without_election_is_mismatch(self):
        # The switch matters: with ELECTION_FROM_EPOCH=0 the same window
        # weights B by its big open balance and fails "paid less".
        h = self.h = ElectionHarness()
        h.ladder({STAKER_A: BAL[STAKER_A], STAKER_C: BAL[STAKER_C]})
        h.open_snapshot(STAKER_B, 60000000000)
        pot = split(COINBASE, {STAKER_A: 55000000000, STAKER_C: 15000000000})
        h.window([COINBASE], pot)
        cyc = h.close_all()[0]
        self.assertEqual(cyc["status"], "MISMATCH")
        sh = h.shares(cyc)
        self.assertEqual(sh[STAKER_B]["ok"], 0)
        self.assertEqual(sh[STAKER_B]["reason"],
                         "paid less than stake share of this payout")

    # b -------------------------------------------------------------------

    def test_topup_earns_only_on_epoch_start_balance(self):
        # A had 55e9 at epoch start and 110e9 at open: election weights A at
        # its epoch-start balance.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.ladder(BAL)
        h.open_snapshot(STAKER_A, 2 * BAL[STAKER_A])
        pot = split(COINBASE, BAL)
        h.window([COINBASE], pot)
        self.assertEqual(h.close_all()[0]["status"], "VERIFIED")

    def test_topup_split_by_open_balance_fails(self):
        # A split that treats A's top-up as earning weight is a mismatch:
        # A is overpaid relative to its epoch-start weight.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.ladder(BAL)
        h.open_snapshot(STAKER_A, 2 * BAL[STAKER_A])
        pot = split(COINBASE, {STAKER_A: 2 * BAL[STAKER_A],
                               STAKER_B: BAL[STAKER_B],
                               STAKER_C: BAL[STAKER_C]})
        h.window([COINBASE], pot)
        self.assertEqual(h.close_all()[0]["status"], "MISMATCH")

    # c -------------------------------------------------------------------

    def test_epoch_below_cutoff_keeps_open_balance_weights(self):
        # ELECTION_FROM_EPOCH=1424: this cycle's epoch (1423) is below the
        # cutoff, so the joiner is paid by open balance like before M2.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH + 1)
        h.ladder({STAKER_A: BAL[STAKER_A], STAKER_C: BAL[STAKER_C]})
        h.open_snapshot(STAKER_B, 60000000000)
        open_w = {STAKER_A: BAL[STAKER_A], STAKER_B: 60000000000,
                  STAKER_C: BAL[STAKER_C]}
        h.window([COINBASE], split(COINBASE, open_w))
        self.assertEqual(h.close_all()[0]["status"], "VERIFIED")

    # d -------------------------------------------------------------------

    def test_fallback_to_ladder_before_epoch_start(self):
        # No snapshots after the epoch anchor inside the window, but the
        # ladder polled 1 s BEFORE the anchor exists: use its last state.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.ladder({STAKER_A: BAL[STAKER_A], STAKER_C: BAL[STAKER_C]},
                 t=EPOCH_ANCHOR_MS - 1000)
        h.open_snapshot(STAKER_B, 60000000000)
        pot = split(COINBASE, {STAKER_A: 55000000000, STAKER_C: 15000000000})
        h.window([COINBASE], pot)
        cyc = h.close_all()[0]
        self.assertEqual(cyc["status"], "VERIFIED")
        sh = h.shares(cyc)
        self.assertEqual(sh[STAKER_B]["actual_luna"], 0)

    def test_no_snapshot_near_epoch_start_skips(self):
        # No ladder rows in either window: never guess with open balances.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.ladder({STAKER_A: BAL[STAKER_A], STAKER_C: BAL[STAKER_C]},
                 t=EPOCH_ANCHOR_MS - 2000000)   # far outside both windows
        h.open_snapshot(STAKER_A, BAL[STAKER_A])
        h.open_snapshot(STAKER_B, 60000000000)
        h.open_snapshot(STAKER_C, BAL[STAKER_C])
        pot = split(COINBASE, {STAKER_A: 55000000000, STAKER_C: 15000000000})
        h.window([COINBASE], pot)
        cyc = h.close_all()[0]
        self.assertEqual(cyc["status"], "SKIPPED")
        self.assertEqual(
            cyc["note"],
            "election weights unavailable (no staker snapshot near epoch start)")

    # e -------------------------------------------------------------------

    def test_deactivation_mid_epoch_earns_zero(self):
        # C deactivates mid-epoch: open balance 0 -> weight 0 even though its
        # epoch-start stake was 15e9; A and B split the whole payout.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.ladder(BAL)
        h.open_snapshot(STAKER_C, 0)
        # a close-side snapshot confirms C stayed at 0 (no unstake reason)
        h.db.upsert_staker_snapshot(
            VALIDATOR, STAKER_C, PAYOUT_FIRST_BLOCK * 1000 + 1000, 0, 0)
        pot = split(COINBASE, {STAKER_A: BAL[STAKER_A], STAKER_B: BAL[STAKER_B]})
        h.window([COINBASE], pot)
        cyc = h.close_all()[0]
        self.assertEqual(cyc["status"], "VERIFIED")
        sh = h.shares(cyc)
        self.assertEqual(sh[STAKER_C]["actual_luna"], 0)
        for a in (STAKER_A, STAKER_B):
            self.assertEqual(sh[a]["ok"], 1)

    # f -------------------------------------------------------------------

    def test_epoch_balances_memoized_across_cycles(self):
        # Two cycles of the SAME epoch: the snapshot-ladder queries run once,
        # not per cycle.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.ladder(BAL)
        orig = h.db.staker_poll_rows
        calls = {"n": 0}

        def wrapped(*a, **k):
            calls["n"] += 1
            return orig(*a, **k)

        h.db.staker_poll_rows = wrapped
        h.window([COINBASE], split(COINBASE, BAL))
        first = h.close_all()[0]
        self.assertEqual(first["status"], "VERIFIED")
        after_one = calls["n"]
        self.assertGreater(after_one, 0)
        # A second window in the same epoch must not query the ladder again.
        h.window([COINBASE], split(COINBASE, BAL))
        second = h.close_all()[0]
        self.assertEqual(second["status"], "VERIFIED")
        self.assertEqual(calls["n"], after_one)
        self.assertEqual(h.jobs._election_bal_cache[EPOCH], dict(BAL))

    # g (orchestrator review) ----------------------------------------------

    def test_joiner_in_later_poll_inside_window_is_not_elected(self):
        # B first appears in a poll 5 min after the epoch-start poll, still
        # inside the 15-min window. Only the FIRST poll is the epoch-start
        # state, so B must weigh 0 and a split that pays B nothing VERIFIES.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.ladder({STAKER_A: BAL[STAKER_A], STAKER_C: BAL[STAKER_C]})
        h.ladder({STAKER_A: BAL[STAKER_A], STAKER_B: 60000000000,
                  STAKER_C: BAL[STAKER_C]}, t=EPOCH_LADDER_MS + 300000)
        pot = split(COINBASE, {STAKER_A: BAL[STAKER_A], STAKER_C: BAL[STAKER_C]})
        h.window([COINBASE], pot)
        cyc = h.close_all()[0]
        self.assertEqual(cyc["status"], "VERIFIED")
        self.assertEqual(h.shares(cyc)[STAKER_B]["expected_luna"], 0)

    def test_live_head_waits_and_transient_gaps_are_not_cached(self):
        # At the live head with no poll after the epoch start yet, the close
        # waits; neither the wait nor an unavailable answer is memoized, so
        # the first definitive poll is used as soon as it exists.
        h = self.h = ElectionHarness(ELECTION_FROM_EPOCH=EPOCH)
        h.db.upsert_reward(VALIDATOR, EPOCH_FIRST_BLOCK + 60, COINBASE,
                           EPOCH_ANCHOR_MS, "cb-anchor")
        j = h.jobs
        self.assertIs(j._epoch_start_balances(EPOCH, allow_fallback=False),
                      j.ELECTION_WAIT)
        self.assertIsNone(j._epoch_start_balances(EPOCH, allow_fallback=True))
        self.assertNotIn(EPOCH, j._election_bal_cache)
        # A pre-start poll is only a history fallback, never cached.
        h.ladder({STAKER_A: BAL[STAKER_A]}, t=EPOCH_ANCHOR_MS - 1000)
        self.assertIs(j._epoch_start_balances(EPOCH, allow_fallback=False),
                      j.ELECTION_WAIT)
        self.assertEqual(j._epoch_start_balances(EPOCH, allow_fallback=True),
                         {STAKER_A: BAL[STAKER_A]})
        self.assertNotIn(EPOCH, j._election_bal_cache)
        # The first post-start poll is definitive and memoized.
        h.ladder(BAL)
        self.assertEqual(j._epoch_start_balances(EPOCH, allow_fallback=False),
                         dict(BAL))
        self.assertEqual(j._election_bal_cache[EPOCH], dict(BAL))


if __name__ == "__main__":
    unittest.main()