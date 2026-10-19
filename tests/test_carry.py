"""Carry-aware verification (M1) and payout modes / operator fees (M2).

DB-level harness: rewards, payout rows and staker snapshots are written
straight into the tracker DB, then run_cycle_close() closes every complete
batch exactly as production does.
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
OPERATOR = "NQ66 OPER ATOR WALL ETXX XXXX XXXX XXXX XXXX"
STRANGER = "NQ99 STRA NGER XXXX XXXX XXXX XXXX XXXX XXXX"


def split(amount, bal=BAL):
    s = sum(bal.values())
    return {a: (b * amount) // s for a, b in bal.items()}


class Harness:
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
        for a, b in BAL.items():
            self.db.upsert_staker_snapshot(VALIDATOR, a, 1, b, 0)
        self.block = 1000
        self.n = 0

    def seed(self, carry=0):
        """A predecessor cycle closed with a known carried balance."""
        cid = self.db.create_cycle(VALIDATOR, self.block - 100, 1, sum(BAL.values()), 0)
        self.db.close_cycle(cid, self.block, 2, 0, 0, "VERIFIED", attribution="tx",
                            carry_luna=carry, fee_carry_luna=0)

    def window(self, incomes, payouts, to=STAKING_CONTRACT_ADDR, named=True,
               extra_rows=()):
        """One cycle window: coinbases (block every 60), then one payout batch.
        payouts: {staker: luna}. extra_rows: (to_addr, luna) outbound rows."""
        for amt in incomes:
            self.block += 60
            self.db.upsert_reward(VALIDATOR, self.block, amt, self.block * 1000,
                                  "cb-%d" % self.block)
        self.block += 30
        for i, (staker, amt) in enumerate(sorted(payouts.items())):
            self.n += 1
            dest = staker if to == "self" else to
            self.db.upsert_restake(VALIDATOR, self.block + i, dest, amt,
                                   (self.block + i) * 1000, "tx-%d" % self.n,
                                   staker if named else "")
        for to_addr, amt in extra_rows:
            self.n += 1
            self.db.upsert_restake(VALIDATOR, self.block + 5, to_addr, amt,
                                   (self.block + 5) * 1000, "tx-%d" % self.n, "")

    def close_all(self):
        # A trailing batch far ahead makes the last real batch complete.
        self.n += 1
        self.db.upsert_restake(VALIDATOR, self.block + 500, STAKING_CONTRACT_ADDR, 1,
                               (self.block + 500) * 1000, "tail-%d" % self.n, STAKER_A)
        if self.db.latest_pending_cycle(VALIDATOR) is None:
            self.jobs._open_pending_cycle()
        self.jobs.run_cycle_close()
        return [c for c in reversed(self.db.recent_cycles(VALIDATOR, 500))
                if c["status"] != "PENDING" and c["opened_block"] >= 1000]


class CarryTest(unittest.TestCase):
    def tearDown(self):
        self.h.db.close()

    def test_carryover_pair_verifies(self):
        # The live shape: window A earns two coinbases but pays only the
        # first (the sweep ran before the second landed); window B earns one
        # and pays two. Pre-M1 both windows were MISMATCH.
        h = self.h = Harness()
        h.seed(0)
        h.window([COINBASE, COINBASE], split(COINBASE))
        h.window([COINBASE], split(2 * COINBASE))
        cyc = h.close_all()
        self.assertEqual([c["status"] for c in cyc[:2]], ["VERIFIED", "VERIFIED"])
        self.assertGreater(cyc[0]["carry_luna"], COINBASE - 10)
        self.assertLess(abs(cyc[1]["carry_luna"]), 10)

    def test_sustained_proportional_skim_is_caught(self):
        # 10% withheld every window, perfectly proportional: the carry grows
        # by ~0.1 coinbase per window and crosses the 1-batch limit.
        h = self.h = Harness()
        h.seed(0)
        for _ in range(14):
            h.window([COINBASE], split(COINBASE * 9 // 10))
        cyc = h.close_all()[:14]
        statuses = [c["status"] for c in cyc]
        self.assertEqual(statuses[:9], ["VERIFIED"] * 9)
        self.assertEqual(statuses[-3:], ["MISMATCH"] * 3)
        reason = h.db.cycle_shares(cyc[-1]["id"])[0]["reason"]
        self.assertIn("rewards withheld", reason)
        self.assertIn("possible skim", reason)

    def test_strict_limit_flags_a_single_short_window(self):
        h = self.h = Harness(CARRY_MAX_BATCHES=0)
        h.seed(0)
        h.window([COINBASE], split(COINBASE * 9 // 10))
        self.assertEqual(h.close_all()[0]["status"], "MISMATCH")

    def test_payout_beyond_earned_plus_carried(self):
        h = self.h = Harness()
        h.seed(0)
        h.window([COINBASE], split(COINBASE + 50000))
        c = h.close_all()[0]
        self.assertEqual(c["status"], "MISMATCH")
        self.assertEqual(c["carry_luna"], 0)        # reset after the flag
        self.assertIn("payout exceeds rewards earned plus carried balance",
                      h.db.cycle_shares(c["id"])[0]["reason"])

    def test_unknown_baseline_establishes_instead_of_flagging(self):
        # First cycle ever: paying out funds earned before tracking began.
        h = self.h = Harness()
        h.db.create_cycle(VALIDATOR, h.block, 1, sum(BAL.values()), 0)  # no predecessor
        h.block += 60          # a full-length window (not a sub-60 slice)
        h.window([], split(600000))
        h.window([COINBASE], split(COINBASE))
        cyc = h.close_all()
        self.assertEqual(cyc[0]["note"], "carry baseline established")
        self.assertEqual(cyc[0]["status"], "VERIFIED")
        self.assertEqual(cyc[0]["carry_luna"], 0)
        self.assertEqual(cyc[1]["status"], "VERIFIED")

    def test_legacy_predecessor_without_carry_is_unknown_baseline(self):
        # A cycle closed before M1 has carry NULL: the first M1 cycle must
        # not treat that as a zero balance and flag the carried payout.
        h = self.h = Harness()
        cid = h.db.create_cycle(VALIDATOR, 900, 1, sum(BAL.values()), 0)
        h.db.close_cycle(cid, 1000, 2, 0, 0, "MISMATCH", attribution="tx")
        h.window([COINBASE], split(2 * COINBASE))
        c = h.close_all()[0]
        self.assertEqual(c["status"], "VERIFIED")
        self.assertEqual(c["note"], "carry baseline established")

    def test_single_underpaid_staker_is_the_one_flagged(self):
        # A carry window (two coinbases paid) where C alone is shorted by
        # 30,000 luna: the median pot keeps A and B clean.
        h = self.h = Harness()
        h.seed(COINBASE)
        pay = split(2 * COINBASE)
        pay[STAKER_C] -= 30000
        h.window([COINBASE], pay)
        c = h.close_all()[0]
        self.assertEqual(c["status"], "MISMATCH")
        shares = {s["staker_address"]: s for s in h.db.cycle_shares(c["id"])}
        self.assertEqual(shares[STAKER_A]["ok"], 1)
        self.assertEqual(shares[STAKER_B]["ok"], 1)
        self.assertEqual(shares[STAKER_C]["ok"], 0)
        self.assertEqual(shares[STAKER_C]["reason"],
                         "paid less than stake share of this payout")
        # The ledger credits only the clean shares of this cycle.
        blocks = lambda a: [r["block"] for r in h.db.staker_rewards(VALIDATOR, a, 10)]
        self.assertNotIn(c["closed_block"], blocks(STAKER_C))
        self.assertIn(c["closed_block"], blocks(STAKER_A))

    def test_overpaid_tiny_shares_do_not_fail_the_big_ones(self):
        # Live shape of cycle 1352 (Sep 2026): tiny stakers got tens of lunas
        # more than their few-luna share; folding that into the pot made the
        # big stakers look ~58 luna short. They are exactly proportional.
        tiny = ["NQ0%d TINY TINY TINY TINY TINY TINY TINY TINY" % i for i in (6, 7)]
        h = self.h = Harness()
        for t_ in tiny:
            h.db.upsert_staker_snapshot(VALIDATOR, t_, 1, 3000000, 0)    # ~35 luna share
        h.seed(0)
        pay = split(COINBASE)
        pay[tiny[0]] = 95
        pay[tiny[1]] = 90
        h.window([COINBASE], pay)
        c = h.close_all()[0]
        self.assertEqual(c["status"], "VERIFIED")

    def test_lagged_staker_view_within_ppm_tolerance(self):
        # The bot's live balances lag the snapshots: the largest share comes
        # in 5 luna low (10 ppm of ~650k = 6). Not an underpayment.
        h = self.h = Harness()
        h.seed(0)
        pay = split(COINBASE)
        pay[STAKER_A] -= 5
        pay[STAKER_B] += 5
        h.window([COINBASE], pay)
        self.assertEqual(h.close_all()[0]["status"], "VERIFIED")

    def test_small_absolute_shortfall_floor(self):
        # Sep-2026 shape: the largest share 18 luna short while two mid-size
        # shares got ~10 extra. Inside SHARE_TOLERANCE_LUNA (50); 51+ fails.
        for short, status in ((18, "VERIFIED"), (120, "MISMATCH")):
            h = self.h = Harness()
            h.seed(0)
            pay = split(COINBASE)
            pay[STAKER_A] -= short
            pay[STAKER_B] += short // 2
            pay[STAKER_C] += short - short // 2
            h.window([COINBASE], pay)
            self.assertEqual(h.close_all()[0]["status"], status, short)
            h.db.close()
        self.h = Harness()

    def test_dust_skipped_staker_leaves_the_weight_set(self):
        # Old dust-floor era: a share below MIN_SHARE was skipped (0 paid)
        # and the others shared the whole payout.
        bal = dict(BAL)
        tiny = "NQ07 TINY TINY TINY TINY TINY TINY TINY TINY"
        h = self.h = Harness()
        h.db.upsert_staker_snapshot(VALIDATOR, tiny, 1, 20000000, 0)  # ~237 luna share
        h.seed(0)
        h.window([COINBASE], split(COINBASE, bal))
        c = h.close_all()[0]
        self.assertEqual(c["status"], "VERIFIED")
        shares = {s["staker_address"]: s for s in h.db.cycle_shares(c["id"])}
        self.assertEqual(shares[tiny]["expected_luna"], 0)

    def test_migration_adds_carry_columns(self):
        d = tempfile.mkdtemp()
        import sqlite3
        conn = sqlite3.connect(os.path.join(d, "tracker.db"))
        conn.execute("CREATE TABLE cycles (id INTEGER PRIMARY KEY AUTOINCREMENT, validator TEXT, "
                     "opened_block INTEGER, closed_block INTEGER, available_luna INTEGER, "
                     "reserve_luna INTEGER, held_luna INTEGER DEFAULT 0, total_luna INTEGER, "
                     "status TEXT CHECK (status IN ('PENDING','VERIFIED','MISMATCH','SKIPPED')), "
                     "opened_at_ms INTEGER, closed_at_ms INTEGER, attribution TEXT NOT NULL DEFAULT '', "
                     "note TEXT NOT NULL DEFAULT '')")
        conn.commit()
        conn.close()
        self.h = Harness()
        db = DB(os.path.join(d, "tracker.db"))
        cols = {r["name"] for r in db.conn.execute("PRAGMA table_info(cycles)")}
        self.assertIn("carry_luna", cols)
        self.assertIn("fee_carry_luna", cols)
        db.close()


class CarryApiTest(unittest.TestCase):
    def test_cycle_json_exposes_carry(self):
        from tracker.api import _cycle_json
        h = Harness()
        h.seed(0)
        h.window([COINBASE, COINBASE], split(COINBASE))
        c = h.close_all()[0]
        j = _cycle_json(h.db.get_cycle(c["id"]))
        self.assertGreater(j["carry_luna"], COINBASE - 10)
        self.assertIn("fee_carry_luna", j)
        h.db.close()


class PayoutModeTest(unittest.TestCase):
    def tearDown(self):
        self.h.db.close()

    def test_transfer_pool_verifies(self):
        # Direct-payout pool: plain transfers to each staker's own address,
        # no relatedAddresses naming (staker taken from the recipient).
        h = self.h = Harness(PAYOUT_MODE="transfer")
        h.seed(0)
        h.window([COINBASE], split(COINBASE), to="self", named=False)
        c = h.close_all()[0]
        self.assertEqual(c["status"], "VERIFIED")
        self.assertEqual(c["attribution"], "tx")

    def test_transfer_payouts_flagged_in_restake_mode(self):
        h = self.h = Harness()
        h.seed(0)
        h.window([COINBASE], split(COINBASE), to="self", named=False)
        c = h.close_all()[0]
        self.assertEqual(c["status"], "MISMATCH")
        reasons = {s["reason"] for s in h.db.cycle_shares(c["id"])}
        self.assertIn("outbound payment outside PAYOUT_MODE=restake", reasons)

    def test_stray_transfer_in_restake_pool_flagged(self):
        h = self.h = Harness()
        h.seed(0)
        h.window([COINBASE], split(COINBASE - 40000), extra_rows=[(STRANGER, 40000)])
        c = h.close_all()[0]
        self.assertEqual(c["status"], "MISMATCH")
        stray = [s for s in h.db.cycle_shares(c["id"]) if s["staker_address"] == STRANGER]
        self.assertEqual(stray[0]["actual_luna"], 40000)
        self.assertEqual(stray[0]["ok"], 0)

    def test_any_mode_accepts_both(self):
        h = self.h = Harness(PAYOUT_MODE="any")
        h.seed(0)
        pay = split(COINBASE)
        h.window([COINBASE], {STAKER_A: pay[STAKER_A]}, to="self", named=False)
        # the rest restaked in the same payout batch
        for i, s in enumerate((STAKER_B, STAKER_C)):
            h.n += 1
            h.db.upsert_restake(VALIDATOR, h.block + 2 + i, STAKING_CONTRACT_ADDR, pay[s],
                                (h.block + 2 + i) * 1000, "tx-any-%d" % i, s)
        self.assertEqual(h.close_all()[0]["status"], "VERIFIED")

    def test_bad_payout_mode_rejected(self):
        self.h = Harness()
        with self.assertRaises(ValueError):
            Config({"PAYOUT_MODE": "airdrop"})

    def test_operator_fee_withdrawal_within_entitlement(self):
        # 10% fee: stakers get 90% of each coinbase, the operator withdraws
        # its 10% to its own wallet. Both verify.
        h = self.h = Harness(VALIDATOR_FEE_PPM=100000, OPERATOR_ADDRS=OPERATOR)
        h.seed(0)
        fee = COINBASE // 10
        h.window([COINBASE], split(COINBASE - fee), extra_rows=[(OPERATOR, fee)])
        c = h.close_all()[0]
        self.assertEqual(c["status"], "VERIFIED")
        self.assertEqual(c["fee_carry_luna"], 0)
        self.assertLess(abs(c["carry_luna"]), 5)      # integer flooring only

    def test_operator_overdraw_flagged(self):
        h = self.h = Harness(VALIDATOR_FEE_PPM=100000, OPERATOR_ADDRS=OPERATOR)
        h.seed(0)
        fee = COINBASE // 10
        h.window([COINBASE], split(COINBASE - fee), extra_rows=[(OPERATOR, 3 * fee)])
        c = h.close_all()[0]
        self.assertEqual(c["status"], "MISMATCH")
        self.assertIn("operator withdrew more than its fee entitlement",
                      h.db.cycle_shares(c["id"])[0]["reason"])

    def test_operator_withdrawal_without_config_is_not_a_payout(self):
        # Without OPERATOR_ADDRS the fee withdrawal is just an unexplained
        # outbound transfer.
        h = self.h = Harness(VALIDATOR_FEE_PPM=100000)
        h.seed(0)
        fee = COINBASE // 10
        h.window([COINBASE], split(COINBASE - fee), extra_rows=[(OPERATOR, fee)])
        self.assertEqual(h.close_all()[0]["status"], "MISMATCH")


if __name__ == "__main__":
    unittest.main()
