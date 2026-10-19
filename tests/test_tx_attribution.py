"""Tests for per-transaction attribution (Task 3)."""

import os
import sqlite3
import tempfile
import unittest

from tests.fake_rpc import (
    STAKER_A,
    STAKER_B,
    STAKER_C,
    STAKER_D,
    VALIDATOR,
    coinbase_tx,
    staking_tx,
    staker_row,
)
from tests.test_verify import (
    COINBASE_AMOUNT,
    EXP_A,
    EXP_B,
    EXP_C,
    TOTAL_LUNA,
    base_env,
    closed_cycle,
    closing_restakes,
    closing_restakes_attributed,
    run_two_pass,
    stage_open_close,
)
from tracker.db import DB


class TxAttributionTest(unittest.TestCase):
    # 1. Snapshot gap: open snapshot exists, NO close snapshot inside the window,
    # a later snapshot exists far after; per-staker txs exact -> cycle VERIFIED,
    # attribution 'tx', actuals equal tx values (not the later cumulative balance).
    def test_snapshot_gap_verified_in_tx_mode(self):
        fake, db, cfg, jobs = base_env()
        try:
            open_rows = [
                staker_row(STAKER_A, 40000000),
                staker_row(STAKER_B, 30000000),
                staker_row(STAKER_C, 30000000),
            ]
            far_later_rows = [
                staker_row(STAKER_A, 40000000 + EXP_A * 3),
                staker_row(STAKER_B, 30000000 + EXP_B * 3),
                staker_row(STAKER_C, 30000000 + EXP_C * 3),
            ]
            db.upsert_validator_snapshot(VALIDATOR, 10000, TOTAL_LUNA, 3, 0)
            for r in open_rows:
                db.upsert_staker_snapshot(
                    VALIDATOR, r["address"], 10000, r["balance"], r["inactiveBalance"]
                )
            for r in far_later_rows:
                db.upsert_staker_snapshot(
                    VALIDATOR, r["address"], 300000, r["balance"], r["inactiveBalance"]
                )

            fake.set_txs([
                coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0", ts=50000),
                *closing_restakes_attributed(
                    [EXP_A, EXP_B, EXP_C],
                    [STAKER_A, STAKER_B, STAKER_C],
                ),
            ])

            jobs.run_ingest()

            pending = db.latest_pending_cycle(VALIDATOR)
            if pending is None:
                db.create_cycle(VALIDATOR, 0, 10000, TOTAL_LUNA, cfg.reserve_luna)

            jobs.run_cycle_close()

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 1)
            c = cycles[0]
            self.assertEqual(c["status"], "VERIFIED")
            self.assertEqual(c["attribution"], "tx")

            shares = {s["staker_address"]: s for s in db.cycle_shares(c["id"])}
            self.assertEqual(shares[STAKER_A]["actual_luna"], EXP_A)
            self.assertEqual(shares[STAKER_B]["actual_luna"], EXP_B)
            self.assertEqual(shares[STAKER_C]["actual_luna"], EXP_C)
            self.assertTrue(all(s["ok"] == 1 for s in shares.values()))
        finally:
            db.close()
            fake.close()

    # 2. run_verify never closes a PENDING cycle (construct a delta-mode window
    # blocked on Guard 1 with close_bound_ms None; call run_verify; status still PENDING).
    def test_run_verify_never_closes_pending_cycle(self):
        fake, db, cfg, jobs = base_env()
        try:
            # Open snapshot only, no close snapshot
            db.upsert_validator_snapshot(VALIDATOR, 10000, TOTAL_LUNA, 3, 0)
            for addr, bal in ((STAKER_A, 40000000), (STAKER_B, 30000000), (STAKER_C, 30000000)):
                db.upsert_staker_snapshot(VALIDATOR, addr, 10000, bal, 0)

            # Delta mode txs (closing_restakes omits relatedAddresses, so rows are unmapped)
            # No later batch present after block 100, so close_bound_ms is None
            fake.set_txs([
                coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0", ts=50000),
                *closing_restakes([EXP_A, EXP_B, EXP_C]),
            ])

            jobs.run_ingest()

            pending = db.latest_pending_cycle(VALIDATOR)
            if pending is None:
                db.create_cycle(VALIDATOR, 0, 10000, TOTAL_LUNA, cfg.reserve_luna)

            # run_cycle_close will attempt close; Guard 1 sees no close snapshot and
            # close_bound_ms is None, so it leaves the cycle PENDING.
            jobs.run_cycle_close()

            pending_after_close = db.latest_pending_cycle(VALIDATOR)
            self.assertIsNotNone(pending_after_close)
            self.assertEqual(pending_after_close["status"], "PENDING")
            self.assertEqual(len(closed_cycle(db)), 0)

            # Now call run_verify: must NEVER close or modify the pending cycle.
            jobs.run_verify()

            pending_after_verify = db.latest_pending_cycle(VALIDATOR)
            self.assertIsNotNone(pending_after_verify)
            self.assertEqual(pending_after_verify["status"], "PENDING")
            self.assertEqual(len(closed_cycle(db)), 0)
        finally:
            db.close()
            fake.close()

    # 3. Delta mode with a later batch present and no close snapshot ->
    # SKIPPED, note "insufficient snapshot coverage", no shares, no ledger credits.
    def test_delta_mode_with_later_batch_and_no_snapshot_skipped(self):
        fake, db, cfg, jobs = base_env()
        try:
            # Open snapshot only
            db.upsert_validator_snapshot(VALIDATOR, 10000, TOTAL_LUNA, 3, 0)
            for addr, bal in ((STAKER_A, 40000000), (STAKER_B, 30000000), (STAKER_C, 30000000)):
                db.upsert_staker_snapshot(VALIDATOR, addr, 10000, bal, 0)

            # Delta mode batch (unmapped rows) + a later batch at block 200
            fake.set_txs([
                coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0", ts=50000),
                *closing_restakes([EXP_A, EXP_B, EXP_C]),
                staking_tx(200, 1000, "tx-batch-2", ts=200000),
            ])

            jobs.run_ingest()

            pending = db.latest_pending_cycle(VALIDATOR)
            if pending is None:
                db.create_cycle(VALIDATOR, 0, 10000, TOTAL_LUNA, cfg.reserve_luna)

            jobs.run_cycle_close()

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 1)
            c = cycles[0]
            self.assertEqual(c["status"], "SKIPPED")
            self.assertEqual(c["attribution"], "delta")
            self.assertEqual(c["note"], "insufficient snapshot coverage")

            # No shares
            self.assertEqual(db.cycle_shares(c["id"]), [])

            # No ledger credits
            for addr in (STAKER_A, STAKER_B, STAKER_C):
                self.assertEqual(db.staker_rewards(VALIDATOR, addr, 10), [])
        finally:
            db.close()
            fake.close()

    # 4. Mid-cycle joiner with a REAL stake (100 NIM = 10,000,000 luna) not paid
    # this window -> VERIFIED in tx mode.
    def test_mid_cycle_joiner_verified_in_tx_mode(self):
        fake, db, cfg, jobs = base_env()
        try:
            joiner_stake = 10000000  # 100 NIM
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
                    staker_row(STAKER_D, joiner_stake),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes_attributed(
                    [EXP_A, EXP_B, EXP_C],
                    [STAKER_A, STAKER_B, STAKER_C],
                ),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 1)
            c = cycles[0]
            self.assertEqual(c["status"], "VERIFIED")
            self.assertEqual(c["attribution"], "tx")

            shares = {s["staker_address"]: s for s in db.cycle_shares(c["id"])}
            self.assertIn(STAKER_A, shares)
            self.assertIn(STAKER_B, shares)
            self.assertIn(STAKER_C, shares)
            self.assertNotIn(STAKER_D, shares)
            self.assertTrue(all(s["ok"] == 1 for s in shares.values()))
        finally:
            db.close()
            fake.close()

    # 5. Underpayment of one staker by 1,000 luna in tx mode ->
    # MISMATCH with that staker ok 0 (the false-VERIFIED guard).
    def test_underpayment_mismatch_with_staker_ok_zero(self):
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
                    staker_row(STAKER_C, 30000000 + EXP_C - 1000),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes_attributed(
                    [EXP_A, EXP_B, EXP_C - 1000],
                    [STAKER_A, STAKER_B, STAKER_C],
                ),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 1)
            c = cycles[0]
            self.assertEqual(c["status"], "MISMATCH")
            self.assertEqual(c["attribution"], "tx")

            shares = {s["staker_address"]: s for s in db.cycle_shares(c["id"])}
            self.assertEqual(shares[STAKER_C]["ok"], 0)
            self.assertEqual(shares[STAKER_C]["actual_luna"], EXP_C - 1000)
            self.assertEqual(shares[STAKER_A]["actual_luna"], EXP_A)
            self.assertEqual(shares[STAKER_B]["actual_luna"], EXP_B)
        finally:
            db.close()
            fake.close()

    # 6. Skim: batch sums to 95% of the coinbase while proportional ->
    # MISMATCH (Guard 4b) in tx mode.
    def test_skim_mismatch_in_tx_mode(self):
        fake, db, cfg, jobs = base_env()
        # M1: one short window is carry under the default limit; the strict
        # limit (0 batches) keeps this a single-window skim test.
        cfg.carry_max_batches = 0
        try:
            skim_batch_total = 1900000  # 95% of 2,000,000
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
                closing_restakes_attributed(
                    [s_a, s_b, s_c],
                    [STAKER_A, STAKER_B, STAKER_C],
                ),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 1)
            c = cycles[0]
            self.assertEqual(c["status"], "MISMATCH")
            self.assertEqual(c["attribution"], "tx")

            shares = db.cycle_shares(c["id"])
            self.assertTrue(all("possible skim" in s["reason"] for s in shares))
        finally:
            db.close()
            fake.close()

    # 7. Payee not in open set -> its own share row, ok 0, MISMATCH.
    def test_payee_not_in_open_set_mismatch(self):
        fake, db, cfg, jobs = base_env()
        try:
            # STAKER_A and STAKER_B in open set; STAKER_C is NOT in open set
            stage_open_close(
                fake,
                [
                    staker_row(STAKER_A, 50000000),
                    staker_row(STAKER_B, 50000000),
                ],
                [
                    staker_row(STAKER_A, 50000000 + 800000),
                    staker_row(STAKER_B, 50000000 + 1000000),
                    staker_row(STAKER_C, 200000),
                ],
            )
            fake.set_staged_txs([
                [coinbase_tx(50, COINBASE_AMOUNT, "tx-cb-0")],
                closing_restakes_attributed(
                    [800000, 1000000, 200000],
                    [STAKER_A, STAKER_B, STAKER_C],
                ),
            ])
            run_two_pass(jobs)

            cycles = closed_cycle(db)
            self.assertEqual(len(cycles), 1)
            c = cycles[0]
            self.assertEqual(c["status"], "MISMATCH")
            self.assertEqual(c["attribution"], "tx")

            shares = {s["staker_address"]: s for s in db.cycle_shares(c["id"])}
            self.assertIn(STAKER_C, shares)
            self.assertEqual(shares[STAKER_C]["expected_luna"], 0)
            self.assertEqual(shares[STAKER_C]["actual_luna"], 200000)
            self.assertEqual(shares[STAKER_C]["ok"], 0)
            self.assertEqual(
                shares[STAKER_C]["reason"],
                "payee had no opening balance (joined during cycle)",
            )
        finally:
            db.close()
            fake.close()

    # 8. Migration: open a DB created with the OLD cycles schema (no attribution/note columns),
    # DB() adds them; existing rows default ''.
    def test_migration_adds_attribution_and_note(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "old_tracker.db")
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE cycles (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  validator TEXT,
                  opened_block INTEGER,
                  closed_block INTEGER,
                  available_luna INTEGER,
                  reserve_luna INTEGER,
                  held_luna INTEGER DEFAULT 0,
                  total_luna INTEGER,
                  status TEXT CHECK (status IN ('PENDING','VERIFIED','MISMATCH','SKIPPED')),
                  opened_at_ms INTEGER,
                  closed_at_ms INTEGER
                );
            """)
            conn.execute("""
                INSERT INTO cycles (
                    id, validator, opened_block, closed_block,
                    available_luna, reserve_luna, held_luna, total_luna, status,
                    opened_at_ms, closed_at_ms
                ) VALUES (
                    1, ?, 0, 100, 2000000, 100000, 0, 100000000, 'VERIFIED',
                    1000, 2000
                );
            """, (VALIDATOR,))
            conn.commit()
            conn.close()

            db = DB(db_path)
            try:
                row = db.get_cycle(1)
                self.assertIsNotNone(row)
                self.assertEqual(row["attribution"], "")
                self.assertEqual(row["note"], "")

                cols = {r["name"] for r in db.conn.execute("PRAGMA table_info(cycles)").fetchall()}
                self.assertIn("attribution", cols)
                self.assertIn("note", cols)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
