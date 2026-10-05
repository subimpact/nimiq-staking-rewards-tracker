"""Regression: next_restake_batch must hop restake-silent stretches.

Root cause of the 12-day close wedge found 2026-10-06: an empty 2000-block
scan window returned None, which run_cycle_close treats as "nothing to
close" and stops forever - even though complete restake batches exist far
above the cursor. The gap itself was left by the old 200-tx ingest bug
(cycle 3321: opened 62078649, first stored restake 62121732 = a 43k-block
quiet stretch, one full epoch).
"""

import tempfile
import unittest

from tracker.config import Config
from tracker.db import DB

VALIDATOR = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
BASE = 63400000


class NextRestakeBatchTest(unittest.TestCase):
    def setUp(self):
        cfg = Config({"VALIDATOR_ADDR": VALIDATOR})
        cfg.data_dir = tempfile.mkdtemp()
        self.db = DB(cfg.db_path())

    def test_hops_silent_stretch_and_finds_batch(self):
        """Restakes exist only at BASE+43_000 (43k-block silent stretch after
        the cursor): the function must hop forward and return that complete
        batch, not None."""
        db = self.db
        db.upsert_restake(VALIDATOR, BASE, VALIDATOR, 60, 1000, "tx-a", "")
        db.upsert_restake(VALIDATOR, BASE + 43_000, VALIDATOR, 60, 1001, "tx-b", "")
        got = db.next_restake_batch(VALIDATOR, BASE, 10)
        self.assertIsNotNone(got)
        last, batch = got
        self.assertEqual(last, BASE + 43_000)
        self.assertEqual([r["tx_hash"] for r in batch], ["tx-b"])

    def test_none_when_no_restakes_beyond_at_all(self):
        """With no restakes anywhere above the cursor the result is still
        None (end of table)."""
        self.db.upsert_restake(VALIDATOR, BASE, VALIDATOR, 60, 1000, "tx-a", "")
        got = self.db.next_restake_batch(VALIDATOR, BASE, 10)
        self.assertIsNone(got)

    def test_drain_wedged_shape_recovers_end_to_end(self):
        """A pending cycle opened before a 43k-block silent stretch closes
        once the batch hop finds the next batch (the cycle-3321 shape)."""
        db = self.db
        # One restake below the cursor (the cycle's opener), then silence,
        # then normal batches resume at +43k.
        db.upsert_restake(VALIDATOR, BASE, VALIDATOR, 60, 1000, "tx-open", "")
        for i in range(3):
            db.upsert_restake(
                VALIDATOR, BASE + 43_000 + i, VALIDATOR, 60, 1001 + i,
                "tx-b%d" % i, "",
            )
        got = db.next_restake_batch(VALIDATOR, BASE, 10)
        self.assertIsNotNone(got)
        last, batch = got
        # The batch captured is the FULL resumed burst (3 txs, gaps < 10).
        self.assertEqual([r["tx_hash"] for r in batch],
                         ["tx-b0", "tx-b1", "tx-b2"])
        self.assertEqual(last, BASE + 43_002)


if __name__ == "__main__":
    unittest.main()