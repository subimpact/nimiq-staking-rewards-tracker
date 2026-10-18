"""Tests for format-tolerant address matching in ingest (Task 4)."""

import tempfile
import unittest

from tracker.config import Config, COINBASE_ADDR, STAKING_CONTRACT_ADDR
from tracker.db import DB
from tracker.jobs import Jobs

VALIDATOR_SPACED = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
REWARD_LOWER_NOSPACE = "nq08act8t0feptg8p5rlh2s3qgxhv15rnvxy"


class _MockFetcher:
    def __init__(self, txs):
        self._txs = txs

    def walk_transactions(self, address, stop_before_block=None, start_cursor=None, max_pages=3):
        return self._txs, "end_of_history", None

    def get_transactions(self, address, stop_before_block=None):
        return self._txs


class AddressMatchingTest(unittest.TestCase):
    def setUp(self):
        cfg = Config({
            "VALIDATOR_ADDR": VALIDATOR_SPACED,
            "REWARD_ADDR": REWARD_LOWER_NOSPACE,
        })
        cfg.data_dir = tempfile.mkdtemp()
        self.cfg = cfg
        self.db = DB(cfg.db_path())

    def tearDown(self):
        self.db.close()

    def test_unnormalized_config_address_classifies_spaced_txs(self):
        """When the config address is given without spaces and in lower case,
        inbound rewards and outbound restakes still classify."""
        txs = [
            {
                "blockNumber": 100,
                "sender": COINBASE_ADDR,
                "recipient": VALIDATOR_SPACED,
                "value": 1500000,
                "timestamp": 1000,
                "hash": "tx-cb",
            },
            {
                "blockNumber": 101,
                "sender": VALIDATOR_SPACED,
                "recipient": STAKING_CONTRACT_ADDR,
                "value": 500000,
                "timestamp": 1001,
                "hash": "tx-restake",
                "relatedAddresses": [VALIDATOR_SPACED, STAKING_CONTRACT_ADDR, "NQ01 AAAA AAAA AAAA AAAA AAAA AAAA AAAA AAAA"],
            },
        ]
        fetcher = _MockFetcher(txs)
        jobs = Jobs(self.db, self.cfg, fetcher=fetcher)
        jobs.run_ingest()

        # Both inbound coinbase and outbound restake were classified and stored
        r_count = self.db.conn.execute("SELECT COUNT(*) FROM rewards").fetchone()[0]
        self.assertEqual(r_count, 1)
        row_r = self.db.conn.execute("SELECT * FROM rewards").fetchone()
        self.assertEqual(row_r["block"], 100)
        self.assertEqual(row_r["amount_luna"], 1500000)

        rt_count = self.db.conn.execute("SELECT COUNT(*) FROM restake_txs").fetchone()[0]
        self.assertEqual(rt_count, 1)
        row_rt = self.db.conn.execute("SELECT * FROM restake_txs").fetchone()
        self.assertEqual(row_rt["block"], 101)
        self.assertEqual(row_rt["amount_luna"], 500000)
        # Verify raw string stored
        self.assertEqual(row_rt["to_addr"], STAKING_CONTRACT_ADDR)


if __name__ == "__main__":
    unittest.main()
