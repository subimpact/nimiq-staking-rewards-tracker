"""Tests for validator snapshot address matching and foreign validator exclusion (Task 3)."""

import tempfile
import unittest

from tracker.config import Config
from tracker.db import DB
from tracker.jobs import Jobs

VALIDATOR = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
OTHER_VALIDATOR = "NQ99 OTH8 R0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
STAKER_A = "NQ01 AAAA AAAA AAAA AAAA AAAA AAAA AAAA AAAA"


class _MockFetcher:
    def __init__(self, validators_payload, stakers_payload=None):
        self._validators = validators_payload
        self._stakers = stakers_payload or {"data": []}

    def get_validators(self):
        return self._validators

    def get_stakers(self):
        return self._stakers

    def get_transactions(self, address, stop_before_block=None):
        return []


class ValidatorSnapshotTest(unittest.TestCase):
    def setUp(self):
        cfg = Config({"VALIDATOR_ADDR": VALIDATOR})
        cfg.data_dir = tempfile.mkdtemp()
        self.cfg = cfg
        self.db = DB(cfg.db_path())

    def tearDown(self):
        self.db.close()

    def test_list_payload_without_our_address_skips_validator_snapshot_but_writes_stakers(self):
        """When payload is a non-empty list not containing our address, do not write
        a validator snapshot, but still write staker snapshots."""
        fetcher = _MockFetcher(
            validators_payload={"data": [{"address": OTHER_VALIDATOR, "total": 99999999, "numStakers": 10}]},
            stakers_payload={"data": [{"address": STAKER_A, "balance": 40000000, "inactiveBalance": 0}]},
        )
        jobs = Jobs(self.db, self.cfg, fetcher=fetcher, now_ms=lambda: 1000)
        jobs.run_snapshot()

        # Validator snapshot was NOT written
        snap = self.db.latest_validator_snapshot(VALIDATOR)
        self.assertIsNone(snap)

        # Staker snapshot WAS written
        stakers = self.db.latest_staker_rows(VALIDATOR)
        self.assertEqual(len(stakers), 1)
        self.assertEqual(stakers[0]["address"], STAKER_A)
        self.assertEqual(int(stakers[0]["balance_luna"]), 40000000)

    def test_bare_list_payload_without_our_address_skips_validator_snapshot(self):
        """A bare list payload not containing our address skips validator snapshot."""
        fetcher = _MockFetcher(
            validators_payload=[{"address": OTHER_VALIDATOR, "total": 99999999}],
            stakers_payload={"data": []},
        )
        jobs = Jobs(self.db, self.cfg, fetcher=fetcher, now_ms=lambda: 1000)
        jobs.run_snapshot()
        self.assertIsNone(self.db.latest_validator_snapshot(VALIDATOR))

    def test_list_payload_with_our_address_writes_validator_snapshot(self):
        """A list payload containing our address writes validator snapshot for our address."""
        fetcher = _MockFetcher(
            validators_payload={"data": [
                {"address": OTHER_VALIDATOR, "total": 50000000, "numStakers": 5},
                {"address": VALIDATOR, "total": 100000000, "numStakers": 3},
            ]},
            stakers_payload={"data": []},
        )
        jobs = Jobs(self.db, self.cfg, fetcher=fetcher, now_ms=lambda: 1000)
        jobs.run_snapshot()
        snap = self.db.latest_validator_snapshot(VALIDATOR)
        self.assertIsNotNone(snap)
        self.assertEqual(int(snap["total_luna"]), 100000000)
        self.assertEqual(int(snap["num_stakers"]), 3)

    def test_single_object_payload_matching_our_address(self):
        """Single-object payload (dict without list data) matching our address is accepted."""
        fetcher = _MockFetcher(
            validators_payload={"address": VALIDATOR, "total": 100000000, "numStakers": 3},
            stakers_payload={"data": []},
        )
        jobs = Jobs(self.db, self.cfg, fetcher=fetcher, now_ms=lambda: 1000)
        jobs.run_snapshot()
        snap = self.db.latest_validator_snapshot(VALIDATOR)
        self.assertIsNotNone(snap)
        self.assertEqual(int(snap["total_luna"]), 100000000)

    def test_single_object_payload_without_address_field(self):
        """Single-object payload (dict without list data) without address field is accepted."""
        fetcher = _MockFetcher(
            validators_payload={"total": 100000000, "numStakers": 3},
            stakers_payload={"data": []},
        )
        jobs = Jobs(self.db, self.cfg, fetcher=fetcher, now_ms=lambda: 1000)
        jobs.run_snapshot()
        snap = self.db.latest_validator_snapshot(VALIDATOR)
        self.assertIsNotNone(snap)
        self.assertEqual(int(snap["total_luna"]), 100000000)

    def test_single_object_payload_with_different_address_skips(self):
        """Single-object payload with a different address is skipped."""
        fetcher = _MockFetcher(
            validators_payload={"address": OTHER_VALIDATOR, "total": 100000000, "numStakers": 3},
            stakers_payload={"data": []},
        )
        jobs = Jobs(self.db, self.cfg, fetcher=fetcher, now_ms=lambda: 1000)
        jobs.run_snapshot()
        snap = self.db.latest_validator_snapshot(VALIDATOR)
        self.assertIsNone(snap)


if __name__ == "__main__":
    unittest.main()
