"""API route tests against a real http.server bound to a temp port."""

import json
import os
import tempfile
import threading
import unittest
import urllib.parse
import urllib.request

from tracker.api import serve
from tracker.config import Config
from tracker.db import DB

from tests.fake_rpc import (
    STAKER_A,
    STAKER_B,
    STAKER_C,
    VALIDATOR,
)

RESERVE = 100000


def make_env(data_dir):
    return {
        "RPC_URL": "http://127.0.0.1:1/unused",
        "STAKERS_URL_TEMPLATE": "http://127.0.0.1:1/stakers/{address}",
        "VALIDATORS_URL_TEMPLATE": "http://127.0.0.1:1/validators",
        "VALIDATOR_ADDR": VALIDATOR,
        "REWARD_ADDR": VALIDATOR,
        "RESERVE_LUNA": str(RESERVE),
        "DATA_DIR": data_dir,
        "PORT": "0",
    }


def seed(db):
    now = 1000000
    db.upsert_validator_snapshot(VALIDATOR, now, 100000000, 3, 0)
    db.upsert_staker_snapshot(VALIDATOR, STAKER_A, now, 40000000, 0)
    db.upsert_staker_snapshot(VALIDATOR, STAKER_B, now, 60000000, 0)
    db.upsert_reward(VALIDATOR, 100, 2000000, now, "tx-cb")
    cid = db.create_cycle(VALIDATOR, 100, now, 100000000, RESERVE)
    db.close_cycle(cid, 200, now + 1000, 1900000, 0, "VERIFIED")
    db.upsert_cycle_share(cid, STAKER_A, 760000, 760000, "tx-a", 1)
    db.upsert_cycle_share(cid, STAKER_B, 1140000, 1140000, "tx-b", 1)
    db.upsert_staker_reward(STAKER_A, 200, 760000, "tx-a", now)
    db.upsert_staker_reward(STAKER_B, 200, 1140000, "tx-b", now)
    db.set_meta("last_run_ms", str(now))
    return cid


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.cfg = Config(make_env(self.dir))
        self.db = DB(self.cfg.db_path())
        self.cid = seed(self.db)

        cfg0 = Config(make_env(self.dir))
        self.server = serve(self.db, cfg0, bind="127.0.0.1", port=0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.db.close()

    def _get(self, path):
        # Nimiq addresses contain spaces; percent-encode for the URL while
        # keeping query separators intact.
        path = urllib.parse.quote(path, safe="/?&=")
        with urllib.request.urlopen(
            "http://127.0.0.1:%s%s" % (self.port, path), timeout=5
        ) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_health(self):
        status, body = self._get("/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(body["db"], self.cfg.db_path())

    def test_summary(self):
        status, body = self._get("/api/validators/%s/summary" % VALIDATOR)
        self.assertEqual(status, 200)
        self.assertEqual(body["validator"], VALIDATOR)
        self.assertEqual(body["num_stakers"], 2)
        self.assertEqual(body["staked_luna"], 100000000)
        self.assertEqual(body["rewards"]["count"], 1)

    def test_stakers(self):
        status, body = self._get("/api/validators/%s/stakers" % VALIDATOR)
        self.assertEqual(status, 200)
        addresses = {s["address"] for s in body["stakers"]}
        self.assertEqual(addresses, {STAKER_A, STAKER_B})
        # Order by balance desc: B (60NIM) before A (40NIM).
        self.assertEqual(body["stakers"][0]["address"], STAKER_B)

    def test_cycles(self):
        status, body = self._get(
            "/api/validators/%s/cycles?limit=10" % VALIDATOR
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["cycles"]), 1)
        c = body["cycles"][0]
        self.assertEqual(c["status"], "VERIFIED")
        self.assertEqual(c["available_luna"], 1900000)

    def test_shares(self):
        status, body = self._get(
            "/api/validators/%s/cycles/%s/shares" % (VALIDATOR, self.cid)
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["cycle"]["status"], "VERIFIED")
        self.assertEqual(len(body["shares"]), 2)
        ok = {s["staker_address"]: s["ok"] for s in body["shares"]}
        self.assertTrue(all(ok.values()))

    def test_rewards(self):
        status, body = self._get(
            "/api/validators/%s/stakers/%s/rewards?limit=10" % (VALIDATOR, STAKER_A)
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["rewards"]), 1)
        self.assertEqual(body["rewards"][0]["amount_luna"], 760000)

    def test_rewards_limit_is_honored(self):
        """Regression: query-string limit was dropped, so the API always
        returned up to 50 rows (the page said "last 10" but rendered 50)."""
        for i in range(5):
            self.db.upsert_staker_reward(STAKER_A, 300 + i, 100000 + i, "tx-%d" % i, 1000 + i)
        status, body = self._get(
            "/api/validators/%s/stakers/%s/rewards?limit=3" % (VALIDATOR, STAKER_A)
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["rewards"]), 3)
        self.assertTrue(body["has_more"])
        # Newest first (ordered by block desc).
        blocks = [r["block"] for r in body["rewards"]]
        self.assertEqual(blocks, sorted(blocks, reverse=True))

    def test_rewards_before_cursor(self):
        for i in range(5):
            self.db.upsert_staker_reward(STAKER_C, 400 + i, 200000 + i, "tx-%d" % i, 1000 + i)
        # First page: latest 2 (blocks 404, 403).
        status, p1 = self._get(
            "/api/validators/%s/stakers/%s/rewards?limit=2" % (VALIDATOR, STAKER_C)
        )
        self.assertEqual([r["block"] for r in p1["rewards"]], [404, 403])
        self.assertTrue(p1["has_more"])
        # Second page: before block 403 -> blocks 402, 401.
        status, p2 = self._get(
            "/api/validators/%s/stakers/%s/rewards?limit=2&before=403" % (VALIDATOR, STAKER_C)
        )
        self.assertEqual([r["block"] for r in p2["rewards"]], [402, 401])
        self.assertTrue(p2["has_more"])
        # Third page: before 401 -> just 400, no more.
        status, p3 = self._get(
            "/api/validators/%s/stakers/%s/rewards?limit=2&before=401" % (VALIDATOR, STAKER_C)
        )
        self.assertEqual([r["block"] for r in p3["rewards"]], [400])
        self.assertFalse(p3["has_more"])

    def test_cycles_has_more_and_before(self):
        # 1 seeded cycle + 2 here = 3 total.
        cid2 = self.db.create_cycle(VALIDATOR, 300, 300000, 100000000, RESERVE)
        self.db.close_cycle(cid2, 400, 400000, 1000000, 0, "VERIFIED")
        cid3 = self.db.create_cycle(VALIDATOR, 500, 500000, 100000000, RESERVE)
        self.db.close_cycle(cid3, 600, 600000, 1000000, 0, "VERIFIED")
        status, body = self._get(
            "/api/validators/%s/cycles?limit=2" % VALIDATOR
        )
        self.assertEqual(status, 200)
        ids = [c["id"] for c in body["cycles"]]
        self.assertEqual(ids, sorted(ids, reverse=True))
        self.assertEqual(len(ids), 2)
        self.assertTrue(body["has_more"])
        status, p2 = self._get(
            "/api/validators/%s/cycles?limit=2&before=%d" % (VALIDATOR, ids[-1])
        )
        self.assertEqual(len(p2["cycles"]), 1)
        self.assertFalse(p2["has_more"])

    def test_verify_route(self):
        status, body = self._get("/api/verify?limit=10")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["cycles"]), 1)
        self.assertEqual(body["cycles"][0]["status"], "VERIFIED")

    def test_missing_route_is_404(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self._get("/api/validators/%s/nope" % VALIDATOR)
        self.assertEqual(ctx.exception.code, 404)
        err = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertEqual(err["error"], "not found")

    def test_unknown_cycle_shares(self):
        status, body = self._get(
            "/api/validators/%s/cycles/99999/shares" % VALIDATOR
        )
        self.assertEqual(status, 200)
        self.assertIn("error", body)


if __name__ == "__main__":
    unittest.main()
