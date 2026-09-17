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
