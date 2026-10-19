"""tools/epoch_stats.py against a fake node (the --rpc-cmd prefix).

Fake chain: epoch e covers blocks [e*43200+1, (e+1)*43200]; a block whose
number is a multiple of 60 is the batch's macro block, every other block is a
micro block; our validator produces every micro block whose number is a
multiple of 7. Blocks below FLOOR are unreadable (shallow node history).
"""

import os
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "epoch_stats.py")
OURS = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
E = 43200

FAKE_NODE = textwrap.dedent('''
    import json, os, sys
    args = sys.argv[1:]
    if args and args[0] == "curl":
        args = args[1:]
    body = json.loads(args[args.index("-d") + 1])
    head = int(open(os.environ["FAKE_HEAD"]).read())
    floor = int(os.environ.get("FAKE_FLOOR", "0"))
    ours = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
    def block(h):
        if h > head:
            return None
        if h < floor:
            return {"number": h, "epoch": None}
        micro = h % 60 != 0
        return {"number": h, "epoch": (h - 1) // 43200,
                "type": "micro" if micro else "macro",
                "producer": ({"validator": ours if h % 7 == 0 else "NQ00 OTHER"}
                             if micro else None)}
    def one(req):
        m = req["method"]
        if m == "getBlockNumber":
            return {"jsonrpc": "2.0", "id": req["id"], "result": {"data": head}}
        b = block(req["params"][0])
        if b is None:
            return {"jsonrpc": "2.0", "id": req["id"], "error": {"message": "not found"}}
        return {"jsonrpc": "2.0", "id": req["id"], "result": {"data": b}}
    out = [one(r) for r in body] if isinstance(body, list) else one(body)
    print(json.dumps(out))
''')


def expected(lo, hi):
    micro = [h for h in range(lo, hi + 1) if h % 60 != 0]
    return len(micro), sum(1 for h in micro if h % 7 == 0)


class EpochStatsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = os.path.join(self.dir, "tracker.db")
        conn = sqlite3.connect(self.db)
        conn.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT)")
        conn.execute("CREATE TABLE rewards (validator TEXT, block INTEGER, amount_luna INTEGER, "
                     "ts_ms INTEGER, tx_hash TEXT)")
        conn.commit()
        conn.close()
        self.node = os.path.join(self.dir, "fake_node.py")
        open(self.node, "w").write(FAKE_NODE)
        self.head = os.path.join(self.dir, "head")
        self.env = dict(os.environ, FAKE_HEAD=self.head)

    def run_tool(self, head, *extra):
        open(self.head, "w").write(str(head))
        r = subprocess.run(
            [sys.executable, TOOL, "--db", self.db, "--validator", OURS,
             "--rpc-cmd", "%s %s" % (sys.executable, self.node)] + list(extra),
            capture_output=True, text=True, env=self.env, timeout=120)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        return r.stdout

    def row(self, ep):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        r = conn.execute("SELECT * FROM epoch_stats WHERE epoch=?", (ep,)).fetchone()
        conn.close()
        return r

    def test_current_epoch_keeps_counting_across_runs(self):
        # The pre-fix tool wrote the current epoch once and never again.
        start = 3 * E + 1
        self.run_tool(start + 299, "--lookback", "0")
        r = self.row(3)
        self.assertEqual((r["micro_total"], r["produced"]), expected(start, start + 299))
        self.run_tool(start + 1000, "--lookback", "0")
        r = self.row(3)
        self.assertEqual(r["end_block"], start + 1000)
        self.assertEqual((r["micro_total"], r["produced"]), expected(start, start + 1000))

    def test_frozen_closed_epoch_is_completed(self):
        # A closed epoch left at its first few hundred blocks (the frozen
        # rows on the live panel) is finished on later runs.
        start2 = 2 * E + 1
        cut = 3 * E - 150
        mt, pr = expected(start2, cut)
        conn = sqlite3.connect(self.db)
        conn.execute("CREATE TABLE epoch_stats (validator TEXT, epoch INTEGER, start_block INTEGER, "
                     "end_block INTEGER, micro_total INTEGER, produced INTEGER, reward_count INTEGER, "
                     "reward_sum_luna INTEGER, updated_at_ms INTEGER, PRIMARY KEY (validator, epoch))")
        conn.execute("INSERT INTO epoch_stats VALUES (?, 2, ?, ?, ?, ?, 0, 0, 0)",
                     (OURS, start2, cut, mt, pr))
        conn.execute("INSERT INTO meta VALUES ('epoch_stats_upto_2', ?)", (str(cut),))
        conn.commit()
        conn.close()
        self.run_tool(3 * E + 61, "--lookback", "0")
        r = self.row(2)
        self.assertEqual(r["end_block"], 3 * E)
        self.assertEqual((r["micro_total"], r["produced"]), expected(start2, 3 * E))

    def test_unreadable_history_is_not_counted_as_zero(self):
        # Node history starts mid-epoch 2: the walk stops at the hole and no
        # row claims blocks it never read.
        self.env["FAKE_FLOOR"] = str(3 * E - 30)
        self.run_tool(3 * E + 61, "--lookback", "1")
        self.assertIsNone(self.row(2))
        self.assertIsNotNone(self.row(3))

    def test_history_floor_stops_older_walks_and_keeps_rewards_whole(self):
        # Epochs 1 and 2 left partial; the node serves only from mid-epoch 2.
        # Epoch 2 counts up to... nothing new (its next block is below the
        # floor), epoch 1 is not walked at all, and both keep their reward
        # sums over the whole epoch.
        conn = sqlite3.connect(self.db)
        conn.execute("CREATE TABLE epoch_stats (validator TEXT, epoch INTEGER, start_block INTEGER, "
                     "end_block INTEGER, micro_total INTEGER, produced INTEGER, reward_count INTEGER, "
                     "reward_sum_luna INTEGER, updated_at_ms INTEGER, PRIMARY KEY (validator, epoch))")
        for ep in (1, 2):
            st = ep * E + 1
            conn.execute("INSERT INTO epoch_stats VALUES (?, ?, ?, ?, 5, 0, 0, 0, 0)", (OURS, ep, st, st + 5))
            conn.execute("INSERT INTO meta VALUES (?, ?)", ("epoch_stats_upto_%d" % ep, str(st + 5)))
            for b in range(st + 59, st + E, 60):
                conn.execute("INSERT INTO rewards VALUES (?, ?, 10, 0, 'h')", (OURS, b))
        conn.commit()
        conn.close()
        self.env["FAKE_FLOOR"] = str(2 * E + 1000)
        self.run_tool(3 * E + 61, "--lookback", "0")
        for ep in (1, 2):
            r = self.row(ep)
            self.assertEqual(r["end_block"], ep * E + 6)
            self.assertEqual(r["micro_total"], 5)
            self.assertEqual(r["reward_count"], 720)

    def test_budget_bounds_a_run(self):
        start = 3 * E + 1
        out = self.run_tool(start + 600, "--max-batches", "4", "--lookback", "0")
        r = self.row(3)
        self.assertEqual(r["end_block"], start + 4 * 60 - 1)
        self.assertIn("batch budget spent", out)
        self.run_tool(start + 600, "--lookback", "0")
        self.assertEqual(self.row(3)["end_block"], start + 600)
        self.assertEqual((self.row(3)["micro_total"], self.row(3)["produced"]),
                         expected(start, start + 600))


if __name__ == "__main__":
    unittest.main()
