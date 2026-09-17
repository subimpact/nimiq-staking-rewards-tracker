#!/usr/bin/env python3
"""Epoch performance collector for the Nimiq Staking Rewards Tracker.

Counts the validator's produced micro blocks per epoch (slot production,
the NimiqPocket "per-epoch performance" model) plus the epoch's reward
income, and stores them in the tracker DB for the /verify panel.

Design notes:
- Epochs are exactly 43,200 blocks (720 batches x 60 blocks: 59 micro + 1 macro).
- Producers rotate through the elected set one per micro block; the producer
  field is only populated on micro blocks (macro blocks report None).
- The reward table is staking YIELD per batch boundary (one coinbase per 60
  blocks), NOT slot production; produced slots must be counted from micro
  producers, which this script does via batched JSON-RPC.
- Expected slots = (our stake / network total) * micro_total, computed from
  the staker ledger at epoch start; deviation is honest labeling only.

Run on the VPS host (needs `docker exec nimiq` for RPC and write access to
the tracker DB). Cron every 5 minutes is fine; closed epochs are counted once
and cached, the current epoch only counts new batches since the last run.
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time

EPOCH_BLOCKS = 43200          # 720 batches x 60 blocks
MICRO_PER_BATCH = 59          # micro blocks per 60-block batch
BATCH_BLOCKS = 60
RPC_TIMEOUT = 20

META_BOUNDARY = "epoch_boundary"          # start block of the current epoch
META_BOUNDARY_EPOCH = "epoch_boundary_epoch"


def node_rpc(method, params, docker):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    run = subprocess.run(
        docker + ["curl", "-s", "-m", "8", "-X", "POST", "http://127.0.0.1:8648",
                  "-H", "Content-Type: application/json", "-d", json.dumps(body)],
        capture_output=True, text=True, timeout=RPC_TIMEOUT,
    )
    try:
        res = json.loads(run.stdout)
        if "error" in res:
            raise RuntimeError(res["error"])
        return res.get("result")
    except Exception as exc:
        raise RuntimeError("RPC %s failed: %s" % (method, exc))


def batch_blocks(heights, docker):
    """One batched RPC call returning {height: block} for up to 60 heights."""
    body = [{"jsonrpc": "2.0", "id": i + 1,
             "method": "getBlockByNumber", "params": [h, False]}
            for i, h in enumerate(heights)]
    run = subprocess.run(
        docker + ["curl", "-s", "-m", "30", "-X", "POST", "http://127.0.0.1:8648",
                  "-H", "Content-Type: application/json", "-d", json.dumps(body)],
        capture_output=True, text=True, timeout=RPC_TIMEOUT + 20,
    )
    out = {}
    try:
        for resp in json.loads(run.stdout):
            d = resp.get("result", {}).get("data", {})
            n = d.get("number")
            if n is not None:
                out[int(n)] = d
    except Exception as exc:
        raise RuntimeError("batch parse failed: %s" % exc)
    return out


def block_epoch(h, docker):
    res = node_rpc("getBlockByNumber", [h, False], docker)
    d = res.get("data") if isinstance(res, dict) else res
    return d.get("epoch") if isinstance(d, dict) else None


def head_block(docker):
    res = node_rpc("getBlockNumber", [], docker)
    return int(res.get("data") if isinstance(res, dict) else res)


def find_epoch_start(target_epoch, lo, hi, docker):
    """Smallest block in [lo, hi] whose epoch >= target (binary search)."""
    while hi - lo > 1:
        mid = (lo + hi) // 2
        e = block_epoch(mid, docker)
        if e is not None and e >= target_epoch:
            hi = mid
        else:
            lo = mid
    return hi


def compact(addr):
    return (addr or "").replace(" ", "").upper()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("DATA_DIR", "/opt/nimiq-staking-rewards-tracker/data") + "/tracker.db")
    ap.add_argument("--validator", default="NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY")
    ap.add_argument("--docker", default="nimiq")
    args = ap.parse_args()

    docker = ["docker", "exec", args.docker]
    ours = compact(args.validator)

    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row

    # The host-side tool must not depend on the container's schema version.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS epoch_stats ("
        " validator TEXT, epoch INTEGER, start_block INTEGER, end_block INTEGER,"
        " micro_total INTEGER, produced INTEGER, reward_count INTEGER,"
        " reward_sum_luna INTEGER, updated_at_ms INTEGER,"
        " PRIMARY KEY (validator, epoch))"
    )
    conn.commit()

    head = head_block(docker)
    if head is None:
        print("cannot determine head block")
        return 1
    head_epoch = block_epoch(head, docker)
    if head_epoch is None:
        print("cannot determine head epoch at block %s" % head)
        return 1
    print("head block %s epoch %s" % (head, head_epoch))

    # Boundary bootstrap: current epoch start from meta, or binary search once.
    start = conn.execute(
        "SELECT v FROM meta WHERE k=?", (META_BOUNDARY,)
    ).fetchone()
    start_epoch = conn.execute(
        "SELECT v FROM meta WHERE k=?", (META_BOUNDARY_EPOCH,)
    ).fetchone()
    boundary = int(start["v"]) if start and start["v"] is not None else None
    boundary_epoch = int(start_epoch["v"]) if start_epoch and start_epoch["v"] is not None else None

    if boundary is None or boundary_epoch != head_epoch or boundary > head:
        # Re-search: epoch starts are 43200 apart; find current epoch start.
        hi = head
        lo = max(head - EPOCH_BLOCKS * 2, 1)
        boundary = find_epoch_start(head_epoch, lo, hi, docker)
        boundary_epoch = head_epoch
        conn.execute(
            "INSERT OR REPLACE INTO meta(k, v) VALUES(?, ?)",
            (META_BOUNDARY, str(boundary)),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(k, v) VALUES(?, ?)",
            (META_BOUNDARY_EPOCH, str(boundary_epoch)),
        )
        conn.commit()
        print("boundary set: epoch %s starts at %s" % (boundary_epoch, boundary))

    # Sanity: block at boundary has the right epoch.
    e = block_epoch(boundary, docker)
    if e != boundary_epoch:
        print("WARNING boundary mismatch: expected epoch %s at %s, got %s"
              % (boundary_epoch, boundary, e))

    # Which epochs to process: the current one, plus any closed ones that were
    # never fully counted (have no epoch_stats row yet).
    stats = {r["epoch"]: r for r in conn.execute(
        "SELECT * FROM epoch_stats WHERE validator=?", (args.validator,)
    ).fetchall()}

    epochs_to_do = []
    epoch_num = boundary_epoch
    # Current epoch plus up to 3 closed epochs lacking a fully counted row.
    for _ in range(4):
        if epoch_num not in stats:
            epochs_to_do.append(epoch_num)
        epoch_num -= 1
    epochs_to_do.reverse()

    for ep in epochs_to_do:
        ep_start = boundary - (boundary_epoch - ep) * EPOCH_BLOCKS
        if ep == boundary_epoch:
            ep_end = head
        else:
            ep_end = ep_start + EPOCH_BLOCKS - 1
        print("processing epoch %s blocks %s..%s" % (ep, ep_start, ep_end))

        existing = stats.get(ep)
        micro_total = int(existing["micro_total"]) if existing else 0
        produced = int(existing["produced"]) if existing else 0
        walked_any = False
        # Incremental: only walk blocks after the last counted one.
        upto_key = "epoch_stats_upto_%s" % ep
        upto_row = conn.execute(
            "SELECT v FROM meta WHERE k=?", (upto_key,)
        ).fetchone()
        walk_from = int(upto_row["v"]) + 1 if upto_row and upto_row["v"] is not None else ep_start

        if walk_from <= ep_end:
            # Walk batches in [walk_from, ep_end]; one batched RPC per batch.
            batch_start = walk_from - (walk_from - ep_start) % BATCH_BLOCKS
            if batch_start < walk_from:
                batch_start += BATCH_BLOCKS
            while batch_start <= ep_end:
                heights = list(range(batch_start, min(batch_start + BATCH_BLOCKS, ep_end + 1)))
                blocks = batch_blocks(heights, docker)
                for h in heights:
                    d = blocks.get(h)
                    if d:
                        walked_any = True
                    if d and d.get("type") == "micro":
                        micro_total += 1
                        p = d.get("producer")
                        prod = p.get("validator", "") if isinstance(p, dict) else (p or "")
                        if compact(prod) == ours:
                            produced += 1
                batch_start += BATCH_BLOCKS
            conn.execute(
                "INSERT OR REPLACE INTO meta(k, v) VALUES(?, ?)",
                (upto_key, str(ep_end)),
            )

        if existing is None and micro_total == 0 and not walked_any:
            # Node history does not reach this epoch; do not record a bogus
            # "0 produced" row that the UI would display as fact.
            print("epoch %s: no readable blocks (node history too shallow), skipping"
                  % ep)
            continue

        # Reward income in the same window from the rewards table.
        rw = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(amount_luna), 0) AS s "
            "FROM rewards WHERE block>=? AND block<=?",
            (ep_start, ep_end),
        ).fetchone()

        conn.execute(
            "INSERT OR REPLACE INTO epoch_stats "
            "(validator, epoch, start_block, end_block, micro_total, produced, "
            "reward_count, reward_sum_luna, updated_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (args.validator, ep, ep_start, ep_end, micro_total, produced,
             int(rw["n"]), int(rw["s"]), int(time.time() * 1000)),
        )
        print("epoch %s: micro=%s produced=%s rewards=%s (%s luna)"
              % (ep, micro_total, produced, rw["n"], rw["s"]))
    conn.commit()
    conn.close()
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
