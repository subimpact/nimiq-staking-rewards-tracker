"""SQLite persistence layer for the Nimiq Staking Rewards Tracker.

Uses WAL mode for concurrency between the scheduler thread and the read-only
HTTP API thread. All writes are idempotent upserts. Money values are integer
lunas throughout (1 NIM = 1e5 lunas).
"""

import os
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

CREATE TABLE IF NOT EXISTS validator_snapshots (
  validator TEXT,
  fetched_at_ms INTEGER,
  total_luna INTEGER,
  num_stakers INTEGER,
  deposit_luna INTEGER
);

CREATE TABLE IF NOT EXISTS staker_snapshots (
  validator TEXT,
  address TEXT,
  fetched_at_ms INTEGER,
  balance_luna INTEGER,
  inactive_luna INTEGER,
  PRIMARY KEY (validator, address, fetched_at_ms)
);

CREATE TABLE IF NOT EXISTS rewards (
  validator TEXT,
  block INTEGER,
  amount_luna INTEGER,
  ts_ms INTEGER,
  tx_hash TEXT,
  PRIMARY KEY (validator, block)
);

CREATE TABLE IF NOT EXISTS cycles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  validator TEXT,
  opened_block INTEGER,
  closed_block INTEGER,
  available_luna INTEGER,
  reserve_luna INTEGER,
  held_luna INTEGER DEFAULT 0,
  total_luna INTEGER,
  status TEXT CHECK (status IN ('PENDING','VERIFIED','MISMATCH')),
  opened_at_ms INTEGER,
  closed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS cycle_shares (
  cycle_id INTEGER REFERENCES cycles(id),
  staker_address TEXT,
  expected_luna INTEGER,
  actual_luna INTEGER,
  tx_hash TEXT,
  ok INTEGER,
  reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (cycle_id, staker_address)
);

CREATE TABLE IF NOT EXISTS staker_rewards (
  staker_address TEXT,
  block INTEGER,
  amount_luna INTEGER,
  tx_hash TEXT,
  ts_ms INTEGER,
  PRIMARY KEY (staker_address, block, tx_hash)
);

CREATE INDEX IF NOT EXISTS idx_staker_rewards_addr
  ON staker_rewards(staker_address, ts_ms);

CREATE TABLE IF NOT EXISTS restake_txs (
  validator TEXT, block INTEGER, to_addr TEXT, amount_luna INTEGER, ts_ms INTEGER,
  tx_hash TEXT, PRIMARY KEY (validator, block, to_addr, tx_hash)
);

CREATE TABLE IF NOT EXISTS epoch_stats (
  validator TEXT,
  epoch INTEGER,
  start_block INTEGER,
  end_block INTEGER,
  micro_total INTEGER,
  produced INTEGER,
  reward_count INTEGER,
  reward_sum_luna INTEGER,
  updated_at_ms INTEGER,
  PRIMARY KEY (validator, epoch)
);
"""


class DB:
    def __init__(self, db_path):
        self.db_path = db_path
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        cols = {
            r["name"] for r in self.conn.execute(
                "PRAGMA table_info(cycle_shares)"
            ).fetchall()
        }
        if "reason" not in cols:
            self.conn.execute(
                "ALTER TABLE cycle_shares ADD COLUMN reason TEXT NOT NULL DEFAULT ''"
            )
        rcols = {
            r["name"] for r in self.conn.execute(
                "PRAGMA table_info(restake_txs)"
            ).fetchall()
        }
        if "staker_address" not in rcols:
            self.conn.execute(
                "ALTER TABLE restake_txs ADD COLUMN staker_address TEXT NOT NULL DEFAULT ''"
            )
        self.conn.commit()

    def close(self):
        self.conn.close()

    # ---- meta ----

    def set_meta(self, key, value):
        self.conn.execute(
            "INSERT INTO meta(k, v) VALUES(?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, str(value)),
        )
        self.conn.commit()

    def get_meta(self, key):
        row = self.conn.execute(
            "SELECT v FROM meta WHERE k=?", (key,)
        ).fetchone()
        if row is None:
            return None
        return row["v"]

    # ---- validator snapshots ----

    def upsert_validator_snapshot(self, validator, fetched_at_ms, total_luna,
                                  num_stakers, deposit_luna):
        self.conn.execute(
            "INSERT OR REPLACE INTO validator_snapshots "
            "(validator, fetched_at_ms, total_luna, num_stakers, deposit_luna) "
            "VALUES (?, ?, ?, ?, ?)",
            (validator, fetched_at_ms, total_luna, num_stakers, deposit_luna),
        )
        self.conn.commit()

    def latest_validator_snapshot(self, validator):
        row = self.conn.execute(
            "SELECT * FROM validator_snapshots WHERE validator=? "
            "ORDER BY fetched_at_ms DESC LIMIT 1",
            (validator,),
        ).fetchone()
        return row

    # ---- staker snapshots ----

    def upsert_staker_snapshot(self, validator, address, fetched_at_ms,
                              balance_luna, inactive_luna):
        self.conn.execute(
            "INSERT OR REPLACE INTO staker_snapshots "
            "(validator, address, fetched_at_ms, balance_luna, inactive_luna) "
            "VALUES (?, ?, ?, ?, ?)",
            (validator, address, fetched_at_ms, balance_luna, inactive_luna),
        )
        self.conn.commit()

    def staker_snapshot_at(self, validator, address, at_ms):
        """Return the latest snapshot for (validator, address) at or before
        at_ms, or None."""
        row = self.conn.execute(
            "SELECT * FROM staker_snapshots WHERE validator=? AND address=? "
            "AND fetched_at_ms <= ? ORDER BY fetched_at_ms DESC LIMIT 1",
            (validator, address, at_ms),
        ).fetchone()
        return row

    def staker_balance_at(self, validator, address, at_or_before_ms):
        """Last snapshot balance for (validator, address) at or before
        at_or_before_ms, or None."""
        row = self.conn.execute(
            "SELECT balance_luna FROM staker_snapshots WHERE validator=? AND address=? "
            "AND fetched_at_ms <= ? ORDER BY fetched_at_ms DESC LIMIT 1",
            (validator, address, at_or_before_ms),
        ).fetchone()
        return int(row["balance_luna"]) if row else None

    def staker_balance_after(self, validator, address, after_ms):
        """First snapshot balance for (validator, address) strictly after
        after_ms, or None."""
        row = self.conn.execute(
            "SELECT balance_luna FROM staker_snapshots WHERE validator=? AND address=? "
            "AND fetched_at_ms > ? ORDER BY fetched_at_ms ASC LIMIT 1",
            (validator, address, after_ms),
        ).fetchone()
        return int(row["balance_luna"]) if row else None

    def staker_addresses_at(self, validator, at_or_before_ms):
        """Set of staker addresses present (snapshotted) at or before
        at_or_before_ms."""
        rows = self.conn.execute(
            "SELECT DISTINCT address FROM staker_snapshots WHERE validator=? "
            "AND fetched_at_ms <= ?",
            (validator, at_or_before_ms),
        ).fetchall()
        return {r["address"] for r in rows}

    def staker_addresses_after(self, validator, after_ms):
        """Set of staker addresses present (snapshotted) strictly after
        after_ms."""
        rows = self.conn.execute(
            "SELECT DISTINCT address FROM staker_snapshots WHERE validator=? "
            "AND fetched_at_ms > ?",
            (validator, after_ms),
        ).fetchall()
        return {r["address"] for r in rows}

    def latest_staker_rows(self, validator):
        """Latest snapshot per staker address, ordered by balance desc."""
        rows = self.conn.execute(
            "SELECT s1.* FROM staker_snapshots s1 "
            "JOIN (SELECT address, MAX(fetched_at_ms) AS m FROM staker_snapshots "
            "      WHERE validator=? GROUP BY address) s2 "
            "ON s1.address = s2.address AND s1.fetched_at_ms = s2.m "
            "WHERE s1.validator=? ORDER BY s1.balance_luna DESC",
            (validator, validator),
        ).fetchall()
        return rows

    # ---- rewards ----

    def upsert_reward(self, validator, block, amount_luna, ts_ms, tx_hash):
        self.conn.execute(
            "INSERT OR REPLACE INTO rewards "
            "(validator, block, amount_luna, ts_ms, tx_hash) "
            "VALUES (?, ?, ?, ?, ?)",
            (validator, block, amount_luna, ts_ms, tx_hash),
        )
        self.conn.commit()

    def rewards_in_window(self, validator, start_block, end_block):
        """Rewards with start_block < block <= end_block."""
        rows = self.conn.execute(
            "SELECT * FROM rewards WHERE validator=? AND block>? AND block<=? "
            "ORDER BY block ASC",
            (validator, start_block, end_block),
        ).fetchall()
        return rows

    def first_last_rewards(self, validator):
        row = self.conn.execute(
            "SELECT MIN(ts_ms) AS first_ts, MAX(ts_ms) AS last_ts, "
            "COUNT(*) AS n FROM rewards WHERE validator=?",
            (validator,),
        ).fetchone()
        return row

    def rewards_since(self, validator, since_block):
        rows = self.conn.execute(
            "SELECT * FROM rewards WHERE validator=? AND block>=? ORDER BY block ASC",
            (validator, since_block),
        ).fetchall()
        return rows

    # ---- cycles ----

    def create_cycle(self, validator, opened_block, opened_at_ms, total_luna,
                     reserve_luna):
        cur = self.conn.execute(
            "INSERT INTO cycles "
            "(validator, opened_block, opened_at_ms, total_luna, reserve_luna, status) "
            "VALUES (?, ?, ?, ?, ?, 'PENDING')",
            (validator, opened_block, opened_at_ms, total_luna, reserve_luna),
        )
        self.conn.commit()
        return cur.lastrowid

    def latest_pending_cycle(self, validator):
        row = self.conn.execute(
            "SELECT * FROM cycles WHERE validator=? AND status='PENDING' "
            "ORDER BY id DESC LIMIT 1",
            (validator,),
        ).fetchone()
        return row

    def latest_closed_cycle(self, validator):
        row = self.conn.execute(
            "SELECT * FROM cycles WHERE validator=? AND status IN "
            "('VERIFIED','MISMATCH') ORDER BY id DESC LIMIT 1",
            (validator,),
        ).fetchone()
        return row

    def close_cycle(self, cycle_id, closed_block, closed_at_ms, available_luna,
                    held_luna, status):
        self.conn.execute(
            "UPDATE cycles SET closed_block=?, closed_at_ms=?, available_luna=?, "
            "held_luna=?, status=? WHERE id=?",
            (closed_block, closed_at_ms, available_luna, held_luna, status, cycle_id),
        )
        self.conn.commit()

    def get_cycle(self, cycle_id):
        row = self.conn.execute(
            "SELECT * FROM cycles WHERE id=?", (cycle_id,)
        ).fetchone()
        return row

    def recent_cycles(self, validator, limit):
        rows, _ = self.recent_cycles_before(validator, limit, None)
        return rows

    def recent_cycles_before(self, validator, limit, before_id):
        """Latest `limit` cycles, optionally strictly older than `before_id`.
        Returns (rows, has_more) where has_more is True when older rows exist."""
        if before_id is not None:
            rows = self.conn.execute(
                "SELECT * FROM cycles WHERE validator=? AND id<? "
                "ORDER BY id DESC LIMIT ?",
                (validator, before_id, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM cycles WHERE validator=? "
                "ORDER BY id DESC LIMIT ?",
                (validator, limit),
            ).fetchall()
        # has_more is true if any cycle has a smaller id than the oldest shown.
        if not rows:
            return rows, False
        oldest = rows[-1]["id"]
        older = self.conn.execute(
            "SELECT 1 FROM cycles WHERE validator=? AND id<? LIMIT 1",
            (validator, oldest),
        ).fetchone()
        return rows, older is not None

    # ---- cycle shares ----

    def upsert_cycle_share(self, cycle_id, staker_address, expected_luna,
                           actual_luna, tx_hash, ok, reason=""):
        self.conn.execute(
            "INSERT OR REPLACE INTO cycle_shares "
            "(cycle_id, staker_address, expected_luna, actual_luna, tx_hash, ok, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (cycle_id, staker_address, expected_luna, actual_luna, tx_hash,
             ok, reason),
        )
        self.conn.commit()

    def clear_cycle_shares(self, cycle_id):
        self.conn.execute(
            "DELETE FROM cycle_shares WHERE cycle_id=?", (cycle_id,)
        )
        self.conn.commit()

    def cycle_shares(self, cycle_id):
        rows = self.conn.execute(
            "SELECT * FROM cycle_shares WHERE cycle_id=? ORDER BY staker_address ASC",
            (cycle_id,),
        ).fetchall()
        return rows

    # ---- staker rewards (G1 ledger) ----

    def upsert_staker_reward(self, staker_address, block, amount_luna, tx_hash,
                             ts_ms):
        self.conn.execute(
            "INSERT OR REPLACE INTO staker_rewards "
            "(staker_address, block, amount_luna, tx_hash, ts_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            (staker_address, block, amount_luna, tx_hash, ts_ms),
        )
        self.conn.commit()

    def credit_staker_reward(self, staker_address, block, amount_luna, tx_hash,
                             ts_ms):
        """One credit per (staker, cycle-closing block). Re-verification may
        upgrade a share's tx_hash from the boundary stand-in to the staker's
        real AddStake hash; a bare upsert would leave BOTH rows (the PK includes
        tx_hash), duplicating the credit on the ledger. Delete-then-insert keeps
        the credit singular and always carries the freshest hash."""
        self.conn.execute(
            "DELETE FROM staker_rewards WHERE staker_address=? AND block=?",
            (staker_address, block),
        )
        self.conn.execute(
            "INSERT INTO staker_rewards "
            "(staker_address, block, amount_luna, tx_hash, ts_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            (staker_address, block, amount_luna, tx_hash, ts_ms),
        )
        self.conn.commit()

    def staker_rewards(self, validator, address, limit):
        rows, _ = self.staker_rewards_before(validator, address, limit, None)
        return rows

    def staker_rewards_before(self, validator, address, limit, before_block):
        """Latest `limit` credits, optionally strictly older than `before_block`.
        Returns (rows, has_more) where has_more is True when older rows exist."""
        if before_block is not None:
            rows = self.conn.execute(
                "SELECT * FROM staker_rewards WHERE staker_address=? AND block<? "
                "ORDER BY block DESC LIMIT ?",
                (address, before_block, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM staker_rewards WHERE staker_address=? "
                "ORDER BY block DESC LIMIT ?",
                (address, limit),
            ).fetchall()
        if not rows:
            return rows, False
        oldest = rows[-1]["block"]
        older = self.conn.execute(
            "SELECT 1 FROM staker_rewards WHERE staker_address=? AND block<? LIMIT 1",
            (address, oldest),
        ).fetchone()
        return rows, older is not None

    # ---- restake txs ----

    def upsert_restake(self, validator, block, to_addr, amount_luna, ts_ms, tx_hash, staker_address=""):
        self.conn.execute(
            "INSERT OR REPLACE INTO restake_txs "
            "(validator, block, to_addr, amount_luna, ts_ms, tx_hash, staker_address) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (validator, block, to_addr, amount_luna, ts_ms, tx_hash, staker_address),
        )
        self.conn.commit()

    def first_restake_after(self, validator, block):
        row = self.conn.execute(
            "SELECT * FROM restake_txs WHERE validator=? AND block>? "
            "ORDER BY block ASC LIMIT 1",
            (validator, block),
        ).fetchone()
        return row

    def next_restake_batch(self, validator, after_block, gap_blocks):
        """Return (last_block, rows) for the first complete restake batch strictly
        after after_block, or None when there are no restakes beyond it.

        A batch is a run of restakes where consecutive blocks differ by at most
        gap_blocks; a larger gap (a new distribution event or a pause) starts a
        new batch. last_block is the block of the batch's final tx, which becomes
        the cycle's closed_block."""
        rows = self.conn.execute(
            "SELECT * FROM restake_txs WHERE validator=? AND block>? "
            "ORDER BY block ASC",
            (validator, after_block),
        ).fetchall()
        if not rows:
            return None
        batch = [rows[0]]
        last = int(rows[0]["block"])
        for row in rows[1:]:
            if int(row["block"]) - last > gap_blocks:
                break
            batch.append(row)
            last = int(row["block"])
        return last, batch

    def restakes_in_window(self, validator, start_block, end_block):
        """Restakes with start_block < block <= end_block (closing batch inclusive)."""
        rows = self.conn.execute(
            "SELECT * FROM restake_txs WHERE validator=? AND block>? AND block<=? "
            "ORDER BY block ASC",
            (validator, start_block, end_block),
        ).fetchall()
        return rows

    def restakes_for_staker(self, validator, start_block, end_block, staker):
        """Restakes in the window whose on-chain attribution names `staker`
        (via relatedAddresses at ingest). Empty string means the endpoint did
        not expose the mapping; the caller falls back to balance-delta math."""
        rows = self.conn.execute(
            "SELECT * FROM restake_txs WHERE validator=? AND block>? AND block<=? "
            "AND staker_address=? ORDER BY block ASC",
            (validator, start_block, end_block, staker),
        ).fetchall()
        return rows

    def max_restake_block(self, validator):
        row = self.conn.execute(
            "SELECT MAX(block) AS m FROM restake_txs WHERE validator=?",
            (validator,),
        ).fetchone()
        return int(row["m"]) if row and row["m"] is not None else 0

    # ---- epoch stats ----

    def upsert_epoch_stats(self, validator, epoch, start_block, end_block,
                           micro_total, produced, reward_count, reward_sum_luna,
                           updated_at_ms):
        self.conn.execute(
            "INSERT OR REPLACE INTO epoch_stats "
            "(validator, epoch, start_block, end_block, micro_total, produced, "
            "reward_count, reward_sum_luna, updated_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (validator, epoch, start_block, end_block, micro_total, produced,
             reward_count, reward_sum_luna, updated_at_ms),
        )
        self.conn.commit()

    def epoch_stats(self, validator, limit=20):
        rows = self.conn.execute(
            "SELECT * FROM epoch_stats WHERE validator=? "
            "ORDER BY epoch DESC LIMIT ?",
            (validator, limit),
        ).fetchall()
        return rows

    # ---- misc ----

    def _now_ms(self):
        return int(time.time() * 1000)
