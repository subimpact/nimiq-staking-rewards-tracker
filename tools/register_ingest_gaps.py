#!/usr/bin/env python3
"""Host-side CLI to scan the rewards table for gaps (>60 blocks) and register them.

Scans consecutive reward rows. If row_above.block - row_below.block > 60,
a hole is identified:
  cursor: tx_hash of the reward row just ABOVE the hole
  floor: block of the reward row just BELOW the hole

Holes with floors already present in meta ingest_gaps are skipped.
Creates the meta table if missing.
"""

import argparse
import json
import sqlite3
import sys


def register_ingest_gaps(db_path, dry_run=False):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
        conn.commit()

        # Check existing gaps
        row = conn.execute("SELECT v FROM meta WHERE k = 'ingest_gaps'").fetchone()
        existing_gaps = []
        if row and row["v"]:
            try:
                data = json.loads(row["v"])
                if isinstance(data, list):
                    existing_gaps = data
            except Exception:
                existing_gaps = []

        existing_floors = {g.get("floor") for g in existing_gaps if isinstance(g, dict)}

        # Scan rewards for gaps > 60 between consecutive reward rows
        # Order by block ASC so row i is below row i+1
        rewards = conn.execute(
            "SELECT block, tx_hash FROM rewards ORDER BY block ASC"
        ).fetchall()

        new_gaps = []
        for i in range(len(rewards) - 1):
            row_below = rewards[i]
            row_above = rewards[i + 1]
            gap_size = row_above["block"] - row_below["block"]
            if gap_size > 60:
                floor = int(row_below["block"])
                cursor = str(row_above["tx_hash"])
                if floor not in existing_floors:
                    hole = {"cursor": cursor, "floor": floor}
                    new_gaps.append(hole)
                    existing_floors.add(floor)

        all_gaps = list(existing_gaps) + new_gaps

        if dry_run:
            print(f"[dry-run] Found {len(new_gaps)} new gap(s) to register:")
            for g in new_gaps:
                print(f"  cursor={g['cursor']} floor={g['floor']}")
            return new_gaps

        if new_gaps:
            conn.execute(
                "INSERT INTO meta(k, v) VALUES('ingest_gaps', ?) "
                "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (json.dumps(all_gaps),),
            )
            conn.commit()
            print(f"Registered {len(new_gaps)} gap(s) into ingest_gaps.")
        else:
            print("No new gaps to register.")

        return new_gaps
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scan rewards table for block gaps > 60 and register ingest gaps."
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument(
        "--dry-run", action="store_true", help="Scan and report gaps without saving to DB"
    )
    args = parser.parse_args()

    register_ingest_gaps(args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
