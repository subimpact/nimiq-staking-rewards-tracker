# nimiq-staking-rewards-tracker: Spec

**Status:** v1.1 (2026-10-19). Owner: subimpact. Supersedes the v1.0 build spec of 2026-09-17. The verification math is documented in full in [README.md](README.md) ("G2: the verification math"); this file holds the architecture, schema and API contract.

**Mission (two community-verified gaps):**
- **G1 (issue #3170):** a staking-rewards tracker validators can deploy. Per-staker rewards ledger: stake, share, every credited reward with its own tx hash, compounding history.
- **G2 (issue #3171):** reward distribution integrity. For every payout cycle, recompute each staker's expected share from chain data and compare it with the actual on-chain AddStake transactions. Emit VERIFIED, MISMATCH or SKIPPED with the per-staker math stored.

Reference deployment: ImpactZero (NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY), feeding nimiq.subimpact.net `/staking` (G1) and `/verify` (G2, public).

## Architecture

- Single container, no external Python dependencies (stdlib only). Python 3.11+, base image `python:3.11-slim`, non-root user.
- `tracker/main.py` starts a read-only HTTP API thread on `0.0.0.0:8649` inside the container (compose publishes it on `127.0.0.1:8649` only) and a scheduler that runs four jobs every `POLL_SECONDS` (60). Each job runs in its own error boundary; per-job status is exposed in `/api/health`. `SIGTERM` shuts down gracefully.
- Data: SQLite at `DATA_DIR/tracker.db`, WAL mode, idempotent upserts. One connection per thread (the scheduler writes; each API request reads on its own connection and closes it), busy timeout 5 s.
- Data sources (public, read-only; the tracker never touches a validator node or its keys):
  - `RPC_URL` (default `https://rpc.nimiqwatch.com`): `getTransactionsByAddress` on the reward address, params `[address, 200, cursor_hash]`, newest first.
  - `STAKERS_URL_TEMPLATE`: staker records (`{data: [{address, balance, inactiveBalance, ...}]}`).
  - `VALIDATORS_URL_TEMPLATE`: validator list; the row whose address matches `VALIDATOR_ADDR` is used, and no validator snapshot is written if it is absent.
- Thresholds mirror the ImpactZero restake bot: `MIN_SHARE_LUNA=500`, `DUST_TOLERANCE_LUNA=500`, `RESERVE_LUNA=0` (informational, never deducted), `BATCH_GAP_BLOCKS=10`. Distribution rule: `VALIDATOR_FEE_PPM=0`, `DENOMINATOR_MODE=stakers`, `PAYOUT_MODE=restake`, `OPERATOR_ADDRS` empty. Carry rule (M1): `CARRY_MAX_BATCHES=1`, `CARRY_DUST_LUNA=10000`, `SHARE_TOLERANCE_PPM=10`; see README "Check 1: the carried balance". Retention: `SNAPSHOT_RETENTION_DAYS=0` (off).

## SQLite schema

```sql
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
  -- keys: ingest_watermark_block, ingest_gaps (JSON list of {cursor, floor})
CREATE TABLE validator_snapshots (
  validator TEXT, fetched_at_ms INTEGER, total_luna INTEGER, num_stakers INTEGER, deposit_luna INTEGER
);
CREATE TABLE staker_snapshots (
  validator TEXT, address TEXT, fetched_at_ms INTEGER, balance_luna INTEGER, inactive_luna INTEGER,
  PRIMARY KEY (validator, address, fetched_at_ms)
);
CREATE TABLE rewards (
  validator TEXT, block INTEGER, amount_luna INTEGER, ts_ms INTEGER, tx_hash TEXT,
  PRIMARY KEY (validator, block)
);
CREATE TABLE restake_txs (
  validator TEXT, block INTEGER, to_addr TEXT, amount_luna INTEGER, ts_ms INTEGER, tx_hash TEXT,
  staker_address TEXT NOT NULL DEFAULT '',   -- credited staker, from relatedAddresses
  PRIMARY KEY (validator, block, to_addr, tx_hash)
);
CREATE TABLE cycles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  validator TEXT, opened_block INTEGER, closed_block INTEGER,
  available_luna INTEGER, reserve_luna INTEGER, held_luna INTEGER DEFAULT 0, total_luna INTEGER,
  status TEXT CHECK (status IN ('PENDING','VERIFIED','MISMATCH','SKIPPED')),
  opened_at_ms INTEGER, closed_at_ms INTEGER,
  attribution TEXT NOT NULL DEFAULT '',      -- 'tx' | 'delta' | '' (degenerate SKIPPED)
  note TEXT NOT NULL DEFAULT '',             -- why a cycle was SKIPPED, or "carry baseline established"
  carry_luna INTEGER,                        -- undistributed balance owed to stakers after this cycle (NULL before M1)
  fee_carry_luna INTEGER                     -- operator entitlement not yet withdrawn
);
CREATE TABLE cycle_shares (
  cycle_id INTEGER REFERENCES cycles(id),
  staker_address TEXT, expected_luna INTEGER, actual_luna INTEGER, tx_hash TEXT, ok INTEGER,
  reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (cycle_id, staker_address)
);
CREATE TABLE staker_rewards (     -- G1 ledger, one credit per staker per closing block
  validator TEXT NOT NULL DEFAULT '',        -- '' only on rows written before 2026-10-19
  staker_address TEXT, block INTEGER, amount_luna INTEGER, tx_hash TEXT, ts_ms INTEGER,
  PRIMARY KEY (staker_address, block, tx_hash)
);
CREATE TABLE epoch_stats (
  validator TEXT, epoch INTEGER, start_block INTEGER, end_block INTEGER, micro_total INTEGER,
  produced INTEGER, reward_count INTEGER, reward_sum_luna INTEGER, updated_at_ms INTEGER,
  PRIMARY KEY (validator, epoch)
);
```

Migrations are additive (`ALTER TABLE ... ADD COLUMN`; the carry columns are added after the CHECK rebuild so a rebuild cannot drop them) except the `cycles` CHECK constraint, which is rebuilt when an older database lacks `SKIPPED`.

## Jobs

1. **snapshot**: validator record plus every staker's active balance, time-stamped. A staker that vanishes from a non-empty list gets one zero-balance tombstone row; an empty list tombstones nobody.
2. **ingest**: head walk from the newest tx back to `ingest_watermark_block` (at most 3 pages of 200). If the walk stops short (page cap or HTTP 429), the uncovered stretch is recorded in `ingest_gaps` and drained newest-first at `BACKFILL_PAGES_PER_POLL` pages per poll. Coinbase txs to the reward address become `rewards`; outbound txs from the reward address become `restake_txs` with the credited staker. Address comparison ignores spacing and case.
3. **cycle close** (the only closer): window = (opened_block, closed_block], where closed_block ends the first complete payout batch after the opener (a gap above `BATCH_GAP_BLOCKS` splits batches). Up to `MAX_CLOSE_PER_TICK` cycles per poll during catch-up. Verdict math: README.
4. **verify**: idempotent re-credit of the latest closed cycle into `staker_rewards`. It never closes or modifies a PENDING cycle.

## API (read-only, GET, JSON; errors `{"error": "..."}`)

- `GET /api/health` -> `{ok, db, last_run_ms, stale, jobs, data_age_s}`
- `GET /api/validators/{addr}/summary`
- `GET /api/validators/{addr}/stakers`
- `GET /api/validators/{addr}/cycles?limit=50&before={id}` -> `{cycles, has_more}`; each cycle includes `attribution` and `note`
- `GET /api/validators/{addr}/cycles/{cycle_id}/shares` -> `{cycle, shares}`; shares carry `expected_luna, actual_luna, ok, reason, tx_hash`
- `GET /api/validators/{addr}/stakers/{address}/rewards?limit=50&before={block}` -> `{rewards, has_more}`
- `GET /api/validators/{addr}/epochs`
- `GET /api/verify?limit=50&before={id}` -> `{cycles, has_more}`

`limit` is clamped to 1..500; `before` cursors are not clamped (block numbers are ~64M). Query values are percent-decoded. Unknown routes and cycles that do not exist under `{addr}` return 404; internal failures return 500 `{"error": "internal error"}`.

## Repo layout

```
tracker/main.py        # entrypoint: API thread + scheduler with per-job isolation
tracker/db.py          # schema, migrations, queries (WAL)
tracker/jobs.py        # snapshot / ingest (+gap backfill) / cycle close / verify
tracker/api.py         # http.server handler and health status
tracker/config.py      # env parsing, defaults
tools/register_ingest_gaps.py  # register historic reward-history holes for backfill
tools/epoch_stats.py   # reference-deployment epoch production counter
tests/                 # stdlib unittest; fake explorer/staker servers mirror live shapes
```

## Constraints

- `python3 -m unittest discover tests` passes before any release.
- No pip installs; no network beyond the three configured sources; no writes outside `DATA_DIR`.
- Public repo: zero secrets. README copy: no em-dashes; formulas and thresholds must match the code.
