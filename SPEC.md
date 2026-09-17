# nimiq-staking-rewards-tracker — Build Spec

**Status:** v1.0 spec (2026-09-17). Owner: subimpact. Build lane: ocode (opencode, ollama-cloud/deepseek-v4-flash:0731).

**Mission (two community-verified gaps):**
- **G1 (issues #3170):** standard staking-rewards tracking web interface validators can deploy on their node. Per-staker rewards ledger: stake, share, every credited reward with tx hash, compounding history, projected yield.
- **G2 (issues #3171):** validator reward transparency / integrity verification. For every distribution cycle, recompute each staker's expected share from chain state and compare against the actual on-chain restake transactions. Emit VERIFIED or MISMATCH with the math visible.

Reference deployment: ImpactZero (NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY), feeds nimiq.subimpact.net `/staking` (G1, sign-in gated via existing Nimiq Pay/Hub flow) and `/verify` (G2, public).

## Architecture

- Single container, **no external Python dependencies** (stdlib only: `sqlite3`, `http.server`, `urllib.request`, `json`, `threading`, `argparse`, `time`, `signal`).
- Language: **Python 3.11+**. Base image `python:3.11-slim`.
- Entry: `tracker/main.py` → starts a read-only HTTP API thread (`http.server`) bound `0.0.0.0:8649` (container) + a scheduler loop every 60s running the four jobs (below). `SIGTERM` graceful shutdown.
- Data: SQLite at `DATA_DIR/tracker.db` (default `/data`), WAL mode. Idempotent upserts, dedup by tx hash and block number.
- **Data sources (all public/read-only, zero production-node changes):**
  - `RPC_URL` (default `https://rpc.nimiqwatch.com`) — `getTransactionsByAddress` on the reward address for coinbases + restake txs (JSON-RPC POST, params `[address, 200, null]`).
  - `STAKERS_URL_TEMPLATE` (default `https://nimiq-api.subimpact.net/api/stakers/{address}`) — latest staker records (`{data: [{address, balance, delegation, inactiveBalance, ...}]}`). Configurable so other validators can point at their own API.
  - `VALIDATORS_URL_TEMPLATE` (default `https://nimiq-api.subimpact.net/api/validators`) — validator record for total stake + staker count.
  - Optional `LOG_FILE` (e.g. host-mounted `/logs/restake.log`, read-only) for cycle boundary markers from the operator's restake bot; purely informational — verification data comes from the chain.
- Config via env vars (see README section below); all thresholds mirror the ImpactZero restake bot: `MIN_TRIGGER_LUNA=2000`, `MIN_SHARE_LUNA=500`, `RESERVE_LUNA=100000` (wallet floor, informational only), `DUST_TOLERANCE_LUNA=500`, `REWARD_ADDR`, `VALIDATOR_ADDR`.

## SQLite schema

```sql
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
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
CREATE TABLE cycles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  validator TEXT, opened_block INTEGER, closed_block INTEGER,
  available_luna INTEGER, reserve_luna INTEGER, held_luna INTEGER DEFAULT 0, total_luna INTEGER,
  status TEXT CHECK (status IN ('PENDING','VERIFIED','MISMATCH')),
  opened_at_ms INTEGER, closed_at_ms INTEGER
);
CREATE TABLE cycle_shares (
  cycle_id INTEGER REFERENCES cycles(id),
  staker_address TEXT, expected_luna INTEGER, actual_luna INTEGER, tx_hash TEXT, ok INTEGER,
  PRIMARY KEY (cycle_id, staker_address)
);
CREATE TABLE staker_rewards (     -- G1 ledger: what the dashboard shows
  staker_address TEXT, block INTEGER, amount_luna INTEGER, tx_hash TEXT, ts_ms INTEGER,
  PRIMARY KEY (staker_address, block, tx_hash)
);
CREATE INDEX idx_staker_rewards_addr ON staker_rewards(staker_address, ts_ms);
```

## Jobs (scheduler loop, 60s)

1. **snapshot**: fetch stakers + validator total; upsert latest row per (validator, address). Keeps history for share recomputation.
2. **rewards ingest**: `getTransactionsByAddress(reward_addr)` via `RPC_URL`; classify inbound:
   - from coinbase (`NQ81 C01N BASE ...`) → `rewards` row;
   - from the validator reward address to the staking contract (`NQ77 0000 ... 0000 0001`) → restake tx (record in a `restake_txs`-style table if present; at minimum used in job 4);
   - anything else → ignore (sentinel domain of the bot; not ours).
   Dedup by block/hash.
3. **cycle close**: a "cycle" = the window between consecutive restake batches from the reward wallet. When new restake txs are seen (and the previous window has a `PENDING` cycle), close it: `available_luna` = rewards inside window − `HELD` (`RESERVE_LUNA` is a wallet safety floor, not a per-cycle deduction; recorded for reference only); `total_luna` = validator snapshot at window open; recompute shares with **job 4**, set `VERIFIED`/`MISMATCH`.
4. **verify (G2)**: for each staker snapshot taken at window open:
   `expected_i = floor(balance_i * available / total)` (integer lunas, deterministic);
   `actual_i` = sum of restake tx values to that staker inside the window.
   `ok = 1` when `actual_i == expected_i`, OR (`expected_i < MIN_SHARE_LUNA` AND `actual_i == 0`) (dust floor skips it), OR `abs(actual_i - expected_i) <= 1` (rounding tolerance).
   Two independent consistency checks determine cycle status beyond the per-line `ok`:
   - internal drift: `abs(sum_txs - (sum_deltas + unaccounted)) > 1` → MISMATCH, reason "unaccounted restake value (possible timing drift)";
   - honesty vs reward income: `abs(sum_txs - sum_rewards_in_window) > DUST_TOLERANCE_LUNA` → MISMATCH, reason "distribution does not match cycle rewards (possible skim)".
   Cycle status `VERIFIED` iff every line `ok` and neither consistency check fires; else `MISMATCH` (per-line diff stored in `cycle_shares.expected/actual`).
   Every verified/mismatched cycle lands in `staker_rewards` (the credited amounts) for G1.

## API (read-only, GET, JSON; errors `{"error": "..."}`)

- `GET /api/health` → `{ok, db, last_run_ms}`
- `GET /api/validators/{addr}/summary` → latest snapshot + counts + first/last rewards
- `GET /api/validators/{addr}/stakers` → latest staker rows
- `GET /api/validators/{addr}/cycles?limit=50` → cycles with status + totals
- `GET /api/validators/{addr}/cycles/{cycle_id}/shares` → per-staker expected/actual/ok
- `GET /api/validators/{addr}/stakers/{address}/rewards?limit=50` → G1 ledger rows
- `GET /api/verify?limit=50` → recent cycles with status (for `/verify` page)

## Repo layout

```
tracker/main.py        # entrypoint: API thread + scheduler
tracker/db.py          # schema, connection (WAL), upserts
tracker/jobs.py        # snapshot / ingest / cycle-close / verify
tracker/api.py         # http.server handler (routes above)
tracker/config.py      # env parsing, defaults
Dockerfile
docker-compose.yml     # ports: "127.0.0.1:8649:8649"; volumes ./data:/data; restart unless-stopped
tests/test_verify.py   # unittest (stdlib), fake RPC server (http.server) with canned fixtures
tests/test_api.py      # API route tests with a temp sqlite
README.md              # quickstart, env table, API doc, G1/G2 explainer, ImpactZero reference
LICENSE                # MIT
```

## Constraints & acceptance

- `python -m unittest discover tests -v` must pass before finishing.
- No pip installs; no runtime network beyond RPC_URL/STAKERS/VALIDATORS; no writes to anything but DATA_DIR.
- `docker build` must succeed from repo root; compose file must be valid (validate with `docker compose config`).
- All rendered copy in the repo README: no tab characters, no em-dashes (house style), no table syntax that breaks on narrow clients (use list lines).
- Keep the README honest: it powers a public validator with real stakes; exact formula + exact thresholds must match the code constants.
- Repo is PUBLIC from creation; zero secrets anywhere (no keys, no tokens).
