# Nimiq Staking Rewards Tracker

Standard staking-rewards tracking (G1) plus on-chain integrity verification (G2) for Nimiq Albatross validators that pay stakers by restaking. Runs entirely from the Python 3.11 standard library inside a single container, exposes a read-only HTTP API, and writes nothing but a local SQLite database under the mounted data directory.

Answers the two community gaps from [core-rs-albatross #3170](https://github.com/nimiq/core-rs-albatross/issues/3170) and [#3171](https://github.com/nimiq/core-rs-albatross/issues/3171):

- G1: a per-staker rewards ledger (stake, pool share, every credited reward with its own AddStake tx hash, compounding history).
- G2: distribution integrity. Every payout cycle is recomputed from chain data and compared with the actual on-chain AddStake transactions. Each cycle is labelled VERIFIED, MISMATCH or SKIPPED and the per-staker math is stored line by line.

Reference deployment: the ImpactZero validator (0% fee, NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY). Live UI at nimiq.subimpact.net.

Read [SPEC.md](SPEC.md) for the schema and API contract.

## Quickstart

```sh
docker compose up -d --build
curl http://127.0.0.1:8649/api/health
```

The API is bound to 127.0.0.1 on the host and only exposes 8649 inside. State is stored in `./data/tracker.db` on the host. The container runs as a non-root user.

## How it works

The process starts two things:

1. A read-only HTTP API thread on 0.0.0.0:8649 (inside the container).
2. A scheduler loop that runs four jobs every 60 seconds. Each job runs in isolation: a failure in one (for example a slow staker API) is logged and reported in `/api/health`, and the other jobs still run.

### The four jobs

- snapshot: fetches the validator record and every staker's active balance and stores a time-stamped row for each. These opening balances are the weights for the expected shares.
- ingest: reads the reward address history from the public explorer (`getTransactionsByAddress`, newest first, hash-cursor pagination). Coinbase rewards become `rewards` rows; outbound transactions from the reward address become `restake_txs` rows, each carrying the staker it credits (the explorer's `relatedAddresses`). After any outage the head walk reads back to the previous high-water mark; if the outage was longer than the per-poll page budget, the remainder is registered as a gap and drained a few pages per poll until it is closed, so no outage loses data.
- cycle close: a cycle is the window between consecutive payout batches (a batch is a run of AddStake txs whose blocks are at most `BATCH_GAP_BLOCKS` apart). When the next complete batch exists, the open window is verified and closed. This is the only code path that closes cycles.
- verify: re-credits the latest closed cycle into the G1 ledger (idempotent).

### G2: the verification math

For one closed window (opened_block, closed_block], all values integer lunas (1 NIM = 100,000 lunas):

- income = sum of coinbase rewards in the window.
- available = income - held. `held` is 0 unless the operator's bot is holding an inbound mis-send. `RESERVE_LUNA` is NOT deducted; it is recorded for reference only.
- distributable = available - floor(available * VALIDATOR_FEE_PPM / 1,000,000). The default fee is 0, so distributable = available.
- denominator = sum of all stakers' active balances at window open (`DENOMINATOR_MODE=stakers`, the default). The validator's own deposit is excluded because the reference bot distributes the deposit's earned slice to stakers. With `DENOMINATOR_MODE=total` the denominator is the validator's total stake at open, deposit included, so the deposit's slice stays with the operator.
- expected_i = floor(balance_i * distributable / denominator).
- actual_i = sum of the window's AddStake transactions that name staker i in `relatedAddresses` ("tx" attribution). For older history where the explorer did not expose that mapping, actual_i falls back to the change in the staker's active balance across the window ("delta" attribution). Each cycle records which mode it used.

A staker line is OK when any of these hold:

- actual_i equals expected_i, or differs by at most 1 luna (integer flooring);
- actual_i exceeds expected_i by at most `DUST_TOLERANCE_LUNA` (500 luna = 0.005 NIM): the bot sweeps a few lunas of dust and its live staker view lags the snapshots slightly;
- expected_i is below `MIN_SHARE_LUNA` and actual_i is 0.

Any underpayment of more than 1 luna fails the line. An overpayment above the dust tolerance fails with "credit exceeds expected share". A payee with no opening balance (joined during the window) gets its own failing line.

Two cycle-level checks run on top:

- skim check: if the restaked total differs from what the rule says stakers are owed (the window's income minus the fee, scaled by stakers' share of the denominator in `total` mode) by more than `DUST_TOLERANCE_LUNA`, the cycle is MISMATCH ("distribution does not match cycle rewards").
- drift check: the attributed credits must account for the restaked total within 1 luna, otherwise MISMATCH ("unaccounted restake value"). When both checks fire, both reasons are recorded.

Status:

- VERIFIED: every line OK and neither cycle check fires.
- MISMATCH: anything else. Per-line expected and actual are stored in `cycle_shares` with a reason.
- SKIPPED: the window cannot be judged. Either it is shorter than one reward interval (60 blocks) yet contains payouts (its funding income sits in the neighbouring window), or, in delta mode only, a staker has no balance snapshot inside the window and none can arrive any more. SKIPPED cycles carry a note, credit nothing and are never counted as verified.

Reading a MISMATCH: the reference bot pays about one minute after rewards land, so a window boundary can fall between a reward and the payout it funds. That shows up as two neighbouring cycles that each miss by about one payout and reconcile as a pair. It is flagged, never hidden; the per-line data lets anyone check the pair.

G1 is the ledger: every OK staker line from a closed cycle is credited into `staker_rewards`, one row per staker per cycle, carrying the staker's own AddStake tx hash.

### Worked example

A window earns one coinbase of 2,000,000 lunas (20 NIM); held = 0, so available = 2,000,000.

At window open three stakers hold:

- A: 40,000,000 lunas (400 NIM)
- B: 30,000,000 lunas (300 NIM)
- C: 30,000,000 lunas (300 NIM)

denominator = 100,000,000 lunas.

- expected_A = floor(40,000,000 * 2,000,000 / 100,000,000) = 800,000 lunas (8 NIM)
- expected_B = floor(30,000,000 * 2,000,000 / 100,000,000) = 600,000 lunas (6 NIM)
- expected_C = 600,000 lunas (6 NIM)

If the closing batch contains AddStake txs naming A, B and C for exactly 800,000, 600,000 and 600,000 lunas, every line is OK, the batch total equals the income, and the cycle is VERIFIED. If B received 590,000, B's line fails (underpaid by 10,000) and so does the skim check (the batch is 10,000 short of the income): MISMATCH.

### Scope and limits

- Built for validators that pay stakers by restaking (AddStake) in proportion to stake, as the open-source ImpactZero restake bot does. A percentage fee (`VALIDATOR_FEE_PPM`) and a deposit-retaining split (`DENOMINATOR_MODE=total`) are supported; other rules (minimum payouts, tiered fees, payout by plain transfer) are not supported yet.
- Staker balances come from `STAKERS_URL_TEMPLATE` (keyed on the validator address). The default is the reference operator's API mirror; point it at any source you trust.
- A staker who vanishes from the staker list is recorded as fully unstaked only when the list is complete (at least as many rows as the validator record's staker count), so a truncated answer never books a fake unstake.

## Environment variables

Listed with their defaults. Thresholds mirror the ImpactZero restake bot.

- RPC_URL: JSON-RPC endpoint for `getTransactionsByAddress`. Default: https://rpc.nimiqwatch.com
- STAKERS_URL_TEMPLATE: URL template for staker records, with {address} substituted. Default: https://nimiq-api.subimpact.net/api/stakers/{address}
- VALIDATORS_URL_TEMPLATE: URL for the validator list. Default: https://nimiq-api.subimpact.net/api/validators
- VALIDATOR_ADDR: the validator being tracked. Default: NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY
- REWARD_ADDR: the reward wallet that receives coinbases and sends restakes. Default: same as VALIDATOR_ADDR
- MIN_TRIGGER_LUNA: the bot's restake trigger threshold, informational. Default: 2000
- MIN_SHARE_LUNA: shares below this with no payout are OK. Default: 500
- DUST_TOLERANCE_LUNA: allowed overpayment per line and allowed batch-vs-income difference. Default: 500
- RESERVE_LUNA: the bot's wallet floor, recorded for reference, never deducted. Default: 0
- BATCH_GAP_BLOCKS: a larger block gap between payout txs starts a new batch. Default: 10
- MAX_CLOSE_PER_TICK: cycles closed per poll when catching up a backlog. Default: 600
- BACKFILL_PAGES_PER_POLL: explorer pages spent per poll draining registered ingest gaps. Default: 3
- VALIDATOR_FEE_PPM: the operator's fee in parts per million of window income (50000 = 5%). Default: 0
- DENOMINATOR_MODE: `stakers` (deposit slice paid to stakers) or `total` (deposit slice kept by the operator). Default: stakers
- SNAPSHOT_RETENTION_DAYS: when above 0, staker snapshots older than this many days are thinned to one per staker per hour, once a day. Cycles older than the cutoff then cannot be re-derived at minute precision. Default: 0 (keep everything)
- DATA_DIR: directory for tracker.db. Default: /data
- PORT: API port inside the container. Default: 8649
- POLL_SECONDS: scheduler interval. Default: 60
- LOG_FILE: optional read-only bot log, informational only. Default: empty

## API endpoints

All GET, all read-only, all JSON. Errors are returned as {"error": "..."}: 404 for unknown routes and for a cycle that does not exist under the given validator, 500 with a generic message for internal failures (details go to the server log only). Nimiq addresses contain spaces, so percent-encode them in the URL path (for example NQ08%20ACT8...).

- GET /api/health -> {ok, db, last_run_ms, stale, jobs, data_age_s}. `ok` is false when the database is unreachable or the scheduler has not completed a tick for 5 poll intervals. `data_age_s` gives the age in seconds of the newest reward, restake and snapshot.
- GET /api/validators/{addr}/summary -> latest snapshot, staker count, staked total, first and last reward
- GET /api/validators/{addr}/stakers -> latest snapshot per staker
- GET /api/validators/{addr}/cycles?limit=50&before={id} -> recent cycles with status, attribution mode and note; `has_more` for paging
- GET /api/validators/{addr}/cycles/{cycle_id}/shares -> per-staker expected, actual, ok, reason and tx hash
- GET /api/validators/{addr}/stakers/{address}/rewards?limit=50&before={block} -> the G1 ledger for one staker
- GET /api/verify?limit=50&before={id} -> recent cycles with status, for the /verify page

## Tools

- tools/register_ingest_gaps.py --db PATH [--dry-run]: finds holes in the stored reward history and registers them for the ingest job to backfill.
- tools/epoch_stats.py: host-side epoch production counter for the reference deployment (reads block producers through the operator's own node).

## Development

Run the tests (stdlib unittest only, no installs):

```sh
python3 -m unittest discover tests -v
```

The tests drive the real fetcher against local fake explorer and staker servers that mirror the live response shapes (spaced addresses, `relatedAddresses`, hash-cursor pagination, HTTP 429).

## License

MIT. See [LICENSE](LICENSE).
