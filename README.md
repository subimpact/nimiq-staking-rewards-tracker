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

For one closed window (opened_block, closed_block], all values integer lunas (1 NIM = 100,000 lunas). A window ends on the last transaction of a payout batch.

What the window earned:

- income = sum of coinbase rewards in the window; available = income - held. `held` is 0 unless the operator's bot is holding an inbound mis-send. `RESERVE_LUNA` is NOT deducted; it is recorded for reference only.
- owed = what the rule says stakers get from that income: available minus the operator fee (`VALIDATOR_FEE_PPM`, default 0), scaled by the stakers' share of the denominator when `DENOMINATOR_MODE=total` (the deposit's slice then stays with the operator). With the defaults, owed = available.

What left the reward wallet:

- every outbound transaction in the window is one of: a staker payout, an operator fee withdrawal (recipient in `OPERATOR_ADDRS`), or a stray payment. Which outbound kind counts as a payout is `PAYOUT_MODE`: `restake` (AddStake to the staking contract, the default and the ImpactZero bot), `transfer` (plain transfer to the staker's own address) or `any`. A stray payment always fails the cycle with its own line.
- actual_i = sum of the window's payouts that name staker i: for a restake the staker comes from the explorer's `relatedAddresses`, for a transfer it is the recipient ("tx" attribution). For older history where the explorer did not expose the mapping, actual_i falls back to the change in the staker's active balance across the window ("delta" attribution). Each cycle records which mode it used.

#### Check 1: the carried balance

The reference bot sweeps its wallet about one minute after a coinbase lands, so a window boundary regularly falls between a reward and the payout that distributes it: one window holds the income, the next the payout. Instead of demanding that each window pays out exactly its own income, the verifier follows the undistributed balance from window to window:

- carry_out = carry_in + owed - paid_out (carry_in is the previous cycle's carry_out).
- carry_out below -`CARRY_DUST_LUNA` (default 10,000 = 0.1 NIM): more was paid out than was ever earned. MISMATCH ("payout exceeds rewards earned plus carried balance"); the carry restarts at 0.
- carry_out above `CARRY_MAX_BATCHES` x the largest of the last 10 coinbases + `CARRY_DUST_LUNA` (default 1 batch): rewards are being withheld. MISMATCH ("rewards withheld ... (possible skim)"). A skim that stays perfectly proportional cannot hide: the carry grows every window until it crosses the limit, and it stays flagged until the money is paid out.
- `CARRY_MAX_BATCHES=0` restores the strict rule: every window must pay out its own income.
- The very first cycle has no baseline (the wallet may hold income earned before tracking began). A shortfall there establishes the baseline instead of failing (note "carry baseline established").
- Fees: with `OPERATOR_ADDRS` set, the operator's entitlement (fee plus, in `total` mode, the deposit slice) is carried the same way, and a withdrawal beyond it fails ("operator withdrew more than its fee entitlement").

The carry of every cycle is stored (`cycles.carry_luna`, `cycles.fee_carry_luna`) so anyone can re-add the chain.

#### Check 2: each staker's share of what was paid

- weights = each staker's active balance at window open (the validator's own deposit is not a staker).
- D = what the stakers present at open received in the window. Stakers whose share floors below `MIN_SHARE_LUNA` and who got nothing (skipped dust) and stakers who unstaked mid-window leave the weight set.
- pot = D, unless the median of the shares that can be measured precisely (>= 10,000 luna) implies a pot more than `DUST_TOLERANCE_LUNA` away from D. Then the median pot is used, so that one shorted staker is the one flagged rather than everyone else.
- expected_i = floor(weight_i x pot / sum of weights).

A line is OK when actual_i equals expected_i within 1 luna (integer flooring), or exceeds it by at most `DUST_TOLERANCE_LUNA` (500), or falls short by at most the larger of `SHARE_TOLERANCE_PPM` (10 parts per million of the share: the bot's live staker view lags the snapshots by a few lunas) and `SHARE_TOLERANCE_LUNA` (50 luna = 0.0005 NIM), or expected_i is below `MIN_SHARE_LUNA` and actual_i is 0. Otherwise it fails with "paid less than stake share of this payout" or "credit exceeds expected share". A payee with no opening balance (joined during the window) gets its own failing line.

A drift check runs on top: the attributed credits plus any unattributed value must equal everything that left the wallet within 1 luna ("unaccounted restake value").

Status:

- VERIFIED: every line OK and no cycle check fires.
- MISMATCH: anything else. Per-line expected and actual are stored in `cycle_shares` with a reason.
- SKIPPED: the window cannot be judged. Either it is shorter than one reward interval (60 blocks), has no income, and the carried balance cannot fund its payouts, or, in delta mode only, a staker has no balance snapshot inside the window and none can arrive any more. SKIPPED cycles carry a note, credit nothing and are never counted as verified.

G1 is the ledger: every OK staker line from a closed cycle is credited into `staker_rewards`, one row per staker per cycle, carrying the staker's own payout tx hash.

### Worked example

At window open three stakers hold A 40,000,000, B 30,000,000 and C 30,000,000 lunas (weights sum 100,000,000). The previous cycle closed with carry 0.

Window 1 earns two coinbases of 2,000,000 lunas (owed 4,000,000), but the bot's sweep ran before the second one landed, so it pays 800,000 / 600,000 / 600,000 (paid_out 2,000,000).

- carry_out = 0 + 4,000,000 - 2,000,000 = 2,000,000: one batch, inside the limit (2,000,000 + 10,000).
- D = 2,000,000; expected = 800,000 / 600,000 / 600,000. Every line OK: VERIFIED.

Window 2 earns one coinbase (owed 2,000,000) and pays 1,600,000 / 1,200,000 / 1,200,000 (paid_out 4,000,000).

- carry_out = 2,000,000 + 2,000,000 - 4,000,000 = 0. VERIFIED. (Under a per-window rule both windows would have failed.)

If in window 2 B had received 1,190,000: the median pot stays 4,000,000, so A and C are OK and B's line fails (10,000 short): MISMATCH. If instead the bot kept paying everyone 90% of each coinbase, every line would stay proportional, but the carry would grow by 200,000 per window and cross the limit after about ten windows: MISMATCH from then on, until the withheld rewards are paid.

### Scope and limits

- Built for validators that pay stakers in proportion to stake, by restaking (AddStake) like the open-source ImpactZero restake bot or by plain transfer (`PAYOUT_MODE`). A percentage fee (`VALIDATOR_FEE_PPM`, with fee withdrawals to `OPERATOR_ADDRS`) and a deposit-retaining split (`DENOMINATOR_MODE=total`) are supported. Other rules (minimum payout amounts, tiered fees, time-weighted shares within a payout period) are not supported yet.
- Pools that pay less often than every batch (for example daily) need `CARRY_MAX_BATCHES` set to the batches in their payout period (1,440 per day).
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
- DUST_TOLERANCE_LUNA: allowed overpayment per staker line, and the median-pot switch threshold. Default: 500
- CARRY_MAX_BATCHES: largest undistributed balance allowed, in batches of income (0 = every window pays its own income). Default: 1
- CARRY_DUST_LUNA: slack on both sides of the carried balance. Default: 10000
- SHARE_TOLERANCE_PPM: allowed shortfall per staker line, in parts per million of the share. Default: 10
- SHARE_TOLERANCE_LUNA: allowed shortfall per staker line in lunas, whichever of the two is larger. Default: 50
- PAYOUT_MODE: `restake`, `transfer` or `any`: which outbound payments count as staker payouts. Default: restake
- OPERATOR_ADDRS: comma-separated operator wallets; outbound to them is a fee withdrawal checked against the fee rule. Default: empty
- RESERVE_LUNA: the bot's wallet floor, recorded for reference, never deducted. Default: 0
- BATCH_GAP_BLOCKS: a larger block gap between payout txs starts a new batch. Default: 10
- MAX_CLOSE_PER_TICK: cycles closed per poll when catching up a backlog. Default: 600
- BACKFILL_PAGES_PER_POLL: explorer pages spent per poll draining registered ingest gaps. Default: 3
- VALIDATOR_FEE_PPM: the operator's fee in parts per million of window income (50000 = 5%). Default: 0
- DENOMINATOR_MODE: `stakers` (deposit slice paid to stakers) or `total` (deposit slice kept by the operator). Default: stakers
- SNAPSHOT_RETENTION_DAYS: when above 0, staker snapshots older than this many days are thinned to one per staker per hour, once a day. Cycles older than the cutoff then cannot be re-derived at minute precision. Default: 0 (keep everything)
- ELECTION_FROM_EPOCH: when above 0, cycles in epochs >= this value weight each staker by min(stake in the epoch-start snapshot, live balance at cycle open): a staker who joined (or topped up) mid-epoch earns only on their epoch-start balance until the next epoch, and a deactivation still stops earning at once. Earlier epochs keep open-balance weights so re-deriving history stays correct. Default: 0 (off)
- ELECTION_SNAPSHOT_WINDOW_MS: how close to the epoch-start anchor a staker snapshot must be to count as the epoch-start balance. Default: 900000 (15 min)
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
- tools/epoch_stats.py: host-side epoch production counter for the reference deployment. It reads block producers through the operator's own node; `--rpc-cmd` sets how curl reaches the node. Every run continues the current epoch and finishes incomplete closed epochs within a batch budget.

## Development

Run the tests (stdlib unittest only, no installs):

```sh
python3 -m unittest discover tests -v
```

The tests drive the real fetcher against local fake explorer and staker servers that mirror the live response shapes (spaced addresses, `relatedAddresses`, hash-cursor pagination, HTTP 429).

## License

Copyright (c) 2026 subimpact network.

Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only). See [LICENSE](LICENSE).

You can run, study, modify and share the tracker freely, including running it for your own validator or stakers. If you run a modified version as a network service, you must offer the source of your modified version to its users.

Earlier releases, up to and including commit `31bb7a8`, were published under the MIT License and remain available under MIT. Everything after that commit is AGPL-3.0-only.
