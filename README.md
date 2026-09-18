# Nimiq Staking Rewards Tracker

Standard staking-rewards tracking (G1) plus on-chain integrity verification (G2) for Nimiq Albatross validators. Runs entirely from the Python 3.11 standard library inside a single container, exposes a read-only HTTP API, and writes nothing but a local SQLite database under the mounted data directory.

Answers the two community gaps from [core-rs-albatross #3170](https://github.com/nimiq/core-rs-albatross/issues/3170) and [#3171](https://github.com/nimiq/core-rs-albatross/issues/3171):

- G1: a per-staker rewards ledger (stake, pool share, every credited reward with its tx hash, compounding history).
- G2: distribution integrity. Every restake cycle is recomputed from chain state and compared with the actual on-chain restake transactions. Each cycle is labelled VERIFIED, MISMATCH or SKIPPED and the math is shown line by line.

Reference deployment: the ImpactZero validator (0% fee, NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY). Live UI at nimiq.subimpact.net.

Read [SPEC.md](SPEC.md) for the exact schema, job definitions and API contract.

## Quickstart

```sh
docker compose up -d --build
curl http://127.0.0.1:8649/api/health
```

The API is bound to 127.0.0.1 on the host and only exposes 8649 inside. State is stored in `./data/tracker.db` on the host. The container runs as a non-root user.

## How it works

The process starts two things:

1. A read-only HTTP API thread on 0.0.0.0:8649.
2. A scheduler loop that runs four jobs every 60 seconds.

### The four jobs

- snapshot: fetches the validator total and every staker's balance, and upserts a time-stamped row for each. Keeps history so shares can be recomputed at the moment a cycle opens.
- rewards ingest: calls `getTransactionsByAddress` on the reward address and classifies inbound transactions. Coinbase block rewards become `rewards` rows; outbound restakes from the reward wallet are recorded as restake transactions. Everything else is ignored.
- cycle close: a cycle is the window between consecutive restake batches. When a new restake batch appears, the previous window closes, its reward is distributed (minus reserve and held amounts) and the cycle is verified.
- verify (G2): recomputes each staker's expected share from chain state and compares it with the actual restake transactions in the window.

### G2 and G1

G2 is the verifier. For a closed cycle:

- available = sum of rewards in the window - RESERVE_LUNA - held.
- total = the validator's total stake at window open.
- expected_i = floor(balance_i * available / total), all in integer lunas.
- actual_i = sum of the restake values sent to that staker in the window.
- A staker line is OK when actual_i equals expected_i, or when expected_i is below MIN_SHARE_LUNA and actual_i is zero (the bot's integer floor can round a dust share to zero), or when abs(actual_i - expected_i) <= 1 (rounding tolerance).

The cycle is VERIFIED only when every staker line is OK; otherwise it is MISMATCH, with the per-line expected and actual stored in `cycle_shares`. A cycle is SKIPPED when its window is shorter than a block-reward interval (60 blocks) yet contains payouts: such a slice cannot contain the income that funds its payouts, so there is nothing to verify and no verdict is issued.

G1 is the ledger. Every OK staker line from a verified or mismatched cycle is credited into `staker_rewards`, which is what the dashboard shows. Mismatched stakers are not credited.

### Worked formula example

Take the constants from the code: RESERVE_LUNA = 100000, MIN_SHARE_LUNA = 500, 1 NIM = 100000 lunas.

A cycle earns a coinbase block reward of 2000000 lunas (20 NIM), so:

- available = 2000000 - 100000 = 1900000 lunas.

At window open the validator total is 100000000 lunas (1000 NIM), with three stakers:

- A: balance 40000000 lunas (400 NIM)
- B: balance 30000000 lunas (300 NIM)
- C: balance 30000000 lunas (300 NIM)

expected_A = floor(40000000 * 1900000 / 100000000) = 760000 lunas (7.6 NIM)
expected_B = floor(30000000 * 1900000 / 100000000) = 570000 lunas (5.7 NIM)
expected_C = floor(30000000 * 1900000 / 100000000) = 570000 lunas (5.7 NIM)

If the closing restake batch sends exactly 760000, 570000 and 570000 lunas, every line is OK and the cycle is VERIFIED. If any line differs by more than the tolerance, that line fails and the cycle is MISMATCH.

## Environment variables

Listed with their defaults. Every threshold mirrors the ImpactZero restake bot.

- RPC_URL: JSON-RPC endpoint for `getTransactionsByAddress`. Default: https://rpc.nimiqwatch.com
- STAKERS_URL_TEMPLATE: URL template for staker records, with {address} substituted. Default: https://nimiq-api.subimpact.net/api/stakers/{address}
- VALIDATORS_URL_TEMPLATE: URL template for the validator record. Default: https://nimiq-api.subimpact.net/api/validators
- VALIDATOR_ADDR: the validator being tracked. Default: NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY
- REWARD_ADDR: the reward wallet that receives coinbases and sends restakes. Default: same as VALIDATOR_ADDR
- MIN_TRIGGER_LUNA: restake trigger threshold in lunas. Default: 2000
- MIN_SHARE_LUNA: dust floor; shares below this with no restake are considered OK. Default: 500
- RESERVE_LUNA: amount of each reward held back as reserve. Default: 100000
- DATA_DIR: host directory for tracker.db. Default: /data
- PORT: API port inside the container. Default: 8649
- POLL_SECONDS: scheduler interval. Default: 60
- LOG_FILE: optional read-only log file for cycle boundary markers (informational only). Default: empty

## API endpoints

All GET, all read-only, all JSON. Errors are returned as {"error": "..."}. Nimiq addresses contain spaces, so percent-encode them in the URL path (for example the validator address above becomes NQ08%20ACT8...).

- GET /api/health -> {ok, db, last_run_ms}
- GET /api/validators/{addr}/summary -> latest snapshot, staker count, staked total, first and last reward
- GET /api/validators/{addr}/stakers -> latest snapshot per staker
- GET /api/validators/{addr}/cycles?limit=50 -> recent cycles with status and totals
- GET /api/validators/{addr}/cycles/{cycle_id}/shares -> per-staker expected, actual and ok
- GET /api/validators/{addr}/stakers/{address}/rewards?limit=50 -> the G1 ledger for one staker
- GET /api/verify?limit=50 -> recent cycles with status, for the /verify page

All money values are integers in lunas (1 NIM = 100000 lunas). NIM decimal amounts appear only in examples here.

## Development

Run the tests (stdlib unittest only, no installs):

```sh
python3 -m unittest discover tests -v
```

The tests spin up a fake RPC server using the standard library http.server with canned fixtures, including a fully verified cycle with three stakers and a mismatched cycle.

## License

MIT. See [LICENSE](LICENSE).
