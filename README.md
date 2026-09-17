# Nimiq Staking Rewards Tracker

Standard staking-rewards tracking + integrity verification for Nimiq Albatross validators, deployable on any node.

Answers the two community gaps from [core-rs-albatross #3170](https://github.com/nimiq/core-rs-albatross/issues/3170) and [#3171](https://github.com/nimiq/core-rs-albatross/issues/3171):

- **G1** — a per-staker rewards ledger: stake, pool share, every credited reward with its tx hash, compounding history, projected yield.
- **G2** — distribution integrity: every restake cycle is recomputed from chain state and compared with the actual on-chain transactions. VERIFIED or MISMATCH, math shown.

Reference deployment: the ImpactZero validator (0% fee, NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY). Live UI at nimiq.subimpact.net.

Read [SPEC.md](SPEC.md) for the schema, jobs and API contract.
