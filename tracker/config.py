"""Environment-driven configuration for the Nimiq Staking Rewards Tracker.

All settings are read from environment variables, with defaults that mirror the
ImpactZero reference validator. No secrets live here and nothing is ever
written back out.
"""

import os
from urllib.parse import quote

DEFAULT_RPC_URL = "https://rpc.nimiqwatch.com"
DEFAULT_STAKERS_URL_TEMPLATE = "https://nimiq-api.subimpact.net/api/stakers/{address}"
DEFAULT_VALIDATORS_URL_TEMPLATE = "https://nimiq-api.subimpact.net/api/validators"
DEFAULT_VALIDATOR_ADDR = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"

# Coinbase reward sender, and the staking contract recipient for restakes.
COINBASE_ADDR = "NQ81 C01N BASE 0000 0000 0000 0000 0000 0000"
STAKING_CONTRACT_ADDR = "NQ77 0000 0000 0000 0000 0000 0000 0000 0001"

# A distribution burst arrives as AddStake txs ~1-2s apart (blocks differ by
# 1-2). A gap strictly larger than this between consecutive restake blocks means
# a new distribution event (or a pause), so it splits batches.
BATCH_GAP_BLOCKS = 10

# Honesty-check tolerance (luna): the bot may sweep a few lunas of dust into the
# batch or carry a couple of lunas across a cycle boundary, so the distributed
# sum may differ from the window's coinbase sum by a small amount. 500 luna =
# 0.005 NIM. A real skim (e.g. 5% of a 6 NIM reward = 30,000 luna) is far over
# the line. This is independent of RESERVE_LUNA, which is a wallet safety floor
# and is NOT deducted from the distributable amount.
DUST_TOLERANCE_LUNA = 500

# Upper bound on cycles closed per scheduler pass. A stalled or backlog state
# (a Guard-1 block stopped closes for 12 days, 2026-09-18..09-30) leaves
# thousands of already-ingested batches unverified; catch-up drains up to this
# many batches per 60s poll, so a ~15k-batch backlog clears in ~25 minutes
# instead of a day. Steady state unaffected: the loop breaks as soon as no
# complete batch is available (2nd iteration). Each closed cycle still runs
# the full attribution + guard set. 0 means "one close per tick" (no batching).
MAX_CLOSE_PER_TICK = 600

# Upper bound on pages walked per poll when draining pending ingest gaps.
BACKFILL_PAGES_PER_POLL = 3

# Carry-aware verification (M1, 2026-10-19). The payout engine sweeps its
# wallet about a minute after a coinbase lands, so a cycle boundary often
# falls between a reward and the payout that distributes it: one window holds
# the income, the next holds the payout. The verifier therefore tracks the
# undistributed balance ("carry") from cycle to cycle:
#   carry_out = carry_in + owed_to_stakers - paid_out
# and requires it to stay between -CARRY_DUST_LUNA and
# CARRY_MAX_BATCHES x (largest recent coinbase) + CARRY_DUST_LUNA.
# A withheld payout makes the carry grow past the limit; an unexplained
# payout makes it go negative. 0 batches = every window must pay out its own
# income (the pre-M1 strict rule).
CARRY_MAX_BATCHES = 1
CARRY_DUST_LUNA = 10000

# Under-payment tolerance for a staker's share of a payout, in parts per
# million of that share (minimum 1 luna for integer flooring). The payout
# engine's live staker view lags the minute snapshots by a few lunas.
SHARE_TOLERANCE_PPM = 10
# ...and never less than this many lunas (0.0005 NIM). The Sep-2026 bot
# version split a few dozen lunas differently between stakers; withheld
# totals are caught by the carry check, so this only bounds redistribution
# between stakers, alongside the 500-luna DUST_TOLERANCE_LUNA upper band.
SHARE_TOLERANCE_LUNA = 50

# Opt-in election weighting (payout basis "Option 1"). When ELECTION_FROM_EPOCH
# is above 0, every cycle whose epoch (from its opened_block) is >= this value
# weights each staker by min(stake of the staker in the epoch-start snapshot,
# the staker's live balance at cycle open): a mid-epoch joiner earns nothing
# until the next epoch and a mid-epoch top-up earns only on its epoch-start
# balance. Epoch math: epoch = (block - 1) // 43200 - 79. Cycles in earlier
# epochs keep the open-balance weights so re-deriving history stays correct.
# 0 = disabled (open-balance weights everywhere).
ELECTION_FROM_EPOCH = 0
# A staker's epoch-start balance is the first staker snapshot this close to the
# epoch's first reward/restake timestamp (defaults to 15 min; the bot takes its
# epoch-start snapshot within ~60 s of the boundary).
ELECTION_SNAPSHOT_WINDOW_MS = 900000

PAYOUT_MODES = ("restake", "transfer", "any")


def _get_int(env, name, default):
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _get_str(env, name, default):
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


class Config:
    def __init__(self, environ=None):
        env = os.environ if environ is None else environ
        self.rpc_url = _get_str(env, "RPC_URL", DEFAULT_RPC_URL)
        self.stakers_url_template = _get_str(
            env, "STAKERS_URL_TEMPLATE", DEFAULT_STAKERS_URL_TEMPLATE
        )
        self.validators_url_template = _get_str(
            env, "VALIDATORS_URL_TEMPLATE", DEFAULT_VALIDATORS_URL_TEMPLATE
        )
        self.validator_addr = _get_str(env, "VALIDATOR_ADDR", DEFAULT_VALIDATOR_ADDR)
        self.reward_addr = _get_str(env, "REWARD_ADDR", self.validator_addr)
        self.min_trigger_luna = _get_int(env, "MIN_TRIGGER_LUNA", 2000)
        self.min_share_luna = _get_int(env, "MIN_SHARE_LUNA", 500)
        self.reserve_luna = _get_int(env, "RESERVE_LUNA", 0)
        self.dust_tolerance_luna = _get_int(
            env, "DUST_TOLERANCE_LUNA", DUST_TOLERANCE_LUNA
        )
        self.batch_gap_blocks = _get_int(env, "BATCH_GAP_BLOCKS", BATCH_GAP_BLOCKS)
        self.max_close_per_tick = _get_int(
            env, "MAX_CLOSE_PER_TICK", MAX_CLOSE_PER_TICK
        )
        # Distribution rule (defaults = the reference bot): operator fee in
        # parts per million of window income, and whether the validator
        # deposit's earned slice goes to stakers ("stakers") or stays with the
        # operator ("total": denominator = validator total incl. deposit).
        self.validator_fee_ppm = _get_int(env, "VALIDATOR_FEE_PPM", 0)
        self.denominator_mode = _get_str(env, "DENOMINATOR_MODE", "stakers").lower()
        if self.denominator_mode not in ("stakers", "total"):
            raise ValueError("DENOMINATOR_MODE must be 'stakers' or 'total'")
        # Opt-in: downsample staker snapshots older than N days to one per
        # staker per hour (0 = keep everything).
        self.snapshot_retention_days = _get_int(env, "SNAPSHOT_RETENTION_DAYS", 0)
        self.backfill_pages_per_poll = _get_int(
            env, "BACKFILL_PAGES_PER_POLL", BACKFILL_PAGES_PER_POLL
        )
        self.carry_max_batches = _get_int(env, "CARRY_MAX_BATCHES", CARRY_MAX_BATCHES)
        self.carry_dust_luna = _get_int(env, "CARRY_DUST_LUNA", CARRY_DUST_LUNA)
        self.share_tolerance_ppm = _get_int(
            env, "SHARE_TOLERANCE_PPM", SHARE_TOLERANCE_PPM
        )
        self.share_tolerance_luna = _get_int(
            env, "SHARE_TOLERANCE_LUNA", SHARE_TOLERANCE_LUNA
        )
        # Opt-in election weighting (payout basis "Option 1", see above).
        self.election_from_epoch = _get_int(
            env, "ELECTION_FROM_EPOCH", ELECTION_FROM_EPOCH
        )
        self.election_snapshot_window_ms = _get_int(
            env, "ELECTION_SNAPSHOT_WINDOW_MS", ELECTION_SNAPSHOT_WINDOW_MS
        )
        # How stakers are paid (M2): "restake" = AddStake to the staking
        # contract (ImpactZero bot), "transfer" = plain transfer to the
        # staker's own address, "any" = either. An outbound payment that does
        # not match the mode is flagged.
        self.payout_mode = _get_str(env, "PAYOUT_MODE", "restake").lower()
        if self.payout_mode not in PAYOUT_MODES:
            raise ValueError("PAYOUT_MODE must be one of %s" % ", ".join(PAYOUT_MODES))
        # Operator wallets (comma separated). Outbound to these is the
        # operator withdrawing its fee, checked against what the fee rule
        # entitles it to, never counted as a staker payout.
        self.operator_addrs = [
            a.strip() for a in _get_str(env, "OPERATOR_ADDRS", "").split(",") if a.strip()
        ]
        self.data_dir = _get_str(env, "DATA_DIR", "/data")
        self.port = _get_int(env, "PORT", 8649)
        self.poll_seconds = _get_int(env, "POLL_SECONDS", 60)
        self.log_file = _get_str(env, "LOG_FILE", "")

    def db_path(self):
        return os.path.join(self.data_dir, "tracker.db")

    def stakers_url(self):
        # Stakers delegate to the VALIDATOR, not to its reward wallet.
        return self.stakers_url_template.format(address=quote(self.validator_addr))

    def validators_url(self):
        return self.validators_url_template


def load(environ=None):
    return Config(environ)
