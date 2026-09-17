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
        self.reserve_luna = _get_int(env, "RESERVE_LUNA", 100000)
        self.data_dir = _get_str(env, "DATA_DIR", "/data")
        self.port = _get_int(env, "PORT", 8649)
        self.poll_seconds = _get_int(env, "POLL_SECONDS", 60)
        self.log_file = _get_str(env, "LOG_FILE", "")

    def db_path(self):
        return os.path.join(self.data_dir, "tracker.db")

    def stakers_url(self):
        return self.stakers_url_template.format(address=quote(self.reward_addr))

    def validators_url(self):
        return self.validators_url_template


def load(environ=None):
    return Config(environ)
