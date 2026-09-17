"""Scheduler jobs: snapshot, rewards ingest, cycle close, verify.

All money values are integer lunas (1 NIM = 1e5 lunas). The four jobs mirror
the ImpactZero restake bot thresholds exactly as defined in SPEC.md.

Cycle model (deterministic, integer-only):
  - A restake is any outbound transaction from the reward address.
  - The window of a cycle is [opened_block, closed_block): a cycle is closed
    when a new restake transaction appears at a block strictly greater than the
    cycle's opened_block.
  - available_luna = sum(rewards in window) - reserve_luna - held_luna.
  - total_luna = the validator's latest snapshot at window open.
  - expected_i = floor(balance_i * available_luna / total_luna).
  - actual_i = sum of per-staker restakes inside the window.
"""

import json
import time
import urllib.request

from tracker.config import COINBASE_ADDR

MAX_TX_BATCH = 200


class LiveFetcher:
    """Default network layer (stdlib urllib only)."""

    def __init__(self, cfg):
        self.cfg = cfg
        self._id = 0

    def _rpc(self, method, params):
        body = json.dumps(
            {"jsonrpc": "2.0", "method": method, "params": params, "id": self._id}
        ).encode("utf-8")
        self._id += 1
        req = urllib.request.Request(
            self.cfg.rpc_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "nimiq-staking-rewards-tracker/1.0 (+https://github.com/subimpact/nimiq-staking-rewards-tracker)",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if "error" in payload:
            raise RuntimeError("RPC error: %s" % payload["error"])
        return payload.get("result")

    def _get(self, url):
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "nimiq-staking-rewards-tracker/1.0 (+https://github.com/subimpact/nimiq-staking-rewards-tracker)",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def get_transactions(self, address):
        res = self._rpc("getTransactionsByAddress", [address, MAX_TX_BATCH, None])
        if isinstance(res, dict):
            return res.get("data") or []
        return res or []

    def get_stakers(self):
        return self._get(self.cfg.stakers_url())

    def get_validators(self):
        return self._get(self.cfg.validators_url())


def _tx_block(tx):
    return int(tx.get("blockNumber") if "blockNumber" in tx else tx.get("block", 0))


def _tx_sender(tx):
    return tx.get("sender") or tx.get("from") or tx.get("fromAddress") or ""


def _tx_recipient(tx):
    return tx.get("recipient") or tx.get("to") or tx.get("toAddress") or ""


def _tx_value(tx):
    raw = tx.get("value")
    if raw is None:
        raw = tx.get("amount", 0)
    return int(raw)


def _tx_ts(tx):
    return int(tx.get("timestamp", time.time() * 1000))


def _tx_hash(tx):
    return tx.get("hash") or ""


class Jobs:
    def __init__(self, db, cfg, fetcher=None, now_ms=None):
        self.db = db
        self.cfg = cfg
        self.fetcher = fetcher if fetcher is not None else LiveFetcher(cfg)
        # Monotonic clock injection: tests pass a counter so consecutive
        # snapshots never share a millisecond (same-ms timestamps made the
        # strict-after snapshot lookup flaky).
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))

    def _latest_validator_total(self):
        row = self.db.latest_validator_snapshot(self.cfg.validator_addr)
        return int(row["total_luna"]) if row else 0

    # ------------------------------------------------------------------- job 1

    def run_snapshot(self):
        now = self._now_ms()
        validator_payload = self.fetcher.get_validators()
        stakers_payload = self.fetcher.get_stakers()

        vrows = _list_payload(validator_payload)
        ours = self.cfg.validator_addr.replace(" ", "").lower()
        vrow = next(
            (r for r in vrows if isinstance(r, dict) and r.get("address")
             and r["address"].replace(" ", "").lower() == ours),
            None,
        )
        if vrow is None:
            vrow = _first_payload(validator_payload)
        total_luna = int(vrow.get("balance") or vrow.get("total") or vrow.get("totalStake") or 0)
        api_num_stakers = int(vrow.get("numStakers") or vrow.get("stakerCount") or 0)
        deposit_luna = int(vrow.get("deposit") or vrow.get("validatorDeposit") or 0)

        # Write staker rows first so their sum is available for the deposit.
        staker_sum = 0
        staker_count = 0
        for srow in _list_payload(stakers_payload):
            address = srow.get("address")
            if not address:
                continue
            balance = int(srow.get("balance") or 0)
            inactive = int(srow.get("inactiveBalance") or 0)
            staker_sum += balance
            staker_count += 1
            self.db.upsert_staker_snapshot(
                self.cfg.validator_addr, address, now, balance, inactive
            )

        # Deposit is not exposed by the API; derive it as total minus the sum
        # of staker records when both are present (100000 NIM when known).
        if deposit_luna == 0 and total_luna > 0 and staker_sum > 0 and staker_count > 0:
            deposit_luna = max(total_luna - staker_sum, 0)
        num_stakers = staker_count if staker_count > 0 else api_num_stakers
        self.db.upsert_validator_snapshot(
            self.cfg.validator_addr, now, total_luna, num_stakers, deposit_luna
        )

    # ------------------------------------------------------------------- job 2

    def run_ingest(self):
        txs = self.fetcher.get_transactions(self.cfg.reward_addr)
        for tx in txs or []:
            block = _tx_block(tx)
            sender = _tx_sender(tx).strip()
            recipient = _tx_recipient(tx).strip()
            value = _tx_value(tx)
            ts = _tx_ts(tx)
            tx_hash = _tx_hash(tx)

            # Inbound coinbase block reward -> rewards row.
            if _is_coinbase(sender) and recipient == self.cfg.reward_addr:
                self.db.upsert_reward(
                    self.cfg.validator_addr, block, value, ts, tx_hash
                )
                continue

            # Outbound from the reward address -> restake.
            if sender == self.cfg.reward_addr and recipient:
                self.db.upsert_restake(
                    self.cfg.validator_addr, block, recipient, value, ts, tx_hash
                )
                continue

            # Anything else is the sentinel domain of the bot; ignore.

    # ------------------------------------------------------------------- job 3

    def run_cycle_close(self):
        pending = self.db.latest_pending_cycle(self.cfg.validator_addr)
        if pending is None:
            self._open_pending_cycle()
            return

        # Find the first restake strictly after the cycle's opened_block.
        boundary = self.db.first_restake_after(
            self.cfg.validator_addr, int(pending["opened_block"])
        )
        if boundary is None:
            return

        closed_block = int(boundary["block"])
        window = self.db.restakes_in_window(
            self.cfg.validator_addr, int(pending["opened_block"]), closed_block
        )
        self._verify_and_close(pending, closed_block, window)

    def _open_pending_cycle(self):
        opened_block = self.db.max_restake_block(self.cfg.validator_addr) or 0
        total = self._latest_validator_total()
        self.db.create_cycle(
            self.cfg.validator_addr, opened_block, self._now_ms(),
            total, self.cfg.reserve_luna,
        )

    def _verify_and_close(self, pending, closed_block, window):
        opened_block = int(pending["opened_block"])
        rewards = self.db.rewards_in_window(
            self.cfg.validator_addr, opened_block, closed_block
        )
        available = _sum_ints(r["amount_luna"] for r in rewards) - self.cfg.reserve_luna
        held = int(pending["held_luna"])
        available -= held
        if available < 0:
            available = 0

        total = int(pending["total_luna"]) or 1
        opened_at_ms = int(pending["opened_at_ms"])
        boundary_hash = ""
        boundary = self.db.first_restake_after(self.cfg.validator_addr, opened_block)
        if boundary is not None:
            boundary_hash = boundary["tx_hash"]

        # Option A attribution: active balances only change via AddStake, so a
        # staker's credit is the active-balance delta between the snapshot
        # at/before the window opened and the first snapshot strictly after.
        open_addrs = self.db.staker_addresses_at(self.cfg.validator_addr, opened_at_ms)
        close_addrs = self.db.staker_addresses_after(
            self.cfg.validator_addr, opened_at_ms
        )

        # Guard 1: every staker present at open needs a snapshot strictly after
        # the cycle opened; otherwise the window cannot be attributed.
        for addr in open_addrs:
            if self.db.staker_balance_after(
                self.cfg.validator_addr, addr, opened_at_ms
            ) is None:
                # Insufficient snapshots; leave the cycle PENDING.
                return

        unaccounted = 0
        reason_map = {}
        actual_map = {}
        for addr in open_addrs:
            open_bal = self.db.staker_balance_at(
                self.cfg.validator_addr, addr, opened_at_ms
            ) or 0
            close_bal = self.db.staker_balance_after(
                self.cfg.validator_addr, addr, opened_at_ms
            )
            delta = close_bal - open_bal
            actual_map[addr] = delta
            if delta < 0:
                # Guard 2: active balance decreased (unstake).
                unaccounted += -delta
                reason_map[addr] = "unstaked during cycle"

        # Guard 3: new stakers that appear mid-cycle have no open balance and
        # cannot be attributed; count their window credit as unaccounted.
        for addr in close_addrs - open_addrs:
            close_bal = self.db.staker_balance_after(
                self.cfg.validator_addr, addr, opened_at_ms
            )
            unaccounted += close_bal or 0

        all_ok = True
        sum_delta = 0
        expected_total = 0
        for addr in open_addrs:
            balance = self.db.staker_balance_at(
                self.cfg.validator_addr, addr, opened_at_ms
            ) or 0
            expected = (balance * available) // total
            expected_total += expected
            actual = actual_map[addr]
            sum_delta += actual

            if addr in reason_map:
                ok = False
                reason = reason_map[addr]
            elif actual > expected + 1:
                # Guard 5: credit exceeds expected share.
                ok = False
                reason = "credit exceeds expected share (possible external top-up)"
            else:
                ok = _is_ok(expected, actual, self.cfg.min_share_luna)
                reason = ""
            all_ok = all_ok and ok
            self.db.upsert_cycle_share(
                pending["id"], addr, expected, actual, boundary_hash,
                1 if ok else 0, reason,
            )

        # Guard 4: self-consistency check against the on-chain restake value.
        sum_txs = _sum_ints(r["amount_luna"] for r in window)
        drift = abs(sum_txs - (sum_delta + unaccounted)) > 1
        if drift:
            status = "MISMATCH"
        else:
            status = "VERIFIED" if all_ok else "MISMATCH"

        closed_at_ms = self._now_ms()
        self.db.close_cycle(
            pending["id"], closed_block, closed_at_ms, available, held, status
        )

        # Surface the drift reason on shares that have none yet.
        if drift:
            drift_reason = "unaccounted restake value (possible timing drift)"
            for share in self.db.cycle_shares(pending["id"]):
                if share["reason"]:
                    continue
                self.db.upsert_cycle_share(
                    pending["id"], share["staker_address"],
                    share["expected_luna"], share["actual_luna"],
                    share["tx_hash"], share["ok"], drift_reason,
                )

        # G1 ledger (guard 7): credited amounts for ok stakers. The AddStake
        # payload cannot be decoded without raw txs, so the boundary batch hash
        # stands in for the credit's tx_hash.
        for share in self.db.cycle_shares(pending["id"]):
            if not share["ok"]:
                continue
            credited = int(share["actual_luna"])
            self.db.upsert_staker_reward(
                share["staker_address"], closed_block, credited,
                boundary_hash, closed_at_ms,
            )

        self._open_pending_cycle()

    # ------------------------------------------------------------------- job 4

    def run_verify(self):
        """Standalone verify (G2): recompute shares for the most recent cycle
        that has a closing restake boundary available."""
        pending = self.db.latest_pending_cycle(self.cfg.validator_addr)
        if pending is not None:
            boundary = self.db.first_restake_after(
                self.cfg.validator_addr, int(pending["opened_block"])
            )
            if boundary is None:
                # Window not closed yet; nothing to verify.
                return
            window = self.db.restakes_in_window(
                self.cfg.validator_addr, int(pending["opened_block"]),
                int(boundary["block"]),
            )
            self._verify_and_close(pending, int(boundary["block"]), window)
            return

        # No pending cycle: re-emit the latest closed cycle's shares (idempotent
        # via the G1 ledger upsert) so /verify always has fresh data.
        latest = self.db.latest_closed_cycle(self.cfg.validator_addr)
        if latest is not None:
            shares = self.db.cycle_shares(latest["id"])
            for share in shares:
                if not share["ok"]:
                    continue
                self.db.upsert_staker_reward(
                    share["staker_address"], latest["closed_block"],
                    int(share["actual_luna"]), share["tx_hash"], self._now_ms(),
                )


# -------------------------------------------------------------------- helpers


def _first_payload(payload):
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list) and data:
            return data[0]
        return payload
    if isinstance(payload, list) and payload:
        return payload[0]
    return {}


def _list_payload(payload):
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        return []
    if isinstance(payload, list):
        return payload
    return []


def _is_coinbase(sender):
    return COINBASE_ADDR.replace(" ", "") in sender.replace(" ", "").upper()


def _is_ok(expected, actual, min_share):
    if actual == expected:
        return True
    if expected < min_share and actual == 0:
        return True
    if abs(actual - expected) <= 1:
        return True
    return False


def _sum_ints(it):
    total = 0
    for v in it:
        total += int(v)
    return total
