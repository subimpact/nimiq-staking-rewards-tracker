"""Scheduler jobs: snapshot, rewards ingest, cycle close, verify.

All money values are integer lunas (1 NIM = 1e5 lunas). The four jobs mirror
the ImpactZero restake bot thresholds exactly as defined in SPEC.md.

Cycle model (deterministic, integer-only):
  - A restake is any outbound transaction from the reward address.
  - The window of a cycle is [opened_block, closed_block): a cycle is closed
    when a new restake transaction appears at a block strictly greater than the
    cycle's opened_block.
  - available_luna = sum(rewards in window) - held_luna. reserve_luna is a
    wallet safety floor (keep ~1 NIM for fees/rounding), NOT a per-cycle
    deduction from the distributable amount; it is recorded for reference only.
  - total_luna = the validator's latest snapshot at window open.
  - expected_i = floor(balance_i * available_luna / total_luna).
  - actual_i = sum of per-staker restakes inside the window.
"""

import json
import time
import urllib.request

from tracker.config import COINBASE_ADDR, STAKING_CONTRACT_ADDR

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


def _tx_related(tx):
    """On-chain staker attribution for an outbound restake: the explorer's
    relatedAddresses names every party of the tx — for an AddStake the staker
    being credited is listed alongside the validator and the staking contract.
    Absent or empty means the endpoint did not expose the mapping (fail-open:
    the verifier falls back to balance-delta math and the boundary hash)."""
    return tx.get("relatedAddresses") or tx.get("related") or []


def _staker_from_related(tx, validator_addr, contract_addr):
    """The explorer returns addresses spaced ('NQ27 NCB1 ...'); keep the third
    party's address exactly as sent so it matches the spaced addresses stored
    in the snapshot tables."""
    related = [str(a) for a in _tx_related(tx)]
    v = validator_addr.replace(" ", "")
    c = contract_addr.replace(" ", "")
    for a in related:
        if a and a.replace(" ", "") not in (v, c):
            return a
    return ""


class Jobs:
    def __init__(self, db, cfg, fetcher=None, now_ms=None):
        self.db = db
        self.cfg = cfg
        self._last_closed_pending_id = None
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
        present = set()
        for srow in _list_payload(stakers_payload):
            address = srow.get("address")
            if not address:
                continue
            present.add(address)
            balance = int(srow.get("balance") or 0)
            inactive = int(srow.get("inactiveBalance") or 0)
            staker_sum += balance
            staker_count += 1
            self.db.upsert_staker_snapshot(
                self.cfg.validator_addr, address, now, balance, inactive
            )

        # Tombstone departed stakers (2026-09-30 review): an address with a
        # prior nonzero balance that vanished from the staker list has fully
        # unstaked. Without a closing snapshot row its attribution blocks the
        # cycle close forever (the 2026-09-18..09-30 wedge, nonzero variant).
        # One zero-balance snapshot makes the vanish observable: Guard 2
        # records it as an honest "unstaked during cycle" MISMATCH, and the
        # departed-zero exclusion drops it from later windows.
        # GUARD: an EMPTY payload must never tombstone the whole pool - a
        # stakers-API outage or empty answer means "no data", not "everyone
        # left". Absence is only meaningful when the endpoint answered with
        # at least one row.
        if present:
            for row in self.db.latest_staker_rows(self.cfg.validator_addr):
                addr = row["address"]
                if addr not in present and int(row["balance_luna"] or 0) > 0:
                    self.db.upsert_staker_snapshot(
                        self.cfg.validator_addr, addr, now, 0, 0
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
                staker = _staker_from_related(tx, self.cfg.validator_addr, STAKING_CONTRACT_ADDR)
                self.db.upsert_restake(
                    self.cfg.validator_addr, block, recipient, value, ts, tx_hash, staker
                )
                continue

            # Anything else is the sentinel domain of the bot; ignore.

    # ------------------------------------------------------------------- job 3

    def run_cycle_close(self):
        # Close several batches per poll when a backlog exists (a wedged close
        # left 15k batches after 12 days of stalls, 2026-09-18..09-30). Normal
        # steady state closes the one available batch per pass, so this only
        # changes catch-up speed, not verify semantics: every cycle still runs
        # the full attribution + guard set.
        cap = self.cfg.max_close_per_tick
        rounds = cap if cap and cap > 0 else 1
        for _ in range(rounds):
            pending = self.db.latest_pending_cycle(self.cfg.validator_addr)
            if pending is None:
                break

            # Close on the end of the first complete restake batch strictly
            # after the cycle's opened_block. The restake bot sends a
            # distribution as a burst of AddStake txs ~1-2 blocks apart;
            # capturing the whole batch keeps the window aligned with the
            # per-staker balance deltas.
            boundary = self.db.next_restake_batch(
                self.cfg.validator_addr, int(pending["opened_block"]),
                self.cfg.batch_gap_blocks,
            )
            if boundary is None:
                break

            closed_block, batch = boundary
            window = self.db.restakes_in_window(
                self.cfg.validator_addr, int(pending["opened_block"]), closed_block
            )
            before = pending["id"]
            self._verify_and_close(pending, closed_block, window,
                                   boundary=batch)
            row = self.db.get_cycle(before)
            if row is None or row["status"] == "PENDING":
                # No progress possible (e.g. a hard Guard-1 block): stop the
                # catch-up loop for this tick instead of spinning on the
                # same pending cycle.
                break
            self._last_closed_pending_id = before

        if self.db.latest_pending_cycle(self.cfg.validator_addr) is None:
            self._open_pending_cycle()

    def _open_pending_cycle(self):
        """Open the successor cycle window.

        Chain from the LAST CLOSED cycle's closed_block whenever one exists:
        in steady state that block is also the latest restake block (a cycle
        closes on its batch end, so nothing newer exists yet), and after a
        stall it keeps the entire backlog of already-ingested batches inside
        their own windows instead of swallowing them into one giant PENDING
        cycle (2026-09-18..09-30 incident).

        opened_at_ms must be a chain-timestamp anchor, not the wall clock:
        snapshot lookups run at-or-before/strictly-after opened_at_ms, and
        for a historical window the relevant snapshots live in the past. The
        first restake tx timestamp after the opener is that anchor; the wall
        clock is only the fallback when no later tx is visible yet.
        """
        validator = self.cfg.validator_addr
        latest = self.db.latest_closed_cycle_any(validator)
        if latest is not None and latest["closed_block"] is not None:
            opened_block = int(latest["closed_block"])
            opened_at = (
                self.db.first_restake_ts_after(validator, opened_block)
                or self._now_ms()
            )
        else:
            opened_block = self.db.max_restake_block(validator) or 0
            opened_at = self._now_ms()
        # The fallback denominator must reflect the window's own era: for a
        # historical (catch-up) window, today's validator total is wrong.
        era = self.db.validator_snapshot_at(validator, opened_at)
        total = int(era["total_luna"]) if era is not None else self._latest_validator_total()
        self.db.create_cycle(
            validator, opened_block, opened_at, total, self.cfg.reserve_luna,
        )

    def _verify_and_close(self, pending, closed_block, window, boundary=None):
        opened_block = int(pending["opened_block"])
        rewards = self.db.rewards_in_window(
            self.cfg.validator_addr, opened_block, closed_block
        )
        # available = full coinbase sum in the window. reserve_luna is a wallet
        # safety floor, NOT a per-cycle deduction from what the bot distributes:
        # the bot pays out the whole reward (plus/minus dust), so carving the
        # reserve here would make every expected share lower than actual and
        # trip guard 5 for every staker. held stays as-is (0 in practice).
        sum_rewards = _sum_ints(r["amount_luna"] for r in rewards)
        available = sum_rewards
        held = int(pending["held_luna"])
        available -= held
        if available < 0:
            available = 0

        # Restake total in the window (also used by guard 4a below).
        sum_txs = _sum_ints(r["amount_luna"] for r in window)

        # Degenerate-window guard: a coinbase lands every 60 blocks, so a
        # window shorter than that can never contain the income that funds its
        # payouts. When the cycle opener lands mid-burst (the bot spreads one
        # distribution across 1-2 blocks a second apart), the window collapses
        # to a block or two: payouts appear, but their funding coinbase sits in
        # the adjacent window, so expected is 0 for every staker and Guard 5
        # would MISMATCH every paid staker. There is nothing to verify: the
        # restake txs remain on record in restake_txs for auditors, and the
        # funding income is verified in its own window. Close SKIPPED.
        # A window >= 60 blocks with payouts and no income is a real anomaly
        # (operator paid with no earnings) and still goes MISMATCH.
        span = closed_block - opened_block
        if available == 0 and sum_txs > 0 and span < 60:
            self.db.clear_cycle_shares(pending["id"])
            self.db.close_cycle(
                pending["id"], closed_block, self._now_ms(),
                available, held, "SKIPPED",
            )
            self._open_pending_cycle()
            return

        total = int(pending["total_luna"]) or 1
        opened_at_ms = int(pending["opened_at_ms"])
        # Re-anchor stale wall-clock openers (2026-09-30 review): a PENDING
        # cycle that hard-blocked for days carries a wall-clock opened_at_ms
        # older than its closing batch, so "first snapshot strictly after"
        # would resolve to a pre-payout poll and mis-attribute the window.
        # Anchoring at the first restake tx strictly after the opener block
        # gives the identical chain-time semantics new cycles already get.
        reanchored = self.db.first_restake_ts_after(
            self.cfg.validator_addr, opened_block
        )
        if reanchored is not None and reanchored > opened_at_ms:
            opened_at_ms = reanchored
        boundary_hash = ""
        if boundary:
            boundary_hash = boundary[-1]["tx_hash"]
        else:
            b = self.db.next_restake_batch(
                self.cfg.validator_addr, opened_block, self.cfg.batch_gap_blocks
            )
            if b is not None:
                boundary_hash = b[1][-1]["tx_hash"]

        # Option A attribution: active balances only change via AddStake, so a
        # staker's credit is the active-balance delta between the snapshot
        # at/before the window opened and the first snapshot strictly after.
        # Balance lookups are memoized per (addr) per window: the exclusion
        # scan, Guard 1, the delta loop, staker_sum and the expected loop all
        # read the same two values, and at 3+ stakers x hundreds of closes per
        # tick the redundant queries dominate drain latency.
        open_bal_map = {}
        close_bal_map = {}
        open_addrs = self.db.staker_addresses_at(self.cfg.validator_addr, opened_at_ms)
        close_addrs = self.db.staker_addresses_after(
            self.cfg.validator_addr, opened_at_ms
        )

        # A staker whose balance was already 0 at the cycle open and whose last
        # snapshot IS that open (never snapshotted again afterwards) has fully
        # unstaked and left the staker list: the snapshot job stops fetching
        # them, so a "strictly after" snapshot can never exist. Blocking the
        # close on them wedges every later cycle (12-day outage root cause,
        # 2026-09-18..09-30). They hold no stake and expect no share; drop them
        # from the attribution set instead. A staker with a NONZERO balance at
        # open whose snapshots genuinely stopped still blocks the close: that
        # is real insufficient data and must stay PENDING.
        excluded = set()
        for addr in sorted(open_addrs):
            bal_at = self.db.staker_balance_at(
                self.cfg.validator_addr, addr, opened_at_ms
            )
            bal_after = self.db.staker_balance_after(
                self.cfg.validator_addr, addr, opened_at_ms
            )
            open_bal_map[addr] = bal_at
            if bal_after is not None:
                close_bal_map[addr] = bal_after
            if bal_after is None and bal_at == 0:
                excluded.add(addr)
        open_addrs -= excluded

        # Guard 1: every remaining staker present at open needs a snapshot
        # strictly after the cycle opened; otherwise the window cannot be
        # attributed.
        for addr in sorted(open_addrs):
            if self.db.staker_balance_after(
                self.cfg.validator_addr, addr, opened_at_ms
            ) is None:
                # Insufficient snapshots; leave the cycle PENDING.
                return

        unaccounted = 0
        reason_map = {}
        actual_map = {}
        for addr in sorted(open_addrs):
            open_bal = open_bal_map.get(addr) or 0
            close_bal = close_bal_map.get(addr)
            if close_bal is None:
                close_bal = self.db.staker_balance_after(
                    self.cfg.validator_addr, addr, opened_at_ms
                )
                if close_bal is None:
                    # Data vanished between Guard 1 and attribution; do not
                    # guess a zero delta, leave the cycle PENDING.
                    return
                close_bal_map[addr] = close_bal
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
        # Per-staker proof: when the explorer named the credited staker in
        # relatedAddresses, each share carries that real AddStake tx hash;
        # otherwise the boundary batch hash remains the stand-in. Keys are
        # normalized (case/whitespace) because RPC implementations vary.
        staker_hash_map = {}
        for r in window:
            if r["staker_address"]:
                staker_hash_map[r["staker_address"].replace(" ", "").upper()] = r["tx_hash"]
        # Denominator = sum of staker ledger balances at cycle open, matching
        # what the payout engine actually distributes against (its validator
        # balance excludes the 100k deposit, so the deposit's earned slice is
        # redistributed to stakers; the verifier must expect that). Falls back
        # to the validator total when no staker snapshots exist.
        staker_sum = sum(
            (open_bal_map.get(a) or 0)
            for a in open_addrs
        )
        denom = staker_sum if staker_sum > 0 else total
        for addr in sorted(open_addrs):
            balance = open_bal_map.get(addr) or 0
            expected = (balance * available) // denom
            expected_total += expected
            actual = actual_map[addr]
            sum_delta += actual

            if addr in reason_map:
                ok = False
                reason = reason_map[addr]
            elif actual > expected + self.cfg.dust_tolerance_luna:
                # Guard 5: credit exceeds expected share by more than the dust
                # noise floor. The bot pays every staker's proportional share
                # each cycle (integer floor; only zero shares are skipped), and
                # its live staker view lags our minute-snapshots by a few lunas,
                # so a small overage is normal operating behavior. Anything
                # beyond the tolerance is an external top-up or misallocation.
                ok = False
                reason = "credit exceeds expected share (possible external top-up)"
            else:
                ok = _is_ok(expected, actual, self.cfg.min_share_luna, self.cfg.dust_tolerance_luna)
                reason = ""
            all_ok = all_ok and ok
            self.db.upsert_cycle_share(
                pending["id"], addr, expected, actual,
                staker_hash_map.get(addr.replace(" ", "").upper()) or boundary_hash,
                1 if ok else 0, reason,
            )

        # Guard 4a: internal consistency of the attribution. The sum of on-chain
        # restakes must match the sum of attributed deltas plus any unaccounted
        # (restaked) value within 1 luna, else the window was mis-windowed.
        drift = abs(sum_txs - (sum_delta + unaccounted)) > 1

        # Guard 4b: honesty against the cycle's actual reward income. The bot
        # distributes what it earns (within dust); if the batch sum is far from
        # the coinbase sum in the same window, the operator is skimming while
        # staying internally proportional.
        skim = abs(sum_txs - sum_rewards) > self.cfg.dust_tolerance_luna

        if drift or skim:
            status = "MISMATCH"
        else:
            status = "VERIFIED" if all_ok else "MISMATCH"

        closed_at_ms = self._now_ms()
        self.db.close_cycle(
            pending["id"], closed_block, closed_at_ms, available, held, status
        )

        # Surface the drift/skim reasons on shares that have none yet.
        if drift or skim:
            if drift:
                reason = "unaccounted restake value (possible timing drift)"
            else:
                reason = "distribution does not match cycle rewards (possible skim)"
            for share in self.db.cycle_shares(pending["id"]):
                if share["reason"]:
                    continue
                self.db.upsert_cycle_share(
                    pending["id"], share["staker_address"],
                    share["expected_luna"], share["actual_luna"],
                    share["tx_hash"], share["ok"], reason,
                )

        # G1 ledger (guard 7): credited amounts for ok stakers. tx_hash is the
        # staker's real AddStake hash when the explorer named them in
        # relatedAddresses; the boundary batch hash stands in only when the
        # endpoint did not expose the mapping.
        for share in self.db.cycle_shares(pending["id"]):
            if not share["ok"]:
                continue
            credited = int(share["actual_luna"])
            self.db.credit_staker_reward(
                share["staker_address"], closed_block, credited,
                share["tx_hash"], closed_at_ms,
            )

        self._open_pending_cycle()

    # ------------------------------------------------------------------- job 4

    def run_verify(self):
        """Standalone verify (G2): recompute shares for the most recent cycle
        that has a closing restake boundary available. Skipped when the close
        job already handled the pending cycle this tick (drain rate 600 makes
        a duplicated close attempt the common case, not the exception)."""
        pending = self.db.latest_pending_cycle(self.cfg.validator_addr)
        if pending is not None:
            if pending["id"] == self._last_closed_pending_id:
                return
            boundary = self.db.next_restake_batch(
                self.cfg.validator_addr, int(pending["opened_block"]),
                self.cfg.batch_gap_blocks,
            )
            if boundary is None:
                # Window not closed yet; nothing to verify.
                return
            closed_block, _ = boundary
            window = self.db.restakes_in_window(
                self.cfg.validator_addr, int(pending["opened_block"]),
                closed_block,
            )
            self._verify_and_close(pending, closed_block, window)
            return

        # No pending cycle: re-emit the latest closed cycle's shares (idempotent
        # via the G1 ledger upsert) so /verify always has fresh data.
        latest = self.db.latest_closed_cycle(self.cfg.validator_addr)
        if latest is not None:
            shares = self.db.cycle_shares(latest["id"])
            for share in shares:
                if not share["ok"]:
                    continue
                self.db.credit_staker_reward(
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


def _is_ok(expected, actual, min_share, dust_tolerance=500):
    if actual == expected:
        return True
    if expected < min_share and actual == 0:
        return True
    # Integer-rounding tolerance (the bot floors each share).
    if abs(actual - expected) <= 1:
        return True
    # Dust-noise floor: actual may exceed expected by up to one dust
    # tolerance because dust shares (< min_share) are skipped and swept into
    # later batches, and the bot's live staker view lags the verifier's
    # snapshots by a few lunas. Upper drift within the tolerance is normal;
    # under-drift (actual < expected - 1) still flags.
    if actual > expected and actual - expected <= dust_tolerance:
        return True
    return False


def _sum_ints(it):
    total = 0
    for v in it:
        total += int(v)
    return total
