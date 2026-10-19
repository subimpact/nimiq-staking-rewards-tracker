"""Scheduler jobs: snapshot, rewards ingest, cycle close, verify.

All money values are integer lunas (1 NIM = 1e5 lunas). The four jobs mirror
the ImpactZero restake bot thresholds exactly as defined in SPEC.md.

Cycle model (deterministic, integer-only):
  - A restake is any outbound transaction from the reward address.
  - The window of a cycle is [opened_block, closed_block): a cycle is closed
    when a new restake transaction appears at a block strictly greater than the
    cycle's opened_block.
  - available_luna = sum(rewards in window) - held_luna. reserve_luna (the
    bot's wallet floor, 0 today) is NOT a per-cycle deduction from the
    distributable amount; it is recorded for reference only.
  - total_luna = the validator's latest snapshot at window open.
  - expected_i = floor(balance_i * available_luna / total_luna).
  - actual_i = sum of the window's AddStake txs naming staker i in
    relatedAddresses ("tx" attribution); the active-balance delta across the
    window only when some tx lacks that mapping ("delta" attribution).
"""

import json
import sys
import time
import urllib.error
import urllib.request

from tracker.config import (
    BACKFILL_PAGES_PER_POLL,
    COINBASE_ADDR,
    STAKING_CONTRACT_ADDR,
)

MAX_TX_BATCH = 200
# Upper bound on pages walked back per ingest head walk. At ~18.6 txs/min
# (19 stakers, measured 2026-10-18) one page covers ~11 minutes, so steady
# state needs one page; anything the head walk cannot reach is recorded in
# meta ingest_gaps and drained by the backfill pass (no outage loses data).
# Keep it small: dozens of calls per poll rate-limit (HTTP 429) on the shared
# public explorer.
MAX_TX_PAGES = 3
# Watermark meta keys: ingest walks back to the previous watermark so a poll
# outage (deploy, egress outage, RPC downtime) longer than one page of txs
# can no longer skip blocks permanently.
META_INGEST_WATERMARK = "ingest_watermark_block"
META_INGEST_GAPS = "ingest_gaps"


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

    def get_transactions(self, address, stop_before_block=None):
        """Fetch the validator/reward address's txs, newest first, following
        the explorer's hash cursor to older pages. Kept for backward compatibility.
        """
        txs, _status, _last_hash = self.walk_transactions(
            address, stop_before_block=stop_before_block
        )
        return txs

    def walk_transactions(
        self, address, stop_before_block=None, start_cursor=None, max_pages=MAX_TX_PAGES
    ):
        """Fetch transactions newest first, walking older pages using hash cursor.

        Returns (txs, status, last_hash) where status is one of:
          - "reached_floor": oldest tx < stop_before_block
          - "end_of_history": short page (< MAX_TX_BATCH) or empty data
          - "truncated": hit max_pages cap or HTTP 429
        """
        txs = []
        cursor = start_cursor
        last_hash = start_cursor
        status = "truncated" if max_pages <= 0 else "end_of_history"
        for _page in range(max_pages):
            try:
                res = self._rpc(
                    "getTransactionsByAddress", [address, MAX_TX_BATCH, cursor]
                )
            except urllib.error.HTTPError as exc:
                if exc.code == 429:
                    status = "truncated"
                    break
                raise
            if isinstance(res, dict):
                data = res.get("data") or []
            else:
                data = res or []
            if not data:
                status = "end_of_history"
                break
            txs.extend(data)
            last_hash = _tx_hash(data[-1])
            oldest_block = min(_tx_block(t) for t in data)
            if stop_before_block is not None and oldest_block < stop_before_block:
                status = "reached_floor"
                break
            if len(data) < MAX_TX_BATCH:
                status = "end_of_history"
                break
            cursor = last_hash
            if not cursor:
                status = "end_of_history"
                break
        else:
            status = "truncated"
        return txs, status, last_hash

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


def _tx_hash(tx):
    return tx.get("hash") or ""


def _tx_value(tx):
    raw = tx.get("value")
    if raw is None:
        raw = tx.get("amount", 0)
    return int(raw)


def _tx_related(tx):
    """On-chain staker attribution for an outbound restake: the explorer's
    relatedAddresses names every party of the tx — for an AddStake the staker
    being credited is listed alongside the validator and the staking contract.
    Absent or empty means the endpoint did not expose the mapping (fail-open:
    the verifier falls back to balance-delta math and the boundary hash)."""
    return tx.get("relatedAddresses") or tx.get("related") or []


def _tx_ts(tx):
    return int(tx.get("timestamp", time.time() * 1000))


def _staker_from_related(tx, validator_addr, contract_addr, reward_addr=None):
    """The explorer returns addresses spaced ('NQ27 NCB1 ...'); keep the third
    party's address exactly as sent so it matches the spaced addresses stored
    in the snapshot tables. The validator, the staking contract and the
    reward wallet (when it differs from the validator, it is the sender of
    every payout) are never the credited staker."""
    related = [str(a) for a in _tx_related(tx)]
    skip = {_norm_addr(validator_addr), _norm_addr(contract_addr)}
    if reward_addr:
        skip.add(_norm_addr(reward_addr))
    for a in related:
        if a and _norm_addr(a) not in skip:
            return a
    return ""


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
            if isinstance(validator_payload, dict) and not isinstance(validator_payload.get("data"), list):
                addr = validator_payload.get("address")
                if not addr or addr.replace(" ", "").lower() == ours:
                    vrow = validator_payload
            elif len(vrows) == 1 and isinstance(vrows[0], dict) and not vrows[0].get("address"):
                vrow = vrows[0]

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
        # PARTIAL-PAYLOAD GUARD (2026-10-19, F12): a truncated or paginated
        # answer is non-empty yet omits live stakers, and tombstoning those
        # would book a fake unstake. Tombstone only when the list is at least
        # as long as the validator record's own staker count (when the
        # record reports one); otherwise log and wait for a complete list.
        api_count = 0
        if vrow is not None:
            api_count = int(vrow.get("numStakers") or vrow.get("stakerCount") or 0)
        complete = bool(present) and (api_count == 0 or staker_count >= api_count)
        if present and not complete:
            print("snapshot: staker list has %d rows but the validator reports "
                  "%d stakers; skipping departure tombstones this poll"
                  % (staker_count, api_count), file=sys.stderr)
        if complete:
            for row in self.db.latest_staker_rows(self.cfg.validator_addr):
                addr = row["address"]
                if addr not in present and int(row["balance_luna"] or 0) > 0:
                    self.db.upsert_staker_snapshot(
                        self.cfg.validator_addr, addr, now, 0, 0
                    )

        # Deposit is not exposed by the API; derive it as total minus the sum
        # of staker records when both are present (100000 NIM when known).
        if vrow is not None:
            total_luna = int(vrow.get("balance") or vrow.get("total") or vrow.get("totalStake") or 0)
            api_num_stakers = int(vrow.get("numStakers") or vrow.get("stakerCount") or 0)
            deposit_luna = int(vrow.get("deposit") or vrow.get("validatorDeposit") or 0)
            if deposit_luna == 0 and total_luna > 0 and staker_sum > 0 and staker_count > 0:
                deposit_luna = max(total_luna - staker_sum, 0)
            num_stakers = staker_count if staker_count > 0 else api_num_stakers
            self.db.upsert_validator_snapshot(
                self.cfg.validator_addr, now, total_luna, num_stakers, deposit_luna
            )

        self._maybe_retention(now)

    def _maybe_retention(self, now):
        """Opt-in snapshot downsampling, at most once a day (F14)."""
        days = int(getattr(self.cfg, "snapshot_retention_days", 0) or 0)
        if days <= 0:
            return
        last = self.db.get_meta("retention_last_ms")
        if last and str(last).isdigit() and now - int(last) < 86400000:
            return
        self.db.downsample_staker_snapshots(now - days * 86400000)
        self.db.set_meta("retention_last_ms", str(now))

    # ------------------------------------------------------------------- job 2

    def _store_tx(self, tx):
        validator = self.cfg.validator_addr
        block = _tx_block(tx)
        sender = _tx_sender(tx).strip()
        recipient = _tx_recipient(tx).strip()
        value = _tx_value(tx)
        ts = _tx_ts(tx)
        tx_hash = _tx_hash(tx)

        norm_reward = _norm_addr(self.cfg.reward_addr)

        # Inbound coinbase block reward -> rewards row.
        if _is_coinbase(sender) and _norm_addr(recipient) == norm_reward:
            self.db.upsert_reward(validator, block, value, ts, tx_hash)
            return

        # Outbound from the reward address -> restake.
        if _norm_addr(sender) == norm_reward and recipient:
            staker = _staker_from_related(
                tx, validator, STAKING_CONTRACT_ADDR, self.cfg.reward_addr
            )
            self.db.upsert_restake(
                validator, block, recipient, value, ts, tx_hash, staker
            )
            return

    def _load_gaps(self):
        raw = self.db.get_meta(META_INGEST_GAPS)
        if not raw:
            return []
        try:
            data = json.loads(raw)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _save_gaps(self, gaps):
        self.db.set_meta(META_INGEST_GAPS, json.dumps(gaps))

    def _record_gap(self, cursor, floor):
        if not cursor or floor is None:
            return
        gaps = self._load_gaps()
        gaps = [g for g in gaps if g.get("floor") != floor]
        gaps.insert(0, {"cursor": cursor, "floor": floor})
        self._save_gaps(gaps)

    def run_ingest(self):
        # Walk back to the previous ingest watermark (the oldest block the
        # last successful poll already covered). First ingest after a cold
        # start with no watermark falls back to the oldest already-ingested
        # tx block, and only when both are unknown does it read a single
        # newest page (the pre-fix behavior; the next successful poll plants
        # the watermark and the catch-up window opens from then on).
        validator = self.cfg.validator_addr
        wm_raw = self.db.get_meta(META_INGEST_WATERMARK)
        if wm_raw is not None and str(wm_raw).strip().isdigit():
            stop_before = int(wm_raw)
        else:
            oldest = self.db.oldest_reward_or_restake_block(validator)
            stop_before = oldest if oldest else None

        is_cold_start = (stop_before is None)
        head_max_pages = 1 if is_cold_start else MAX_TX_PAGES

        if hasattr(self.fetcher, "walk_transactions"):
            txs, status, last_hash = self.fetcher.walk_transactions(
                self.cfg.reward_addr,
                stop_before_block=stop_before,
                start_cursor=None,
                max_pages=head_max_pages,
            )
        else:
            txs = self.fetcher.get_transactions(
                self.cfg.reward_addr, stop_before_block=stop_before
            )
            oldest = min((_tx_block(t) for t in txs), default=None)
            if stop_before is not None and oldest is not None and oldest < stop_before:
                status = "reached_floor"
            else:
                status = "end_of_history"
            last_hash = _tx_hash(txs[-1]) if txs else None

        for tx in txs or []:
            self._store_tx(tx)

        # Advance the watermark to the NEWEST tx block seen: the next poll's
        # walk-back starts just before what this poll definitively covered.
        # Only advance when txs were actually seen (an empty poll on a
        # just-started chain must not pin the watermark at None).
        if txs:
            watermark = max(_tx_block(t) for t in txs)
            if stop_before is None or watermark >= stop_before:
                self.db.set_meta(META_INGEST_WATERMARK, str(watermark))

        # Truncation during head walk records a pending gap (cold start must NOT register a gap)
        if not is_cold_start and status == "truncated" and last_hash:
            self._record_gap(cursor=last_hash, floor=stop_before)

        # Drain gaps newest-first up to BACKFILL_PAGES_PER_POLL
        if hasattr(self.fetcher, "walk_transactions"):
            backfill_cap = getattr(self.cfg, "backfill_pages_per_poll", BACKFILL_PAGES_PER_POLL)
            remaining_pages = backfill_cap
            gaps = self._load_gaps()
            while remaining_pages > 0 and gaps:
                gap = gaps[0]
                cursor = gap.get("cursor")
                floor = gap.get("floor")
                gap_txs, gap_status, gap_last_hash = self.fetcher.walk_transactions(
                    self.cfg.reward_addr,
                    stop_before_block=floor,
                    start_cursor=cursor,
                    max_pages=remaining_pages,
                )
                for tx in gap_txs or []:
                    self._store_tx(tx)

                pages_used = max(1, (len(gap_txs) + MAX_TX_BATCH - 1) // MAX_TX_BATCH) if gap_txs else 1
                remaining_pages -= pages_used

                if gap_status in ("reached_floor", "end_of_history"):
                    gaps.pop(0)
                    self._save_gaps(gaps)
                elif gap_status == "truncated":
                    if gap_last_hash:
                        gap["cursor"] = gap_last_hash
                    self._save_gaps(gaps)
                    break
                else:
                    break


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
            # Close-time bound for snapshot lookups (2026-10-06): the first
            # restake tx timestamp after the closing batch. Without it the
            # "strictly after" close-side lookups could reach into far later
            # snapshots and a staker who joined DAYS after this historical
            # window would fake an unattributable (Guard 3) credit here -
            # every catch-up cycle in a joiner's pre-join era became a
            # MISMATCH that way (2026-10-06 drain finding). The next batch's
            # own tx timestamps are chain time, the same clock snapshots are
            # polled against; beyond it, balances belong to the next window.
            close_bound_ms = self.db.first_restake_ts_after(
                self.cfg.validator_addr, closed_block
            )
            self._verify_and_close(pending, closed_block, window,
                                   boundary=batch, close_bound_ms=close_bound_ms)
            row = self.db.get_cycle(before)
            if row is None or row["status"] == "PENDING":
                # No progress possible (e.g. a hard Guard-1 block): stop the
                # catch-up loop for this tick instead of spinning on the
                # same pending cycle.
                break

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

    def _verify_and_close(self, pending, closed_block, window, boundary=None,
                          close_bound_ms=None):
        validator = self.cfg.validator_addr
        opened_block = int(pending["opened_block"])
        rewards = self.db.rewards_in_window(validator, opened_block, closed_block)
        # available = full coinbase sum in the window. reserve_luna is a wallet
        # safety floor, NOT a per-cycle deduction from what the bot distributes.
        # held stays as-is (0 in practice: the mis-send sentinel).
        sum_rewards = _sum_ints(r["amount_luna"] for r in rewards)
        available = sum_rewards
        held = int(pending["held_luna"])
        available -= held
        if available < 0:
            available = 0
        span = closed_block - opened_block
        dust = int(getattr(self.cfg, "carry_dust_luna", 10000))

        # ---- carry-in (M1) -------------------------------------------------
        # The payout engine sweeps its wallet ~1 minute after a coinbase, so a
        # cycle boundary regularly sits between a reward and the payout that
        # distributes it. The verifier follows the undistributed balance from
        # cycle to cycle instead of demanding that every window pays out
        # exactly its own income. A predecessor closed before M1 (NULL carry)
        # or no predecessor at all means the baseline is unknown.
        prev = self.db.latest_closed_cycle_any(validator)
        carry_in = None
        fee_carry_in = 0
        if prev is not None and prev["id"] != pending["id"]:
            carry_in = prev["carry_luna"]
            if prev["fee_carry_luna"] is not None:
                fee_carry_in = int(prev["fee_carry_luna"])
        baseline_known = carry_in is not None
        carry_in = int(carry_in or 0)

        # ---- classify every outbound row of the window (M2) ----------------
        operator_norm = {_norm_addr(a) for a in getattr(self.cfg, "operator_addrs", [])}
        contract_norm = _norm_addr(STAKING_CONTRACT_ADDR)
        payout_mode = getattr(self.cfg, "payout_mode", "restake")
        payout_rows = []
        stray_rows = []
        fee_paid = 0
        for r in window:
            to_norm = _norm_addr(r["to_addr"])
            if to_norm and to_norm in operator_norm:
                fee_paid += int(r["amount_luna"])
                continue
            is_restake = to_norm == contract_norm
            if payout_mode == "any":
                allowed = True
            elif payout_mode == "transfer":
                allowed = not is_restake
            else:
                allowed = is_restake
            (payout_rows if allowed else stray_rows).append(r)

        def _staker_of(r):
            # Restakes name the staker via relatedAddresses; a plain transfer
            # pays the staker's own address directly.
            saddr = r["staker_address"]
            if saddr and saddr.strip():
                return saddr
            if _norm_addr(r["to_addr"]) != contract_norm:
                return r["to_addr"] or ""
            return ""

        sum_payout = _sum_ints(r["amount_luna"] for r in payout_rows)
        sum_stray = _sum_ints(r["amount_luna"] for r in stray_rows)
        paid_out = sum_payout + sum_stray        # left the wallet, not as fee

        # Degenerate-window guard: a coinbase lands every 60 blocks, so a
        # window shorter than that can never contain the income funding its
        # payouts. Under M1 the carried balance can fund them; only when it
        # cannot (no baseline yet, or the carry does not cover the payout) is
        # there nothing to verify: close SKIPPED. A window >= 60 blocks with
        # payouts and no income is judged normally (carry or anomaly).
        if available == 0 and paid_out > 0 and span < 60 and (
                not baseline_known or carry_in - paid_out < -dust):
            self.db.clear_cycle_shares(pending["id"])
            self.db.close_cycle(
                pending["id"], closed_block, self._now_ms(),
                available, held, "SKIPPED",
                attribution="",
                note="degenerate window: payouts without income in a sub-60-block slice",
                carry_luna=max(carry_in - paid_out, 0) if baseline_known else None,
                fee_carry_luna=fee_carry_in - fee_paid,
            )
            self._open_pending_cycle()
            return

        paid = {}
        paid_raw_addr = {}
        unmapped = []
        staker_hash_map = {}
        for r in payout_rows:
            saddr = _staker_of(r)
            if saddr and saddr.strip():
                norm = _norm_addr(saddr)
                paid[norm] = paid.get(norm, 0) + int(r["amount_luna"])
                paid_raw_addr.setdefault(norm, saddr)
                staker_hash_map[norm] = r["tx_hash"]
            else:
                unmapped.append(r)

        mode = "tx" if not unmapped else "delta"

        total = int(pending["total_luna"]) or 1
        opened_at_ms = int(pending["opened_at_ms"])
        # Re-anchor stale wall-clock openers (2026-09-30 review): a PENDING
        # cycle that hard-blocked for days carries a wall-clock opened_at_ms
        # older than its closing batch, so "first snapshot strictly after"
        # would resolve to a pre-payout poll and mis-attribute the window.
        reanchored = self.db.first_restake_ts_after(validator, opened_block)
        if reanchored is not None and reanchored > opened_at_ms:
            opened_at_ms = reanchored
        boundary_hash = ""
        if boundary:
            boundary_hash = boundary[-1]["tx_hash"]
        else:
            b = self.db.next_restake_batch(
                validator, opened_block, self.cfg.batch_gap_blocks
            )
            if b is not None:
                boundary_hash = b[1][-1]["tx_hash"]

        # Opening balances weight each staker's share; in delta mode the
        # close-side balances also measure the credit. Lookups memoized per
        # address per window (drain latency).
        open_bal_map = {}
        close_bal_map = {}
        open_addrs = self.db.staker_addresses_at(validator, opened_at_ms)
        close_addrs = self.db.staker_addresses_after(
            validator, opened_at_ms, close_bound_ms
        )

        # A staker whose balance was already 0 at open and who was never
        # snapshotted again has fully unstaked and left the staker list; drop
        # them instead of wedging the close (2026-09-18..09-30 outage). A
        # NONZERO staker whose snapshots stopped still blocks (delta mode).
        excluded = set()
        for addr in sorted(open_addrs):
            bal_at = self.db.staker_balance_at(validator, addr, opened_at_ms)
            bal_after = self.db.staker_balance_after(
                validator, addr, opened_at_ms, close_bound_ms
            )
            open_bal_map[addr] = bal_at
            if bal_after is not None:
                close_bal_map[addr] = bal_after
            if bal_after is None and bal_at == 0:
                excluded.add(addr)
        open_addrs -= excluded

        # ---- what the rule says stakers are owed from this window's income --
        # Denominator = sum of staker balances at open (the reference bot
        # redistributes the deposit's earned slice). VALIDATOR_FEE_PPM takes
        # the operator's cut; DENOMINATOR_MODE=total keeps the deposit slice
        # with the operator. Falls back to the validator total when no
        # staker snapshots exist.
        staker_sum = sum((open_bal_map.get(a) or 0) for a in open_addrs)
        denom = staker_sum if staker_sum > 0 else total
        fee_ppm = int(getattr(self.cfg, "validator_fee_ppm", 0) or 0)
        fee_cut = (available * fee_ppm) // 1000000
        if (getattr(self.cfg, "denominator_mode", "stakers") == "total"
                and total > staker_sum > 0):
            denom = total
        owed = available - fee_cut
        if denom != staker_sum and staker_sum > 0:
            owed = (owed * staker_sum) // denom
        operator_take = available - owed

        carry_out = carry_in + owed - paid_out
        fee_carry_out = fee_carry_in + operator_take - fee_paid

        def _close_skipped(note):
            self.db.clear_cycle_shares(pending["id"])
            self.db.close_cycle(
                pending["id"], closed_block, self._now_ms(),
                available, held, "SKIPPED", attribution="delta", note=note,
                carry_luna=(carry_out if carry_out >= -dust else 0)
                if baseline_known else None,
                fee_carry_luna=fee_carry_out,
            )
            self._open_pending_cycle()

        unaccounted = 0
        reason_map = {}
        actual_map = {}
        non_open_payees = []

        if mode == "tx":
            # Actuals are the payout txs that name each staker; snapshots
            # only weight the expected share.
            open_norm = {_norm_addr(a) for a in open_addrs}
            for addr in sorted(open_addrs):
                actual_map[addr] = paid.get(_norm_addr(addr), 0)
                open_bal = open_bal_map.get(addr) or 0
                close_bal = close_bal_map.get(addr)
                if close_bal is not None and (close_bal - open_bal < 0):
                    reason_map[addr] = "unstaked during cycle"
            for norm in sorted(paid.keys()):
                if norm not in open_norm:
                    non_open_payees.append((norm, paid[norm]))
                    unaccounted += paid[norm]
        else:
            # Delta mode: some payout rows do not name their staker, so the
            # credit is the active-balance delta across the window.
            # Guard 1: every remaining staker present at open needs a
            # snapshot strictly after the open. With a later batch on record
            # (close_bound_ms set) it can never arrive: close SKIPPED.
            for addr in sorted(open_addrs):
                if addr not in close_bal_map:
                    if close_bound_ms is not None:
                        _close_skipped("insufficient snapshot coverage")
                        return
                    return        # wait for the snapshot (stays PENDING)
            for addr in sorted(open_addrs):
                delta = close_bal_map[addr] - (open_bal_map.get(addr) or 0)
                actual_map[addr] = delta
                if delta < 0:
                    # Guard 2: active balance decreased (unstake).
                    unaccounted += -delta
                    reason_map[addr] = "unstaked during cycle"
            # Guard 3: new stakers that appear mid-cycle cannot be
            # attributed; count their window credit as unaccounted.
            for addr in close_addrs - open_addrs:
                close_bal = self.db.staker_balance_after(
                    validator, addr, opened_at_ms, close_bound_ms
                )
                unaccounted += close_bal or 0

        # ---- per-staker check: share of what was actually paid out ---------
        # expected_i = balance_i * D / S over the stakers present at open,
        # D = what those stakers received in this window. Stakers whose share
        # floors below MIN_SHARE and who got nothing are the payout engine's
        # skipped dust: they leave the weight set. Whether D itself is the
        # right total is the carry check's job below.
        # Stakers already failed by a hard reason (unstaked mid-window) leave
        # the weight set together with whatever they received.
        weighted = {a for a in open_addrs if a not in reason_map}
        weights = {a: (open_bal_map.get(a) or 0) for a in weighted}
        D = sum(max(actual_map[a], 0) for a in weighted)
        S = sum(weights.values())
        min_share = self.cfg.min_share_luna
        skipped = {
            a for a in weighted
            if actual_map[a] == 0 and (S <= 0 or (weights[a] * D) // S < min_share)
        }
        S_eff = S - sum(weights[a] for a in skipped)
        # Pot = the amount the split is judged against. Normally D itself
        # (exact proportionality of what was paid). When one or a few stakers
        # are off, D is pulled by them and would blame everyone else, so the
        # pot follows the MEDIAN implied pot of the precisely measurable
        # shares (>= 10,000 luna) whenever it differs from D by more than
        # the dust tolerance: the outliers are then the ones flagged.
        pot = D
        if S_eff > 0:
            implied = sorted(
                (actual_map[a] * S_eff) // weights[a]
                for a in weighted
                if a not in skipped and weights[a] > 0
                and (weights[a] * D) // S_eff >= 10000 and actual_map[a] > 0
            )
            if len(implied) >= 3:
                mid = len(implied) // 2
                med = (implied[mid] if len(implied) % 2
                       else (implied[mid - 1] + implied[mid]) // 2)
                if abs(med - D) > self.cfg.dust_tolerance_luna:
                    pot = med
        ppm = int(getattr(self.cfg, "share_tolerance_ppm", 10))

        all_ok = True
        sum_delta = 0
        for addr in sorted(open_addrs):
            expected = 0
            if addr in weighted and addr not in skipped and S_eff > 0:
                expected = (weights[addr] * pot) // S_eff
            actual = actual_map[addr]
            sum_delta += actual
            if addr in reason_map:
                ok = False
                reason = reason_map[addr]
            elif actual > expected + self.cfg.dust_tolerance_luna:
                ok = False
                reason = "credit exceeds expected share (possible external top-up)"
            elif _is_ok(expected, actual, min_share,
                        self.cfg.dust_tolerance_luna, ppm):
                ok = True
                reason = ""
            else:
                ok = False
                reason = "paid less than stake share of this payout"
            all_ok = all_ok and ok
            self.db.upsert_cycle_share(
                pending["id"], addr, expected, actual,
                staker_hash_map.get(_norm_addr(addr)) or boundary_hash,
                1 if ok else 0, reason,
            )

        if mode == "tx":
            for norm, amt in non_open_payees:
                all_ok = False
                self.db.upsert_cycle_share(
                    pending["id"], paid_raw_addr.get(norm, norm), 0, amt,
                    staker_hash_map.get(norm) or boundary_hash,
                    0, "payee had no opening balance (joined during cycle)",
                )
        for r in stray_rows:
            # Money left the reward wallet outside the payout rule (e.g. a
            # plain transfer from a restake pool): never a verified payout.
            all_ok = False
            unaccounted += int(r["amount_luna"])
            self.db.upsert_cycle_share(
                pending["id"], r["to_addr"] or "?", 0, int(r["amount_luna"]),
                r["tx_hash"], 0,
                "outbound payment outside PAYOUT_MODE=%s" % payout_mode,
            )

        # Guard 4a: internal consistency of the attribution. Everything that
        # left the wallet (besides operator fee withdrawals) must equal the
        # attributed credits plus the unaccounted value within 1 luna.
        drift = abs(paid_out - (sum_delta + unaccounted)) > 1

        # Guard 4b (M1): the carried balance. Negative = more paid out than
        # was ever earned (unexplained funds); above the limit = rewards
        # withheld from stakers. With no baseline yet (first cycle, or the
        # first after an upgrade) a negative carry establishes it instead.
        carry_flag = ""
        note = ""
        carry_store = carry_out
        if carry_out < -dust:
            carry_store = 0
            if baseline_known:
                carry_flag = ("payout exceeds rewards earned plus carried "
                              "balance (unexplained funds)")
            else:
                note = "carry baseline established"
        else:
            batches = int(getattr(self.cfg, "carry_max_batches", 1))
            unit = self.db.max_recent_reward(validator, closed_block) or 0
            if carry_out > batches * unit + dust:
                carry_flag = ("rewards withheld: undistributed balance above "
                              "%d batch(es) of income (possible skim)" % batches)
        fee_flag = ""
        fee_store = fee_carry_out
        if operator_norm and fee_carry_out < -dust:
            fee_store = 0
            if baseline_known:
                fee_flag = "operator withdrew more than its fee entitlement"

        if drift or carry_flag or fee_flag:
            status = "MISMATCH"
        else:
            status = "VERIFIED" if all_ok else "MISMATCH"

        closed_at_ms = self._now_ms()
        self.db.close_cycle(
            pending["id"], closed_block, closed_at_ms, available, held, status,
            attribution=mode, note=note, carry_luna=carry_store,
            fee_carry_luna=fee_store,
        )

        # Surface the cycle-level reasons on shares that have none yet.
        parts = []
        if drift:
            parts.append("unaccounted restake value (possible timing drift)")
        if carry_flag:
            parts.append(carry_flag)
        if fee_flag:
            parts.append(fee_flag)
        if parts:
            reason = "; ".join(parts)
            for share in self.db.cycle_shares(pending["id"]):
                if share["reason"]:
                    continue
                self.db.upsert_cycle_share(
                    pending["id"], share["staker_address"],
                    share["expected_luna"], share["actual_luna"],
                    share["tx_hash"], share["ok"], reason,
                )

        # G1 ledger (guard 7): credited amounts for ok stakers. tx_hash is the
        # staker's real payout hash when the explorer named them; the
        # boundary batch hash stands in only when it did not.
        for share in self.db.cycle_shares(pending["id"]):
            if not share["ok"]:
                continue
            self.db.credit_staker_reward(
                share["staker_address"], closed_block, int(share["actual_luna"]),
                share["tx_hash"], closed_at_ms, validator=validator,
            )

        self._open_pending_cycle()

    # ------------------------------------------------------------------- job 4

    def run_verify(self):
        """Standalone verify (G2): idempotent re-credit of the latest closed
        cycle's ok shares so /verify always has fresh data.

        run_cycle_close is the only closer; run_verify must never close or
        modify a PENDING cycle.
        """
        latest = self.db.latest_closed_cycle(self.cfg.validator_addr)
        if latest is not None:
            shares = self.db.cycle_shares(latest["id"])
            for share in shares:
                if not share["ok"]:
                    continue
                self.db.credit_staker_reward(
                    share["staker_address"], latest["closed_block"],
                    int(share["actual_luna"]), share["tx_hash"], self._now_ms(),
                    validator=self.cfg.validator_addr,
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


def _norm_addr(addr):
    return (addr or "").replace(" ", "").upper()


def _is_coinbase(sender):
    return COINBASE_ADDR.replace(" ", "") in sender.replace(" ", "").upper()


def _is_ok(expected, actual, min_share, dust_tolerance=500, under_ppm=0):
    if actual == expected:
        return True
    if expected < min_share and actual == 0:
        return True
    # Integer-rounding tolerance (the bot floors each share).
    if abs(actual - expected) <= 1:
        return True
    # Relative under-drift tolerance (M1): the payout engine's live staker
    # view lags the minute snapshots by a few lunas, which moves a large
    # share by a few lunas. under_ppm parts per million of the share.
    if actual < expected and expected - actual <= (expected * under_ppm) // 1000000:
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
