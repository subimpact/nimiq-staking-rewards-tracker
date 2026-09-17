"""Read-only HTTP API for the Nimiq Staking Rewards Tracker.

Implements the routes from SPEC.md using only the Python standard library
(http.server). Every endpoint is a GET returning JSON. Errors are returned as
JSON bodies of the form {"error": "..."} with an appropriate status code.
"""

import json
import re
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _query(params, key, default):
    vals = params.get(key)
    if not vals:
        return default
    return vals[-1]


class APIServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, addr, db, cfg):
        self.db = db
        self.cfg = cfg
        self.started_at_ms = None
        self.last_run_ms = None
        super().__init__(addr, Handler)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # keep the API quiet
        pass

    def do_GET(self):
        self.handle_request()

    def do_HEAD(self):
        self.handle_request()

    def handle_request(self):
        parsed = self._parse_path(self.path)
        if parsed is None:
            self._send_json(404, {"error": "not found"})
            return
        kind = parsed["kind"]
        params = parsed["params"]
        try:
            if kind == "health":
                payload = self._health()
            elif kind == "verify":
                payload = self._verify(params)
            elif kind == "summary":
                payload = self._summary(params["vaddr"])
            elif kind == "stakers":
                payload = self._stakers(params["vaddr"])
            elif kind == "cycles":
                payload = self._cycles(params["vaddr"], params)
            elif kind == "shares":
                payload = self._shares(params["vaddr"], params["cid"])
            elif kind == "rewards":
                payload = self._rewards(params["saddr"], params)
            else:  # pragma: no cover - defensive
                self._send_json(404, {"error": "not found"})
                return
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"error": str(exc)})
            return
        self._send_json(200, payload)

    # ------------------------------------------------------------------ routes

    def _health(self):
        srv = self.server
        try:
            srv.db.conn.execute("SELECT 1")
            db_ok = True
        except Exception:
            db_ok = False
        return {
            "ok": db_ok,
            "db": srv.cfg.db_path(),
            "last_run_ms": srv.last_run_ms,
        }

    def _verify(self, params):
        limit = _int_param(params, "limit", 50)
        before = _int_param(params, "before", None) if "before" in params else None
        if before is not None and before < 1:
            before = None
        rows, has_more = self.server.db.recent_cycles_before(
            self.server.cfg.validator_addr, limit, before
        )
        return {"cycles": [_cycle_json(r) for r in rows], "has_more": has_more}

    def _summary(self, vaddr):
        db = self.server.db
        snap = db.latest_validator_snapshot(vaddr)
        stakers = db.latest_staker_rows(vaddr)
        total_balance = sum(int(r["balance_luna"]) for r in stakers)
        fr = db.first_last_rewards(vaddr)
        summary = {
            "validator": vaddr,
            "snapshot": _row_dict(snap),
            "num_stakers": len(stakers),
            "staked_luna": total_balance,
            "rewards": {
                "count": int(fr["n"]) if fr else 0,
                "first_ts_ms": int(fr["first_ts"]) if fr and fr["first_ts"] else None,
                "last_ts_ms": int(fr["last_ts"]) if fr and fr["last_ts"] else None,
            },
        }
        return summary

    def _stakers(self, vaddr):
        rows = self.server.db.latest_staker_rows(vaddr)
        return {"stakers": [_staker_json(r) for r in rows]}

    def _cycles(self, vaddr, params):
        limit = _int_param(params, "limit", 50)
        before = _int_param(params, "before", None) if "before" in params else None
        if before is not None and before < 1:
            before = None
        rows, has_more = self.server.db.recent_cycles_before(
            vaddr, limit, before
        )
        return {"cycles": [_cycle_json(r) for r in rows], "has_more": has_more}

    def _shares(self, vaddr, cid):
        db = self.server.db
        cycle = db.get_cycle(cid)
        if cycle is None:
            return {"error": "cycle not found", "cycle_id": cid}
        shares = db.cycle_shares(cid)
        return {
            "cycle": _cycle_json(cycle),
            "shares": [_share_json(r) for r in shares],
        }

    def _rewards(self, saddr, params):
        limit = _int_param(params, "limit", 50)
        before = _int_param(params, "before", None) if "before" in params else None
        if before is not None and before < 1:
            before = None
        rows, has_more = self.server.db.staker_rewards_before(
            self.server.cfg.validator_addr, saddr, limit, before
        )
        return {"rewards": [_reward_json(r) for r in rows], "has_more": has_more}

    # ------------------------------------------------------------------ helpers

    def _parse_path(self, path):
        raw = path.split("?", 1)
        path_only = raw[0]
        query = raw[1] if len(raw) > 1 else ""
        params = {}
        if query:
            for pair in query.split("&"):
                if "=" in pair:
                    k, _, v = pair.partition("=")
                    params.setdefault(k, []).append(v)

        if path_only == "/api/health":
            return {"kind": "health", "params": params}
        if path_only == "/api/verify":
            return {"kind": "verify", "params": params}
        def unq(s):
            return urllib.parse.unquote(s)

        m = re.match(
            r"^/api/validators/(?P<vaddr>[^/]+)/summary$", path_only
        )
        if m:
            return {"kind": "summary", "params": {"vaddr": unq(m.group("vaddr"))}}
        m = re.match(
            r"^/api/validators/(?P<vaddr>[^/]+)/stakers$", path_only
        )
        if m:
            return {"kind": "stakers", "params": {"vaddr": unq(m.group("vaddr"))}}
        m = re.match(
            r"^/api/validators/(?P<vaddr>[^/]+)/cycles$", path_only
        )
        if m:
            merged = dict(params)
            merged.update({"vaddr": unq(m.group("vaddr"))})
            return {"kind": "cycles", "params": merged}
        m = re.match(
            r"^/api/validators/(?P<vaddr>[^/]+)/cycles/(?P<cid>[0-9]+)/shares$",
            path_only,
        )
        if m:
            return {
                "kind": "shares",
                "params": {"vaddr": unq(m.group("vaddr")), "cid": m.group("cid")},
            }
        m = re.match(
            r"^/api/validators/(?P<vaddr>[^/]+)/stakers/(?P<saddr>[^/]+)/rewards$",
            path_only,
        )
        if m:
            merged = dict(params)
            merged.update({
                "vaddr": unq(m.group("vaddr")),
                "saddr": unq(m.group("saddr")),
            })
            return {"kind": "rewards", "params": merged}
        return None

    def _send_json(self, status, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _int_param(params, key, default):
    val = _query(params, key, None)
    if val is None:
        return default
    try:
        return max(1, min(int(val), 500))
    except ValueError:
        return default


def _row_dict(row):
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def _cycle_json(r):
    return {
        "id": r["id"],
        "validator": r["validator"],
        "opened_block": r["opened_block"],
        "closed_block": r["closed_block"],
        "available_luna": r["available_luna"],
        "reserve_luna": r["reserve_luna"],
        "held_luna": r["held_luna"],
        "total_luna": r["total_luna"],
        "status": r["status"],
        "opened_at_ms": r["opened_at_ms"],
        "closed_at_ms": r["closed_at_ms"],
    }


def _staker_json(r):
    return {
        "validator": r["validator"],
        "address": r["address"],
        "fetched_at_ms": r["fetched_at_ms"],
        "balance_luna": r["balance_luna"],
        "inactive_luna": r["inactive_luna"],
    }


def _share_json(r):
    payload = {
        "cycle_id": r["cycle_id"],
        "staker_address": r["staker_address"],
        "expected_luna": r["expected_luna"],
        "actual_luna": r["actual_luna"],
        "tx_hash": r["tx_hash"],
        "ok": bool(r["ok"]),
    }
    if r["reason"]:
        payload["reason"] = r["reason"]
    return payload


def _reward_json(r):
    return {
        "staker_address": r["staker_address"],
        "block": r["block"],
        "amount_luna": r["amount_luna"],
        "tx_hash": r["tx_hash"],
        "ts_ms": r["ts_ms"],
    }


def serve(db, cfg, bind="0.0.0.0", port=None):
    port = cfg.port if port is None else port
    server = APIServer((bind, port), db, cfg)
    return server
