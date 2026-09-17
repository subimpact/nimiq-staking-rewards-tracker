"""Shared fixtures for tracker tests: a fake HTTP RPC/data server."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tracker.config import STAKING_CONTRACT_ADDR

STAKER_A = "NQ01 AAAA AAAA AAAA AAAA AAAA AAAA AAAA AAAA"
STAKER_B = "NQ02 BBBB BBBB BBBB BBBB BBBB BBBB BBBB BBBB"
STAKER_C = "NQ03 CCCC CCCC CCCC CCCC CCCC CCCC CCCC CCCC"
VALIDATOR = "NQ08 ACT8 T0FE PTG8 P5RL H2S3 QGXH V15R NVXY"
REWARD_ADDR = VALIDATOR


class FakeRPC(BaseHTTPRequestHandler):
    def log_message(self, *args):  # noqa: A002
        pass

    def _send(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        req = json.loads(raw)
        method = req.get("method")
        params = req.get("params", [])
        if method == "getTransactionsByAddress":
            result = self.server.fake.txs(params[0])
        else:
            result = None
        self._send({"jsonrpc": "2.0", "result": result, "id": req.get("id")})

    def do_GET(self):
        if self.path.startswith("/api/stakers/"):
            result = self.server.fake.stakers_payload
        elif self.path.startswith("/api/validators"):
            result = self.server.fake.validators_payload
        else:
            result = None
        self._send(result)


class FakeServer:
    def __init__(self):
        import tempfile
        self.data_dir = tempfile.mkdtemp()
        self.txs = lambda addr: []
        self.stakers_payload = {"data": []}
        self.validators_payload = {"data": []}

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), FakeRPC)
        self.httpd.fake = self
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()

    def url(self):
        return "http://127.0.0.1:%s" % self.port

    # ---- fixture builders ----

    def set_stakers(self, rows):
        self.stakers_payload = {"data": rows}

    def set_validator(self, total_luna, num_stakers, deposit_luna=0):
        self.validators_payload = {
            "data": [{
                "total": total_luna,
                "numStakers": num_stakers,
                "deposit": deposit_luna,
            }]
        }

    def set_txs(self, tx_list):
        def handler(addr):
            return tx_list
        self.txs = handler

    def set_staged_txs(self, stages):
        """Serve one stage per get_transactions call, in order. This simulates
        the chain advancing between scheduler passes."""
        queue = list(stages)

        def handler(addr):
            return queue.pop(0) if queue else []
        self.txs = handler


def coinbase_tx(block, value_luna, tx_hash, ts=None):
    return {
        "blockNumber": block,
        "sender": "NQ81 C01N BASE 0000 0000 0000 0000 0000 0001",
        "recipient": REWARD_ADDR,
        "value": value_luna,
        "timestamp": ts if ts is not None else block * 1000,
        "hash": tx_hash,
    }


def restake_tx(block, to_addr, value_luna, tx_hash):
    return {
        "blockNumber": block,
        "sender": REWARD_ADDR,
        "recipient": to_addr,
        "value": value_luna,
        "timestamp": block * 1000,
        "hash": tx_hash,
    }


def staking_tx(block, value_luna, tx_hash):
    return restake_tx(block, STAKING_CONTRACT_ADDR, value_luna, tx_hash)


def staker_row(address, balance_luna, inactive=0):
    return {
        "address": address,
        "balance": balance_luna,
        "delegation": balance_luna,
        "inactiveBalance": inactive,
    }
