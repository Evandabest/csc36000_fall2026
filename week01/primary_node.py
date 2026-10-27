#!/usr/bin/env python3
"""
primary_node.py

Primary coordinator that:
1) Maintains an in-memory registry of secondary nodes (registered by secondary_node.py)
2) Distributes prime-range computation requests to registered secondary nodes
3) Aggregates results in memory and returns a final result (count or list sample)

Endpoints
---------
GET  /health
GET  /nodes
POST /register
POST /compute
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from primes_in_range import get_primes


class Registry:
    def __init__(self, ttl_s: int = 3600):
        self.ttl_s = ttl_s
        self.lock = threading.Lock()
        self.nodes: Dict[str, Dict[str, Any]] = {}

    def upsert(self, node: Dict[str, Any]) -> Dict[str, Any]:
        node_id = str(node["node_id"])
        now = time.time()
        record = {
            "node_id": node_id,
            "host": str(node["host"]),
            "port": int(node["port"]),
            "cpu_count": int(node.get("cpu_count", 1)),
            "last_seen": float(node.get("ts", now)),
            "registered_at": now,
        }
        with self.lock:
            if node_id in self.nodes:
                record["registered_at"] = self.nodes[node_id].get("registered_at", now)
            self.nodes[node_id] = record
            return record

    def active_nodes(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self.lock:
            stale = [nid for nid, rec in self.nodes.items() if (now - float(rec.get("last_seen", 0))) > self.ttl_s]
            for nid in stale:
                del self.nodes[nid]
            return list(self.nodes.values())


REGISTRY = Registry(ttl_s=120)


def _post_json(url: str, payload: Dict[str, Any], timeout_s: int = 60) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def split_into_slices(low: int, high: int, n: int) -> List[Tuple[int, int]]:
    if n <= 0:
        return []
    total = high - low
    base = total // n
    rem = total % n
    out = []
    start = low
    for i in range(n):
        size = base + (1 if i < rem else 0)
        end = start + size
        if start < end:
            out.append((start, end))
        start = end
    return out

"""

this fallback function will compute the number of primes on the local primary node if the secondary nodes fail. this function just calls get prime with the upper and lower bound without splitting into slices for the distributed approach

"""
def compute_local_fallback(
    sl: Tuple[int, int],
    *,
    mode: str,
    max_return_primes: int,
    failed_node: Dict[str, Any] | None = None,
    failure: Exception | None = None,
) -> Dict[str, Any]:
    """Compute a failed secondary slice directly on the primary."""
    low, high = sl
    t0 = time.perf_counter()
    max_prime = -1
    primes_truncated = False

    if mode == "count":
        total_primes = int(get_primes(low, high, return_list=False))
        primes: List[int] | None = None
    else:
        all_primes = list(get_primes(low, high, return_list=True))
        total_primes = len(all_primes)
        max_prime = all_primes[-1] if all_primes else -1
        primes = all_primes[:max_return_primes]
        primes_truncated = len(all_primes) > max_return_primes

    elapsed_s = time.perf_counter() - t0
    failed_node_id = failed_node.get("node_id") if failed_node else None
    result: Dict[str, Any] = {
        "node_id": "primary-fallback",
        "node": {"host": "local", "port": None, "cpu_count": 1},
        "slice": [low, high],
        "round_trip_s": 0.0,
        "node_elapsed_s": elapsed_s,
        "node_sum_chunk_s": elapsed_s,
        "total_primes": total_primes,
        "max_prime": max_prime,
        "primes": primes,
        "primes_truncated": primes_truncated,
        "fallback": True,
        "failed_node_id": failed_node_id,
    }
    if failure is not None:
        result["failure"] = str(failure)

    fallback_reason = (
        f"after secondary {failed_node_id} failed"
        if failed_node_id
        else "because no secondary was available"
    )
    print(f"[primary_node] Local fallback completed slice [{low}, {high}) {fallback_reason}")
    return result


def distributed_compute(payload: Dict[str, Any]) -> Dict[str, Any]:
    low = int(payload["low"])
    high = int(payload["high"])
    if high <= low:
        raise ValueError("high must be > low")

    mode = str(payload.get("mode", "count"))
    if mode not in ("count", "list"):
        raise ValueError("mode must be 'count' or 'list'")
    
    sec_exec = str(payload.get("secondary_exec", "processes"))
    if sec_exec not in ("single", "threads", "processes"):
        raise ValueError("secondary_exec must be single|threads|processes")

    sec_workers = payload.get("secondary_workers", None)
    if sec_workers is not None:
        sec_workers = int(sec_workers)

    max_return_primes = int(payload.get("max_return_primes", 5000))
    include_per_node = bool(payload.get("include_per_node", False))

    nodes = REGISTRY.active_nodes()
    if not nodes:
        raise ValueError("no active secondary nodes registered")
    
    chunk = int(payload.get("chunk", 500_000))

    nodes_sorted = sorted(nodes, key=lambda n: n["node_id"])
    slices = split_into_slices(low, high, len(nodes_sorted))
    nodes_sorted = nodes_sorted[:len(slices)]

    t0 = time.perf_counter()

    per_node_results: List[Dict[str, Any]] = []
    total_primes = 0
    primes_sample: List[int] = []
    primes_truncated = False
    max_prime = -1

    def call_node(node: Dict[str, Any], sl: Tuple[int, int]) -> Dict[str, Any]:
        host = node["host"]
        port = node["port"]
        url = f"http://{host}:{port}/compute"
        req = {
            "low": sl[0],
            "high": sl[1],
            "mode": mode,
            "chunk": chunk,
            "exec": sec_exec,
            "workers": sec_workers,
            "max_return_primes": max_return_primes if mode == "list" else 0,
            "include_per_chunk": False,
        }
        req = {k: v for k, v in req.items() if v is not None}

        t_call0 = time.perf_counter()
        resp = _post_json(url, req, timeout_s=3600)
        t_call1 = time.perf_counter()

        if not resp.get("ok"):
            raise RuntimeError(f"node {node['node_id']} error: {resp}")
        
        node_elapsed_s = float(resp.get("elapsed_seconds", 0.0))
        print(f"Node ID: {node['node_id']} completed in: {node_elapsed_s}")

        return {
            "node_id": node["node_id"],
            "node": {"host": host, "port": port, "cpu_count": node.get("cpu_count", 1)},
            "slice": list(sl),
            "round_trip_s": t_call1 - t_call0,
            "node_elapsed_s": node_elapsed_s,
            "node_sum_chunk_s": float(resp.get("sum_chunk_compute_seconds", 0.0)),
            "total_primes": int(resp.get("total_primes", 0)),
            "max_prime": int(resp.get("max_prime", -1)),
            "primes": resp.get("primes", None),
            "primes_truncated": bool(resp.get("primes_truncated", False)),
        }

    failed_secondaries: List[Dict[str, Any]] = []
    if nodes_sorted:
        with ThreadPoolExecutor(max_workers=min(32, len(nodes_sorted))) as ex:
            futures = {
                ex.submit(call_node, node, sl): (node, sl)
                for node, sl in zip(nodes_sorted, slices)
            }
            for future in as_completed(futures):
                node, sl = futures[future]
                try:
                    per_node_results.append(future.result())
                except Exception as exc:
                    failed_secondaries.append({
                        "node_id": node["node_id"],
                        "slice": list(sl),
                        "error": str(exc),
                    })
                    print(
                        f"[primary_node] Secondary {node['node_id']} failed for "
                        f"slice [{sl[0]}, {sl[1]}): {exc}"
                    )
                    per_node_results.append(
                        compute_local_fallback(
                            sl,
                            mode=mode,
                            max_return_primes=max_return_primes,
                            failed_node=node,
                            failure=exc,
                        )
                    )
    else:
        print("[primary_node] No active secondary nodes; computing request locally")
        per_node_results.append(
            compute_local_fallback(
                (low, high),
                mode=mode,
                max_return_primes=max_return_primes,
            )
        )

    per_node_results.sort(key=lambda r: r["slice"][0])

    for r in per_node_results:
        total_primes += int(r["total_primes"])
        max_prime = max(max_prime, int(r["max_prime"]))
        if mode == "list" and r.get("primes") is not None:
            ps = list(r["primes"])
            if len(primes_sample) < max_return_primes:
                remaining = max_return_primes - len(primes_sample)
                primes_sample.extend(ps[:remaining])
                if len(ps) > remaining:
                    primes_truncated = True
            else:
                primes_truncated = True
            if r.get("primes_truncated"):
                primes_truncated = True

    t1 = time.perf_counter()

    resp: Dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "range": [low, high],
        "nodes_used": len(nodes_sorted),
        "nodes_succeeded": len(nodes_sorted) - len(failed_secondaries),
        "fallback_used": any(bool(r.get("fallback")) for r in per_node_results),
        "fallback_slices": sum(bool(r.get("fallback")) for r in per_node_results),
        "failed_secondaries": failed_secondaries,
        "secondary_exec": sec_exec,
        "secondary_workers": sec_workers,
        "chunk": chunk,
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_node_compute_seconds": sum(float(r["node_elapsed_s"]) for r in per_node_results),
        "sum_node_round_trip_seconds": sum(float(r["round_trip_s"]) for r in per_node_results),
    }

    if mode == "list":
        resp["primes"] = primes_sample
        resp["primes_truncated"] = primes_truncated
        resp["max_return_primes"] = max_return_primes

    if include_per_node:
        resp["per_node"] = per_node_results

    return resp


class Handler(BaseHTTPRequestHandler):
    server_version = "PrimaryPrimeCoordinator/1.0"

    def _send_json(self, obj: Dict[str, Any], code: int = 200) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            return self._send_json({"ok": True, "status": "healthy"})
        if parsed.path == "/nodes":
            nodes = REGISTRY.active_nodes()
            nodes.sort(key=lambda n: n["node_id"])
            return self._send_json({"ok": True, "nodes": nodes, "ttl_s": REGISTRY.ttl_s})
        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            return self._send_json({"ok": False, "error": "invalid content-length"}, code=400)

        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except Exception as e:
            return self._send_json({"ok": False, "error": f"bad json: {e}"}, code=400)

        if parsed.path == "/register":
            for k in ("node_id", "host", "port"):
                if k not in payload:
                    return self._send_json({"ok": False, "error": f"missing field: {k}"}, code=400)
            rec = REGISTRY.upsert(payload)
            print(f"[primary_node] Added node: {payload} to registry")
            return self._send_json({"ok": True, "node": rec})

        if parsed.path == "/compute":
            try:
                for k in ("low", "high"):
                    if k not in payload:
                        raise ValueError(f"missing field: {k}")
                resp = distributed_compute(payload)
                return self._send_json(resp, code=200)
            except Exception as e:
                return self._send_json({"ok": False, "error": str(e)}, code=400)

        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def log_message(self, fmt, *args):
        return


def main() -> None:
    ap = argparse.ArgumentParser(description="Primary coordinator for distributed prime computation.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9200)
    ap.add_argument("--ttl", type=int, default=3600, help="Seconds to keep node registrations alive (default 3600).")
    args = ap.parse_args()

    global REGISTRY
    REGISTRY = Registry(ttl_s=max(10, int(args.ttl)))

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[primary_node] listening on http://{args.host}:{args.port}")
    print("  GET  /health")
    print("  GET  /nodes")
    print("  POST /register")
    print("  POST /compute")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[primary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        httpd.shutdown()
    finally:
        httpd.server_close()
        print("[primary_node] server stopped.")


if __name__ == "__main__":
    main()
