"""Which node stores a key, and what AddNode / pin do to that map.

In the readings: partitioning / sharding — Think Distributed Systems ch. 7;
Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

import hashlib

from platform import node_pb2
from platform.node import Node
from platform.util import RpcFailed, mode

from .base import PluginBase


class Plugin(PluginBase):
    @staticmethod
    def _hash(data: bytes) -> int:
        return int.from_bytes(hashlib.sha256(data).digest(), "big")

    def _hash_mod(self, nodes: list[str], key: bytes) -> str:
        return nodes[self._hash(key) % len(nodes)]

    @staticmethod
    def _key_range(nodes: list[str], key: bytes) -> str:
        index = min(key[0] * len(nodes) // 256, len(nodes) - 1) if key else 0
        return nodes[index]

    def _slots(self, nodes: list[str], key: bytes, n_slots: int) -> str:
        slot = self._hash(key) % n_slots
        # A delimited decimal slot makes the hash input unambiguous.
        prefix = str(slot).encode("ascii") + b":"
        return max(nodes, key=lambda peer: (self._hash(prefix + peer.encode("utf-8")), peer))

    def owner(self, node: Node, key: bytes, mode: str) -> str:
        """Which node id should own ``key`` under this partitioning mode.

        HASH_MOD remaps most keys when membership changes; SLOTS moves about
        ``1/(N+1)``; KEY_RANGE keeps neighbors together; DIRECTORY honors pin;
        ITEM ignores pin. ``N`` is ``len(node.nodes)``.
        """
        selected = mode.strip() or "SLOTS"
        with node.lock:
            nodes = list(node.nodes)
            pinned = node.directory.get(key) if selected == "DIRECTORY" else None
            n_slots = node.n_slots
        if not nodes:
            return node.node_id
        if selected == "HASH_MOD":
            return self._hash_mod(nodes, key)
        if selected == "KEY_RANGE":
            return self._key_range(nodes, key)
        if selected == "DIRECTORY" and pinned in nodes:
            return pinned
        if selected in ("SLOTS", "DIRECTORY", "ITEM"):
            return self._slots(nodes, key, n_slots)
        raise ValueError(f"unknown partitioning mode: {selected}")

    def on_add_node(self, node: Node, request: node_pb2.AddNodeRequest) -> node_pb2.StatusResponse:
        """Disseminate membership / pins; leave physical movement to rebalance."""
        accepted = True
        changed = False
        pins = []
        with node.lock:
            if request.pin_key:
                if mode(request.modes, "partitioning") != "DIRECTORY" or request.node_id not in node.nodes:
                    accepted = False
                elif node.directory.get(request.pin_key) != request.node_id:
                    node.directory[request.pin_key] = request.node_id
                    changed = True
            elif not request.node_id or not request.host or not 0 < request.port <= 65535:
                accepted = False
            else:
                address = (request.host, int(request.port))
                changed = request.node_id not in node.nodes or node.peers.get(request.node_id) != address
                if request.node_id not in node.nodes:
                    node.nodes.append(request.node_id)
                    node.nodes.sort(key=lambda peer: (len(peer), peer))
                node.peers[request.node_id] = address
                if changed:
                    pins = list(node.directory.items())
            peers = [peer for peer in node.nodes if peer != node.node_id]

        # Commit locally before broadcasting so duplicate requests stop cycles.
        # Never hold node.lock across RPCs (including status_now).
        if changed:
            for peer in peers:
                try:
                    if not node.rpc(peer, "AddNode", request).ok:
                        accepted = False
                except RpcFailed:
                    accepted = False

            # Joining nodes can already know their own id, but still need pins
            # created before they joined, even if this add used another mode.
            for key, peer in pins:
                pin = node_pb2.AddNodeRequest(
                    node_id=peer, pin_key=key,
                    modes=node_pb2.Modes(partitioning="DIRECTORY"),
                )
                try:
                    if not node.rpc(request.node_id, "AddNode", pin).ok:
                        accepted = False
                except RpcFailed:
                    accepted = False

        status = node.status_now()
        status.ok = accepted
        return status

    def rebalance(self, node: Node, modes: node_pb2.Modes) -> None:
        """Send keys this node holds to their current owner if that owner is someone else."""
        partitioning_mode = mode(modes, "partitioning")
        with node.lock:
            items = list(node.store.items())
        for key, value in items:
            owner = self.owner(node, key, partitioning_mode)
            if owner == node.node_id:
                continue
            request = node_pb2.WriteRequest(key=key, value=value, modes=modes)
            try:
                response = node.rpc(owner, "Write", request)
            except RpcFailed:
                continue
            if response.ok:
                # Recheck placement and value so a pin change or a concurrent
                # local write during the RPC does not discard fresh bytes.
                if self.owner(node, key, partitioning_mode) != owner:
                    continue
                with node.lock:
                    if node.store.get(key) == value:
                        del node.store[key]
