"""Who may accept a Write on this node, especially after a split.

In the readings: single-leader, multileader, leaderless — Think Distributed
Systems §8.4.5.

See week06/README.md. Useful imports: ``from platform.util import mode, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import MAX_HOP, RpcFailed, copy_with_hop, hop_count, mode

from .base import PluginBase


class Plugin(PluginBase):
    def admit(
        self, node: Node, request: node_pb2.WriteRequest, owner: str
    ) -> node_pb2.WriteResponse | None:
        """Drive the Write: accept here (return None), forward, or reject.

        ``None`` means this node continues and stores. A WriteResponse means
        the plugin already forwarded or failed (``not leader``, hop limit, …).
        """
        selected = mode(request.modes, "leader_replication") or "SINGLE_LEADER"
        if selected in ("LEADERLESS", "MULTI_LEADER"):
            return None
        if selected != "SINGLE_LEADER":
            return node_pb2.WriteResponse(
                ok=False, error="unknown leader replication mode", owner=owner
            )
        with node.lock:
            leader = node.leader
            members = tuple(node.nodes)
        if not leader or leader not in members:
            return node_pb2.WriteResponse(ok=False, error="not leader", owner=leader)
        if leader == node.node_id:
            return None
        if hop_count(request) >= MAX_HOP:
            return node_pb2.WriteResponse(ok=False, error="hop limit", owner=leader)
        try:
            return node.rpc(leader, "Write", copy_with_hop(request))
        except RpcFailed:
            # Never promote ourselves just because the leader cannot be reached.
            return node_pb2.WriteResponse(
                ok=False, error="not leader", owner=leader
            )

    def after_local_write(
        self,
        node: Node,
        request: node_pb2.WriteRequest,
        value: bytes,
        owner: str,
    ) -> None:
        """After a successful local Write: MULTI_LEADER copies to other leaders."""
        if mode(request.modes, "leader_replication") != "MULTI_LEADER":
            return
        # There is no separate multileader roster in the starter: all members
        # are eligible leaders. Replicate applies a copy without admitting a
        # new Write, so copies do not fan out recursively.
        with node.lock:
            peers = [peer for peer in node.nodes if peer != node.node_id]
            index = node.last_applied
        metadata = node._try(
            "conflicts", "export_meta", node, request.key, value,
            mode(request.modes, "conflicts"),
        )
        copy = node_pb2.ReplicateRequest(
            key=request.key, value=value, kind="STATE", index=index,
            source=node.node_id, modes=request.modes,
            meta=metadata if isinstance(metadata, bytes) else b"",
        )
        for peer in peers:
            try:
                node.rpc(peer, "Replicate", copy)
            except RpcFailed:
                # A disconnected leader must not prevent local acceptance.
                # Catch-up and reconciliation belong to the other plugins.
                continue
