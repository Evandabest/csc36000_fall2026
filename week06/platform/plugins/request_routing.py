"""Write/Read that arrived on a node that does not own the key.

In the readings: request routing — Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import MAX_HOP, RpcFailed, copy_with_hop, mode

from .base import PluginBase


class Plugin(PluginBase):
    def on_write(
        self, node: Node, request: node_pb2.WriteRequest, owner: str
    ) -> node_pb2.WriteResponse | None:
        """If this node should not store the Write, forward, look up, or reject.

        Return a WriteResponse, or None to mean “handle it here.” Increment
        ``request.hop`` when forwarding; stop after 8. A stale directory lookup
        should retry once rather than drop the bytes.
        """
        if request.hop > MAX_HOP:
            return node_pb2.WriteResponse(ok=False, error="hop limit", owner=owner)
        if owner == node.node_id:
            return None
        routing = mode(request.modes, "request_routing") or "FORWARD"
        if routing == "CLIENT_AWARE":
            return node_pb2.WriteResponse(ok=False, error="not owner", owner=owner)

        target = owner
        if routing == "DIRECTORY_TIER":
            target = node.directory.get(request.key, owner)
        forwarded = request
        attempts = 2 if routing == "DIRECTORY_TIER" else 1
        for attempt in range(attempts):
            if forwarded.hop >= MAX_HOP:
                return node_pb2.WriteResponse(ok=False, error="hop limit", owner=target)
            forwarded = copy_with_hop(forwarded)
            if routing == "DIRECTORY_TIER":
                # Ask the chosen node to reject a stale lookup instead of forwarding.
                forwarded.modes.request_routing = "CLIENT_AWARE"
            try:
                response = node.rpc(target, "Write", forwarded)
            except RpcFailed:
                response = node_pb2.WriteResponse(ok=False, error="owner unreachable")
            if attempt == 0 and routing == "DIRECTORY_TIER" and response.error in (
                "not owner", "owner unreachable"
            ):
                target = response.owner or node._owner(
                    request.key, mode(request.modes, "partitioning")
                )
                continue
            return response

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Same idea for Read. Return a ReadResponse or None."""
        if request.hop > MAX_HOP:
            return node_pb2.ReadResponse(ok=False, error="hop limit", served_by=node.node_id)
        if owner == node.node_id:
            return None
        routing = mode(request.modes, "request_routing") or "FORWARD"
        if routing == "CLIENT_AWARE":
            return node_pb2.ReadResponse(ok=False, error="not owner", served_by=node.node_id)

        target = owner
        if routing == "DIRECTORY_TIER":
            target = node.directory.get(request.key, owner)
        forwarded = request
        attempts = 2 if routing == "DIRECTORY_TIER" else 1
        for attempt in range(attempts):
            if forwarded.hop >= MAX_HOP:
                return node_pb2.ReadResponse(ok=False, error="hop limit", served_by=node.node_id)
            forwarded = copy_with_hop(forwarded)
            if routing == "DIRECTORY_TIER":
                forwarded.modes.request_routing = "CLIENT_AWARE"
            try:
                response = node.rpc(target, "Read", forwarded)
            except RpcFailed:
                response = node_pb2.ReadResponse(ok=False, error="owner unreachable")
            if attempt == 0 and routing == "DIRECTORY_TIER" and response.error in (
                "not owner", "owner unreachable"
            ):
                # ReadResponse has no owner field, so look up the owner again.
                target = node._owner(request.key, mode(request.modes, "partitioning"))
                continue
            return response
