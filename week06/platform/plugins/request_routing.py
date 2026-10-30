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
        return self._route(node, request, owner, "Write")

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Same idea for Read. Return a ReadResponse or None."""
        return self._route(node, request, owner, "Read")

    def _route(self, node, request, owner, method):
        """Use the same routing steps for Write and Read."""
        if method == "Write":
            failure = node_pb2.WriteResponse(ok=False, owner=owner)
        else:
            failure = node_pb2.ReadResponse(ok=False, served_by=node.node_id)

        routing = mode(request.modes, "request_routing") or "FORWARD"
        if routing not in ("FORWARD", "DIRECTORY_TIER", "CLIENT_AWARE"):
            failure.error = "invalid request routing mode"
            return failure
        if owner == node.node_id:
            return None
        if routing == "CLIENT_AWARE":
            failure.error = "not owner"
            return failure
        if request.hop >= MAX_HOP:
            failure.error = "hop limit"
            return failure

        destination = owner
        attempts = 1
        if routing == "DIRECTORY_TIER":
            destination = node.directory.get(request.key, owner)
            attempts = 2

        for attempt in range(attempts):
            forwarded = copy_with_hop(request)
            if routing == "DIRECTORY_TIER":
                # The destination must reject the request if it is not the owner.
                forwarded.modes.request_routing = "CLIENT_AWARE"
            try:
                response = node.rpc(destination, method, forwarded)
            except RpcFailed:
                failure.error = "owner unreachable"
                response = failure

            if response.ok or response.error not in ("not owner", "owner unreachable"):
                return response
            # On the second attempt, use the owner provided by partitioning.
            destination = owner
        return response
