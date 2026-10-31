"""When Write returns: send copies, wait for W, time out if a node is paused.

In the readings: synchronous / asynchronous / hybrid (quorum) replication —
Think Distributed Systems §8.4.3; Database Internals ch. 11.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase
from platform.util import mode, RpcFailed


class Plugin(PluginBase):
    """Write completion plugin: spread, repair, on_read."""

    def __init__(self):
        self.pending = {}
        # peer -> key -> ReplicateRequest


    def spread(
        self,
        node: Node,
        key: bytes,
        value: bytes,
        owner: str,
        modes: node_pb2.Modes,
    ) -> node_pb2.WriteResponse:
        
        """Install locally (via the replication plugin), send copies, wait for W acks.

        W comes from the current membership ``len(node.nodes)``: 1, majority
        (``N//2 + 1``), or all N. Return a WriteResponse: ok if W members acked,
        otherwise ``ack timeout``. Remember failed copies so ``repair`` / ``on_read``
        can finish an incomplete write after the peer is reachable again.
        """
        selected = mode(modes, "write_completion") or "ACK_ONE"
        n = len(node.nodes)

        if selected == "ACK_ONE":
            required = 1
        elif selected == "ACK_QUORUM":
            required = n // 2 + 1
        elif selected == "ACK_ALL":
            required = n
        else:
            return node_pb2.WriteResponse(
                ok = False,
                error = "invalid write completion mode",
                owner = owner,
            )  

        # adding try/eccept block to handle the case where replication plugin is not implemented yet
        try: 
            request = node.plugins["replication"].install(node, key, value, modes)

        except NotImplementedError:
                node._local_write(key, value)
                request = node_pb2.ReplicateRequest(
                    key = key, 
                    value = value, 
                    kind = "STATE",
                    index = node.last_applied,
                    source = node.node_id,
                    modes = modes,
                )

        # The local Write counts as the first acknowledgment.
        acked = [node.node_id]

        # Send the copy to every other member.
        for peer in node.other_nodes():
            try:
                response = node.rpc(peer, "Replicate", request)

                if response.ok:
                    acked.append(peer)
                    self.pending.get(peer, {}).pop(key, None)
                else:
                    self.pending.setdefault(peer, {})[key] = request

            except RpcFailed:
                # Remember this copy for a later repair.
                self.pending.setdefault(peer, {})[key] = request

        # Success depends on the number of acknowledgments received.
        success = len(acked) >= required

        return node_pb2.WriteResponse(
            ok = success,
            error = "" if success else "ack timeout",
            owner = owner,
            acked_by = acked,
        )

    def repair(self, node: Node, modes: node_pb2.Modes) -> None:
        """Hinted handoff: retry copies that timed out while a peer was paused."""

        for peer, copies in list(self.pending.items()):
            for key, request in list(copies.items()):
                try:
                    response = node.rpc(peer, "Replicate", request)

                    if response.ok:
                        copies.pop(key, None)

                except RpcFailed:
                    # Keep the copy pending until the peer recovers.
                    continue

            if not copies:
                self.pending.pop(peer, None)

    def on_read(
        self, 
        node: Node,
        request: node_pb2.ReadRequest,
        owner: str
    ) -> node_pb2.ReadResponse | None:
        
        """Read repair: if this node is missing ``key``, pull from a peer and store it."""

        key = request.key

        # Existing local data can be handled by the starter.
        with node.lock:
            if key in node.store:
                return None

        # avoid requesting missing data from ourselves.
        if owner == node.node_id:
            return None

        # try to read from the owner node, if it fails, return None so the starter can handle it
        try:
            response = node.rpc(owner, "Read", request)

            if response.ok and response.found:
                node._local_write(key, response.value)

                return node_pb2.ReadResponse(
                    ok = True,
                    value = response.value,
                    found = True,
                    served_by = node.node_id,
                )

        except RpcFailed:
            # The starter can handle an unsuccessful Read.
            pass

        return None
