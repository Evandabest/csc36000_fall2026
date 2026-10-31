
import unittest
from unittest.mock import MagicMock

from platform import node_pb2
from platform.plugins.write_completion import Plugin


class TestWriteCompletion(unittest.TestCase):

    def setUp(self):
        self.plugin = Plugin()

        self.node = MagicMock()
        self.node.node_id = "n0"
        self.node.nodes = ["n0", "n1", "n2"]
        self.node.other_nodes.return_value = ["n1", "n2"]
        self.node.last_applied = 0

        # Simulate the unfinished replication plugin.
        self.node.plugins = {
            "replication": MagicMock()
        }
        self.node.plugins["replication"].install.side_effect = (
            NotImplementedError
        )

        # A successful replication acknowledgment.
        self.node.rpc.return_value = node_pb2.ReplicateResponse(
            ok=True
        )

    def write(self, selected_mode):
        modes = node_pb2.Modes(
            write_completion = selected_mode
        )

        return self.plugin.spread(
            self.node,
            b"alpha",
            b"one",
            "n0",
            modes,
        )

    def test_ack_one(self):
        response = self.write("ACK_ONE")

        self.assertTrue(response.ok)
        self.assertIn("n0", response.acked_by)

    def test_ack_quorum(self):
        response = self.write("ACK_QUORUM")

        self.assertTrue(response.ok)
        self.assertGreaterEqual(
            len(response.acked_by), 2
        )

    def test_ack_all(self):
        response = self.write("ACK_ALL")

        self.assertTrue(response.ok)
        self.assertEqual(
            len(response.acked_by), 3
        )

    def test_ack_all_timeout(self):
        def rpc(peer, method, request):
            if peer == "n2":
                from platform.util import RpcFailed
                raise RpcFailed("paused")

            return node_pb2.ReplicateResponse(ok=True)

        self.node.rpc.side_effect = rpc

        response = self.write("ACK_ALL")

        self.assertFalse(response.ok)
        self.assertEqual(response.error, "ack timeout")

    def test_ack_quorum_timeout(self):
        from platform.util import RpcFailed

        self.node.rpc.side_effect = RpcFailed("paused")

        response = self.write("ACK_QUORUM")

        self.assertFalse(response.ok)
        self.assertEqual(response.error, "ack timeout")

    def test_repair(self):
        from platform.util import RpcFailed

        self.node.rpc.side_effect = RpcFailed("paused")

        self.write("ACK_ALL")

        # Simulate recovery.
        self.node.rpc.side_effect = None
        self.node.rpc.return_value = (
            node_pb2.ReplicateResponse(ok=True)
        )

        self.plugin.repair(
            self.node,
            node_pb2.Modes()
        )

        self.assertEqual(self.plugin.pending, {})

    def test_invalid_mode(self):
        response = self.write("INVALID")

        self.assertFalse(response.ok)
        self.assertIn("invalid", response.error)


if __name__ == "__main__":
    unittest.main()
