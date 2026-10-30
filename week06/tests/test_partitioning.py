"""Partitioning checks using the supplied Node and protobuf interfaces.

Run from week06: python -B -m unittest discover -s tests -v
"""
import hashlib
import unittest

from platform import node_pb2
from platform.node import Node
from platform.util import RpcFailed


class PartitioningTests(unittest.TestCase):
    def setUp(self):
        peers = {f"n{i}": ("127.0.0.1", 9600 + i) for i in range(3)}
        self.nodes = {
            name: Node(name, host, port, peers)
            for name, (host, port) in peers.items()
        }
        for node in self.nodes.values():
            node.rpc = self.rpc_from(node)
        self.plugin = self.nodes["n0"].plugins["partitioning"]

    def rpc_from(self, source):
        def rpc(peer, method, request):
            if peer in source.drop_to or peer not in self.nodes:
                raise RpcFailed("unreachable")
            return getattr(self.nodes[peer], method)(request, None)
        return rpc

    def add_node(self, partitioning="SLOTS"):
        peers = dict(self.nodes["n0"].peers)
        peers["n3"] = ("127.0.0.1", 9603)
        joining = Node("n3", "127.0.0.1", 9603, peers)
        joining.rpc = self.rpc_from(joining)
        self.nodes["n3"] = joining
        return self.nodes["n0"].AddNode(node_pb2.AddNodeRequest(
            node_id="n3", host="127.0.0.1", port=9603,
            modes=node_pb2.Modes(partitioning=partitioning),
        ), None)

    def pin(self, key, peer, partitioning="DIRECTORY"):
        return self.nodes["n0"].AddNode(node_pb2.AddNodeRequest(
            node_id=peer, pin_key=key,
            modes=node_pb2.Modes(partitioning=partitioning),
        ), None)

    def test_owners_agree_and_hash_mod_matches_sha256(self):
        for i in range(256):
            key = bytes([i])
            expected = f"n{int.from_bytes(hashlib.sha256(key).digest(), 'big') % 3}"
            self.assertEqual(self.plugin.owner(self.nodes["n0"], key, "HASH_MOD"), expected)
            for selected in ("HASH_MOD", "SLOTS", "KEY_RANGE", "DIRECTORY", "ITEM", ""):
                owners = {self.plugin.owner(node, key, selected) for node in self.nodes.values()}
                self.assertEqual(len(owners), 1)

    def test_range_empty_and_binary_keys(self):
        node = self.nodes["n0"]
        self.assertEqual(self.plugin.owner(node, b"", "KEY_RANGE"), "n0")
        self.assertEqual(self.plugin.owner(node, b"aa1", "KEY_RANGE"), "n1")
        self.assertEqual(self.plugin.owner(node, b"ab2", "KEY_RANGE"), "n1")
        self.assertEqual(self.plugin.owner(node, b"\xff\x00", "KEY_RANGE"), "n2")

    def test_default_and_item_use_slots_and_ignore_pins(self):
        node = self.nodes["n0"]
        key = b"key"
        owner = self.plugin.owner(node, key, "SLOTS")
        target = next(peer for peer in node.nodes if peer != owner)
        self.assertTrue(self.pin(key, target).ok)
        for selected in ("", "ITEM", "SLOTS"):
            self.assertEqual(self.plugin.owner(node, key, selected), owner)
        self.assertEqual(self.plugin.owner(node, key, "DIRECTORY"), target)
        node.directory[key] = "removed"
        self.assertEqual(self.plugin.owner(node, key, "DIRECTORY"), owner)

    def test_remapping_and_membership_propagation(self):
        node = self.nodes["n0"]
        keys = [f"k{i}".encode() for i in range(4096)]
        before = {selected: [self.plugin.owner(node, key, selected) for key in keys]
                  for selected in ("HASH_MOD", "SLOTS")}
        self.assertTrue(self.add_node().ok)
        for member in self.nodes.values():
            self.assertEqual(member.nodes, ["n0", "n1", "n2", "n3"])
            self.assertEqual(member.peers["n3"], ("127.0.0.1", 9603))
            self.assertFalse(member.store)
        fractions = {}
        for selected, old in before.items():
            new = [self.plugin.owner(node, key, selected) for key in keys]
            fractions[selected] = sum(a != b for a, b in zip(old, new)) / len(keys)
            if selected == "SLOTS":
                self.assertTrue(all(a == b or b == "n3" for a, b in zip(old, new)))
        self.assertTrue(0.70 < fractions["HASH_MOD"] < 0.80, fractions)
        # Only 32 slots: allow coarse variation around the expected quarter.
        self.assertTrue(0.10 < fractions["SLOTS"] < 0.40, fractions)

    def test_rebalance_preserves_bytes_in_every_mode(self):
        for selected in ("HASH_MOD", "SLOTS", "KEY_RANGE", "DIRECTORY", "ITEM"):
            with self.subTest(mode=selected):
                self.setUp()
                modes = node_pb2.Modes(partitioning=selected)
                values = {bytes([i * 11]): b"\x00\xff" + bytes([i]) for i in range(24)}
                for key, value in values.items():
                    owner = self.plugin.owner(self.nodes["n0"], key, selected)
                    self.assertTrue(self.nodes[owner].Write(
                        node_pb2.WriteRequest(key=key, value=value, modes=modes), None).ok)
                self.assertTrue(self.add_node(selected).ok)
                for node in self.nodes.values():
                    node.Rebalance(node_pb2.RebalanceRequest(modes=modes), None)
                for key, value in values.items():
                    owner = self.plugin.owner(self.nodes["n0"], key, selected)
                    response = self.nodes[owner].Read(node_pb2.ReadRequest(key=key, modes=modes), None)
                    self.assertTrue(response.ok and response.found)
                    self.assertEqual(response.value, value)
                    self.assertEqual(response.served_by, owner)
                    self.assertEqual([n.node_id for n in self.nodes.values() if key in n.store], [owner])

    def test_pin_survives_join_and_moves_bytes(self):
        modes = node_pb2.Modes(partitioning="DIRECTORY")
        self.nodes["n0"].store[b"k"] = b"pinned"
        self.assertTrue(self.pin(b"k", "n1").ok)
        self.assertTrue(self.add_node().ok)
        for node in self.nodes.values():
            self.assertEqual(node.directory[b"k"], "n1")
            self.assertEqual(self.plugin.owner(node, b"k", "DIRECTORY"), "n1")
            node.Rebalance(node_pb2.RebalanceRequest(modes=modes), None)
        self.assertNotIn(b"k", self.nodes["n0"].store)
        self.assertEqual(self.nodes["n1"].store[b"k"], b"pinned")
        self.assertTrue(self.pin(b"k", "n2").ok)
        self.assertTrue(all(n.directory[b"k"] == "n2" for n in self.nodes.values()))

    def test_invalid_pins_and_membership_do_not_mutate_state(self):
        self.assertFalse(self.pin(b"k", "n1", "ITEM").ok)
        self.assertFalse(self.pin(b"k", "n9").ok)
        self.assertFalse(self.nodes["n0"].AddNode(node_pb2.AddNodeRequest(node_id="n9"), None).ok)
        self.assertTrue(all(not n.directory and len(n.nodes) == 3 for n in self.nodes.values()))

    def test_failed_transfer_keeps_local_copy_then_retry_moves_it(self):
        node = self.nodes["n0"]
        node.store[b"k"] = b""
        self.assertTrue(self.pin(b"k", "n1").ok)
        modes = node_pb2.Modes(partitioning="DIRECTORY")
        node.drop_to.add("n1")
        self.plugin.rebalance(node, modes)
        self.assertIn(b"k", node.store)
        node.drop_to.clear()
        original_rpc = node.rpc
        node.rpc = lambda *args: node_pb2.WriteResponse(ok=False)
        self.plugin.rebalance(node, modes)
        self.assertIn(b"k", node.store)
        node.rpc = original_rpc
        self.plugin.rebalance(node, modes)
        self.assertNotIn(b"k", node.store)
        self.assertEqual(self.nodes["n1"].store[b"k"], b"")

    def test_concurrent_local_update_is_not_deleted(self):
        node = self.nodes["n0"]
        node.store[b"k"] = b"old"
        self.assertTrue(self.pin(b"k", "n1").ok)
        original_rpc = node.rpc
        def updating_rpc(peer, method, request):
            response = original_rpc(peer, method, request)
            with node.lock:
                node.store[b"k"] = b"new"
            return response
        node.rpc = updating_rpc
        self.plugin.rebalance(node, node_pb2.Modes(partitioning="DIRECTORY"))
        self.assertEqual(node.store[b"k"], b"new")

    def test_failed_broadcast_reports_failure_and_reaches_other_peers(self):
        self.nodes["n0"].drop_to.add("n1")
        self.nodes["n2"].drop_to.add("n1")
        self.assertFalse(self.pin(b"k", "n2").ok)
        self.assertEqual(self.nodes["n2"].directory[b"k"], "n2")
        self.assertNotIn(b"k", self.nodes["n1"].directory)


if __name__ == "__main__":
    unittest.main()
