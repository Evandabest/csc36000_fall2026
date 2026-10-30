"""Run from week06/: python -m unittest discover -s tests -v."""
from concurrent.futures import ThreadPoolExecutor
import unittest

import grpc

from platform import node_pb2 as pb, node_pb2_grpc
from platform.node import Node


class RequestRoutingTests(unittest.TestCase):
    def setUp(self):
        self.servers = []
        peers = {}
        for name in ("n0", "n1", "n2"):
            server = grpc.server(ThreadPoolExecutor(max_workers=12))
            port = server.add_insecure_port("127.0.0.1:0")
            peers[name] = ("127.0.0.1", port)
            self.servers.append(server)
        self.nodes = {}
        for (name, (_, port)), server in zip(peers.items(), self.servers):
            node = Node(name, "127.0.0.1", port, peers)
            self.nodes[name] = node
            node_pb2_grpc.add_NodeServicer_to_server(node, server)
            server.start()
        self.channel = grpc.insecure_channel("%s:%s" % peers["n1"])
        self.client = node_pb2_grpc.NodeStub(self.channel)

    def tearDown(self):
        self.channel.close()
        for server in self.servers:
            server.stop(0).wait()

    def write(self, routing="FORWARD", **kwargs):
        return self.client.Write(pb.WriteRequest(
            key=b"alpha", value=b"one", modes=pb.Modes(request_routing=routing),
            **kwargs
        ), timeout=5)

    def read(self, routing="FORWARD", **kwargs):
        return self.client.Read(pb.ReadRequest(
            key=b"alpha", modes=pb.Modes(request_routing=routing), **kwargs
        ), timeout=5)

    def test_forward_write_and_read(self):
        self.assertTrue(self.write().ok)
        self.assertEqual(self.nodes["n0"].store[b"alpha"], b"one")
        response = self.read()
        self.assertTrue(response.ok and response.found)
        self.assertEqual((response.value, response.served_by), (b"one", "n0"))
        self.assertNotIn(b"alpha", self.nodes["n1"].store)

    def test_empty_mode_defaults_to_forward(self):
        self.assertTrue(self.write("").ok)

    def test_client_aware_rejects_without_storing(self):
        self.assertEqual(self.write("CLIENT_AWARE").error, "not owner")
        self.assertEqual(self.read("CLIENT_AWARE").error, "not owner")
        self.assertTrue(all(not node.store for node in self.nodes.values()))

    def test_owner_handles_request_locally(self):
        request = pb.WriteRequest(key=b"alpha", value=b"one",
                                  modes=pb.Modes(request_routing="CLIENT_AWARE"))
        self.assertTrue(self.nodes["n0"].Write(request, None).ok)

    def test_hop_limit_and_last_permitted_hop(self):
        self.assertEqual(self.write(hop=8).error, "hop limit")
        self.assertEqual(self.read(hop=8).error, "hop limit")
        self.assertTrue(self.write(hop=7).ok)
        self.assertTrue(self.read(hop=7).found)

    def test_isolation_and_recovery(self):
        self.nodes["n1"].drop_to = {"n0", "n2"}
        self.assertEqual(self.write().error, "owner unreachable")
        self.assertEqual(self.read().error, "owner unreachable")
        self.assertFalse(self.nodes["n0"].store)
        self.nodes["n1"].drop_to.clear()
        self.assertTrue(self.write().ok)

    def test_stale_directory_write_and_read(self):
        self.nodes["n1"].directory[b"alpha"] = "n2"
        self.assertTrue(self.write("DIRECTORY_TIER").ok)
        response = self.read("DIRECTORY_TIER")
        self.assertEqual((response.value, response.served_by), (b"one", "n0"))
        self.assertFalse(self.nodes["n2"].store)

    def test_unreachable_directory_destination_retries_owner(self):
        self.nodes["n1"].directory[b"alpha"] = "n2"
        self.nodes["n1"].drop_to = {"n2"}
        self.assertTrue(self.write("DIRECTORY_TIER").ok)
        self.assertTrue(self.read("DIRECTORY_TIER").found)

    def test_directory_attempts_are_bounded(self):
        node = self.nodes["n1"]
        node.directory[b"alpha"] = "n2"
        node.drop_to = {"n0", "n2"}
        original_rpc = node.rpc
        calls = []

        def counted_rpc(peer, method, request):
            calls.append((peer, request.hop, request.modes.request_routing))
            return original_rpc(peer, method, request)

        node.rpc = counted_rpc
        self.assertEqual(self.write("DIRECTORY_TIER").error, "owner unreachable")
        self.assertEqual(calls, [("n2", 1, "CLIENT_AWARE"),
                                 ("n0", 1, "CLIENT_AWARE")])

    def test_forwarding_cycle_stops(self):
        class FixedOwner:
            def __init__(self, owner):
                self.target = owner

            def owner(self, *args):
                return self.target

        self.nodes["n0"].plugins["partitioning"] = FixedOwner("n1")
        self.nodes["n1"].plugins["partitioning"] = FixedOwner("n0")
        self.assertEqual(self.write().error, "hop limit")
        self.assertEqual(self.read().error, "hop limit")

    def test_invalid_mode(self):
        self.assertEqual(self.write("UNKNOWN").error, "invalid request routing mode")
        self.assertEqual(self.read("UNKNOWN").error, "invalid request routing mode")


if __name__ == "__main__":
    unittest.main()
