"""Run from week06: python3 -m unittest discover -s tests -v."""
import unittest
from concurrent.futures import ThreadPoolExecutor

import grpc
from platform import node_pb2 as pb, node_pb2_grpc
from platform.node import Node


class LeaderReplicationTests(unittest.TestCase):
    def setUp(self):
        self.servers = []
        self.nodes = {}
        peers = {}
        for name in ('n0', 'n1', 'n2', 'n3', 'n4'):
            server = grpc.server(ThreadPoolExecutor(max_workers=8))
            port = server.add_insecure_port('127.0.0.1:0')
            self.servers.append(server)
            peers[name] = ('127.0.0.1', port)
        for (name, addr), server in zip(peers.items(), self.servers):
            node = Node(name, *addr, peers)
            self.nodes[name] = node
            node_pb2_grpc.add_NodeServicer_to_server(node, server)
            server.start()

    def tearDown(self):
        for server in self.servers:
            server.stop(0).wait()

    def write(self, name, selected, value=b'one', hop=0):
        return self.nodes[name].Write(pb.WriteRequest(
            key=b'alpha', value=value, hop=hop,
            modes=pb.Modes(leader_replication=selected)), None)

    def test_single_leader_forwards_without_local_effect(self):
        response = self.write('n4', 'SINGLE_LEADER')
        self.assertTrue(response.ok)
        self.assertEqual(self.nodes['n0'].store[b'alpha'], b'one')
        self.assertNotIn(b'alpha', self.nodes['n4'].store)
        self.assertEqual(self.nodes['n4'].status_now().leader, 'n0')

    def test_isolated_followers_do_not_accept_or_promote(self):
        self.nodes['n4'].drop_to.add('n0')
        response = self.write('n4', 'SINGLE_LEADER')
        self.assertFalse(response.ok)
        self.assertEqual(response.error, 'not leader')
        self.assertFalse(self.nodes['n4'].store)
        self.assertEqual(self.nodes['n4'].leader, 'n0')

    def test_forward_limit_and_inconsistent_leaders(self):
        self.assertEqual(self.write('n4', 'SINGLE_LEADER', hop=8).error, 'hop limit')
        self.nodes['n0'].leader = 'n1'
        self.nodes['n1'].leader = 'n0'
        self.assertEqual(self.write('n0', 'SINGLE_LEADER').error, 'hop limit')
        self.assertTrue(all(not n.store for n in self.nodes.values()))

    def test_leaderless_split_accepts_conflicting_values(self):
        for node in self.nodes.values():
            node.drop_to = set(self.nodes) - {node.node_id}
        self.assertTrue(self.write('n1', 'LEADERLESS', b'L').ok)
        self.assertTrue(self.write('n4', 'LEADERLESS', b'R').ok)
        self.assertEqual(self.nodes['n1'].store[b'alpha'], b'L')
        self.assertEqual(self.nodes['n4'].store[b'alpha'], b'R')

    def test_multileader_copies_only_to_reachable_members(self):
        left, right = {'n0', 'n1'}, {'n2', 'n3', 'n4'}
        for name, node in self.nodes.items():
            node.drop_to = right if name in left else left
        self.assertTrue(self.write('n1', 'MULTI_LEADER', b'L').ok)
        self.assertTrue(self.write('n4', 'MULTI_LEADER', b'R').ok)
        for name in left:
            self.assertEqual(self.nodes[name].store[b'alpha'], b'L')
        for name in right:
            self.assertEqual(self.nodes[name].store[b'alpha'], b'R')
        for node in self.nodes.values():
            node.drop_to = set()
        self.assertTrue(self.write('n3', 'MULTI_LEADER', b'healed').ok)
        for node in self.nodes.values():
            self.assertEqual(node.store[b'alpha'], b'healed')

    def test_unknown_mode_rejected(self):
        self.assertFalse(self.write('n0', 'typo').ok)
        self.assertFalse(self.nodes['n0'].store)


if __name__ == '__main__':
    unittest.main()
