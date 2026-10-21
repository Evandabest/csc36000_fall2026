# Partitioning Design

**Student:** Atif Tausif

**CUNY alias:** <alias>

**Plugin:** `week06/platform/plugins/partitioning.py`

## Plan

I will implement `owner`, `on_add_node`, and `rebalance`. `owner` will decide which node stores a key. The node will call it on every Write, Read, and Status. `on_add_node` will handle `add-node` and `pin`. `rebalance` will move bytes that a node holds but no longer owns.

Every node will compute the owner by itself from the key and `node.nodes`, so all nodes must agree without talking to each other. For that reason I will hash with `hashlib` (MD5 of the key bytes) and not Python's `hash()`, which gives different results in each process. `N` is `len(node.nodes)`, and `node.nodes` is already sorted the same way on every node. An empty mode string will default to SLOTS.

## Modes and tradeoffs

| Mode | Situation | Planned Write behavior | Expected Read behavior | Tradeoff |
| --- | --- | --- | --- | --- |
| HASH_MOD | 24 keys written on a 3-node cluster, then `add-node n3`. | The owner is `nodes[md5(key) % N]`. The keys spread across n0, n1 and n2. After the add, N becomes 4. | After `rebalance`, a Read on the new owner returns the value. About 3/4 of the keys moved. | The simplest even spread, but adding a node moves most keys. |
| SLOTS | Same 24 keys, then `add-node n3`. | The key goes to slot `md5(key) % n_slots` (32 slots, from `cluster.json`). Each slot is owned by the node with the highest `md5(slot, node_id)` (rendezvous hashing). | After the add, only the slots that n3 now wins move, about 1/(N+1) = 1/4 of the keys. Reads on the other keys still hit the same node. | Fewer moves, but the spread is only as even as 32 slots allow, and the rule is more complex. |
| KEY_RANGE | Keys `aa1`, `ab2`, and one starting with byte `0xff`. | The owner is `nodes[first_byte * N // 256]`. `aa…` and `ab…` land on the same node. A key starting with `0xff` goes to the last node. | A Read of `aa1` and `ab2` is served by the same node. Range scans would stay on one node. | Neighbors stay together, but skewed keys (all starting with `a`) create a hot node. |
| DIRECTORY | `pin k n1`, then `add-node n3`, then `rebalance`. | `pin` records `k → n1` in `node.directory` on every node. Keys that are not pinned fall back to SLOTS. | `k` stays on n1 after the add. After `rebalance`, a Read of `k` on n1 returns the bytes. | Exact control per key, but every node has to store and agree on a directory entry. |
| ITEM | `pin k n1`. | `pin` returns `ok=False`. Ownership comes only from the key's hash (same rule as SLOTS). | A Read of `k` is served by the hash owner, not n1. | No directory to keep in sync, but you cannot place a hot key by hand. |

## Adding nodes, pins, and moving bytes

`on_add_node` handles two kinds of request:

- **Pin** (`pin_key` is set): only DIRECTORY accepts it. The target node has to be in `node.nodes`; if it isn't, the reply is `ok=False`. The node saves the pin and sends the same request to the other nodes with `node.rpc(peer, "AddNode", request)`. A node that already has the same pin does not send it on, so the broadcast ends. ITEM and every other mode reply `ok=False`.
- **Add node:** add the id to `node.nodes`, keeping the same sort as `node.py`, and its address to `node.peers`. Then send the request to the other nodes, skipping any node that already knows the new id. After that, call `rebalance`.

`rebalance` goes through `node.store`. For each key whose owner under the current mode is another node, it sends the bytes with `node.rpc(owner, "Write", ...)`, using the same partitioning mode. It deletes the local copy only after the owner replies `ok`, so a failed move does not lose bytes.

This plugin will choose ownership and move bytes only. Forwarding a request to the owner belongs to request_routing, and making copies belongs to replication and write_completion.

## A combination that should fail

- `pin k n1 --partitioning ITEM` returns `ok=False`.
- `pin k n9 --partitioning DIRECTORY` also returns `ok=False`, because n9 is not a member.

## Planned demo

Commands run from `week06/` after installing the required libraries. Steps 2 and 3 use `--request-routing FORWARD` so a Write sent to any node reaches the owner. If request_routing is still a stub, the `not owner` reply still prints `owner=...`, which is the partitioning answer.

1. Start one node in each of three terminals:

```bash
python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
```

2. Write keys `k01` through `k24` through n0 with HASH_MOD, then check the owners:

```bash
python3 -m platform.client --addr 127.0.0.1:9600 write k01 v01 --partitioning HASH_MOD --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 status
```

Expected: the `owner=` values cover n0, n1, and n2, and `keys_per_node` is spread out.

3. Start n3 in a fourth terminal and add it to the cluster:

```bash
python3 -m platform.node --node-id n3 --port 9603 --peers n0=127.0.0.1:9600,n1=127.0.0.1:9601,n2=127.0.0.1:9602,n3=127.0.0.1:9603
python3 -m platform.client --addr 127.0.0.1:9600 add-node n3 127.0.0.1 9603 --partitioning HASH_MOD
```

Expected: about 18 of the 24 keys change owner. Repeating steps 1–3 on fresh nodes with `--partitioning SLOTS` moves only about 6.

4. Write two neighboring keys with KEY_RANGE:

```bash
python3 -m platform.client --addr 127.0.0.1:9600 write aa1 x --partitioning KEY_RANGE --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 write ab2 y --partitioning KEY_RANGE --request-routing FORWARD
```

Expected: both Writes report the same `owner=`.

5. Pin a key with DIRECTORY, add a node, and rebalance:

```bash
python3 -m platform.client --addr 127.0.0.1:9600 pin k n1 --partitioning DIRECTORY
python3 -m platform.client --addr 127.0.0.1:9600 write k pinned --partitioning DIRECTORY --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 add-node n3 127.0.0.1 9603 --partitioning DIRECTORY
python3 -m platform.client --addr 127.0.0.1:9600 rebalance --partitioning DIRECTORY
python3 -m platform.client --addr 127.0.0.1:9601 read k --partitioning DIRECTORY
```

Expected: the pin returns `ok=True`, `k` stays on n1 after the add, and the Read on n1 returns `pinned` with `served_by=n1`.

6. Try to pin with ITEM:

```bash
python3 -m platform.client --addr 127.0.0.1:9600 pin k n1 --partitioning ITEM
```

Expected: `ok=False`.
