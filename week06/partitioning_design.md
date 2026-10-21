# Partitioning Design

**Student:** Atif Tausif

**CUNY alias:** atausif000

**Plugin:** `week06/platform/plugins/partitioning.py`

## Plan

I will implement `owner`, `on_add_node`, and `rebalance`, and all five modes: `HASH_MOD`, `KEY_RANGE`, `SLOTS`, `DIRECTORY`, and `ITEM`.

| Method | Job |
| --- | --- |
| `owner(node, key, mode)` | Return the node id that should store `key`. The node calls it on every Write, Read, and Status. |
| `on_add_node(node, request)` | Update membership after `add-node`, or record a `pin`. Return a `StatusResponse`. |
| `rebalance(node, modes)` | Send every key this node holds to its current owner, if that owner is another node. |

The plugin keeps two things apart: **logical ownership** (what `owner()` returns) and **physical storage** (which node's `node.store` holds the bytes). Suppose `add-node` changes `banana`'s owner from n0 to n3. `owner(b"banana")` returns `n3` right away, but the bytes stay on n0 until `rebalance` moves them. This follows *Think Distributed Systems* ch. 7, which treats choosing an item's partition and spreading partitions across nodes as separate steps.

### What the plugin receives

From `node.py` and `base.py`:

- `key` is `bytes`. Values are opaque bytes.
- `node.nodes` is a list of node id strings, such as `["n0", "n1", "n2"]`, sorted by `(len, id)` on every node. `N` is `len(node.nodes)`.
- The mode comes from the request: `mode(request.modes, "partitioning")` from `platform.util`. An empty string means the default, which I will set to SLOTS.
- Data lives in `node.store` (`dict[bytes, bytes]`). Updates to it happen under `node.lock`.
- Pins live in `node.directory` (`dict[bytes, str]`). `node.n_slots` is 32, from `cluster.json`.
- To talk to other nodes, I will call `node.rpc(peer_id, "Write" | "AddNode", request)`. If the call fails, it raises `RpcFailed`.

## Modes and tradeoffs

| Mode | Situation | Planned Write behavior | Expected Read behavior | Tradeoff |
| --- | --- | --- | --- | --- |
| HASH_MOD | 24 keys written on a 3-node cluster, then `add-node n3` and `rebalance`. | The owner is `nodes[H(key) % N]`. The keys spread across n0, n1, and n2. After the add, N is 4. | After `rebalance`, a Read on each key's new owner returns the original bytes. About 3/4 of the keys moved. | The simplest even spread, but changing N remaps most keys. |
| SLOTS | Same 24 keys, then `add-node n3` and `rebalance`. | The key goes to slot `H(key) % 32`. Each slot belongs to the node with the highest `H(slot, node_id)` (rendezvous hashing). | Only the slots n3 now wins move, about 1/(N+1) = 1/4 of the keys. Reads on the other keys still hit the same node. | Fewer moves, but the balance is only as fine as 32 slots, and the rule is more complex. |
| KEY_RANGE | Keys `aa1` and `ab2`, and a key starting with byte `0xff`. | The owner is `nodes[min(key[0] * N // 256, N - 1)]`. `aa…` and `ab…` land on the same node, and `0xff…` goes to the last node. | Reads of `aa1` and `ab2` are served by the same node. | Neighboring keys stay together, but skewed keys (many starting with `a`) overload one node. |
| DIRECTORY | `pin k n1`, then `add-node n3`, then `rebalance`. | `pin` records `k → n1` in `node.directory` on every node. Keys that are not pinned use SLOTS. | `k` stays owned by n1 after the add. After `rebalance`, a Read of `k` on n1 returns the bytes. | Exact control per key, but every node has to store and agree on directory state. |
| ITEM | `pin k n1`. | `pin` returns `ok=False`. The owner comes only from the key (the SLOTS rule). | A Read of `k` is served by the hash owner, not n1. | No directory to keep in sync, but you cannot place a hot key by hand. |

## Placement rules

- **Stable hash.** `H(x) = int.from_bytes(hashlib.sha256(x).digest(), "big")`. I will not use Python's `hash()`, because it is randomized per process and the nodes would disagree about owners.
- **HASH_MOD.** `nodes[H(key) % N]`.
- **SLOTS.** `slot = H(key) % S`, where S = `node.n_slots` = 32 and does not depend on N. The owner of a slot is the node with the highest `H(slot || node_id)`. Every node can compute this from `node.nodes` alone, so there is no slot table to keep in sync. When a node joins, only the slots it now wins change owner, about 1/(N+1). Using `slot % N` instead would remap most keys again, just like HASH_MOD.
- **KEY_RANGE.** The first byte (0–255) is split into N equal intervals: `min(key[0] * N // 256, N - 1)`. An empty key goes to `nodes[0]`.
- **DIRECTORY.** If `key` is in `node.directory` and that node is still a member, return it. Otherwise use the SLOTS rule.
- **ITEM.** Use the SLOTS rule and never read `node.directory`.

`owner()` will only choose a mode and call one of these small helpers, so each rule can be tested on its own.

## Adding nodes, pins, and moving bytes

`on_add_node` handles two kinds of request:

- **Pin** (`pin_key` is set). Only DIRECTORY accepts a pin, and only for a node in `node.nodes`. Anything else returns `ok=False`. The node saves the pin, then sends the same request to the other nodes with `node.rpc(peer, "AddNode", request)`. A node that already has that exact pin doesn't send it again, so the broadcast stops. This keeps `node.directory` the same on every node. A local dict that only one node knows about would not be enough.
- **Add node.** The node adds the new id to `node.nodes`, keeping the same sort as `node.py`, and the new address to `node.peers`. It then sends the request to the other nodes. A node that already knows the id doesn't send it again. Adding a node never removes or overrides a pin. This step changes only who owns each key. Bytes do not move until `rebalance`.

`rebalance` moves the bytes:

1. Copy the list of keys in `node.store`.
2. For each key, compute `owner(key)` with the current membership.
3. If this node is the owner, leave the key alone.
4. Otherwise, send it with `node.rpc(owner, "Write", WriteRequest(key, value, modes))`, using the same partitioning mode, so the receiving node agrees it owns the key.
5. Delete the local copy only after the reply is `ok=True`. If the call raises `RpcFailed` or returns `ok=False`, keep the key. A failed move never deletes the only copy.

The `Rebalance` RPC runs only on the node that receives it, so the demo runs `rebalance` on every node. `Status` alone does not prove a move worked, because it reports logical owners. The check is a Read on the new owner.

This plugin only decides ownership and moves bytes. Forwarding requests belongs to request_routing, and making copies belongs to replication and write_completion.

## A combination that should fail

- `pin k n1 --partitioning ITEM` returns `ok=False`.
- `pin k n9 --partitioning DIRECTORY` also returns `ok=False`, because n9 is not a member.

## Implementation order

| Order | Part | Done when |
| --- | --- | --- |
| 1 | `HASH_MOD` + `owner()` dispatch | Owners are the same on every node and spread over all nodes. |
| 2 | `SLOTS` | Adding a node moves about 1/(N+1) of the keys. |
| 3 | `KEY_RANGE` | `aa…` and `ab…` share an owner, and `0xff…` goes to the last node. |
| 4 | `on_add_node` (membership) + `rebalance` | Moved keys can be read on the new owner, byte-for-byte the same. |
| 5 | `ITEM`, `DIRECTORY`, pins | The pin holds through `add-node`, the bytes reach the pinned node, and an ITEM pin returns `ok=False`. |

HASH_MOD and SLOTS come first because the assignment requires them.

## Test checklist

**Ownership**
- [ ] HASH_MOD spreads keys over several nodes.
- [ ] HASH_MOD remaps most keys after `add-node`.
- [ ] SLOTS remaps only about 1/(N+1) of the keys. 24 keys is a small sample, so I will also check a few hundred keys in a local script.
- [ ] KEY_RANGE keeps neighboring prefixes together.
- [ ] Every node computes the same owner for the same key.

**Membership and moving data**
- [ ] `add-node` updates membership on every node.
- [ ] `rebalance` actually moves the bytes, checked with a Read on the new owner.
- [ ] Moved values are byte-for-byte the same.
- [ ] A failed move (target paused or isolated) keeps the local copy.

**Pinning**
- [ ] A DIRECTORY pin to an existing node returns `ok=True`.
- [ ] The pinned owner doesn't change after `add-node`.
- [ ] `rebalance` moves the pinned key's bytes to the pinned node.
- [ ] An ITEM pin returns `ok=False`.

## Planned demo

Run all commands from `week06/` after installing the requirements. Writes use `--request-routing FORWARD` so a Write sent to any node reaches its owner. If request_routing is still a stub, the reply is `not owner`, but it still prints `owner=...`, which is the partitioning result.

1. Start one node in each of three terminals:

```bash
python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
```

2. Write 24 keys through n0 with HASH_MOD and save the owners:

```bash
for i in $(seq -w 1 24); do python3 -m platform.client --addr 127.0.0.1:9600 write k$i v$i --partitioning HASH_MOD --request-routing FORWARD; done | tee before.txt
```

Expected: the `owner=` values include n0, n1, and n2.

3. Start n3 in a fourth terminal, add it, and rebalance every node:

```bash
python3 -m platform.node --node-id n3 --port 9603 --peers n0=127.0.0.1:9600,n1=127.0.0.1:9601,n2=127.0.0.1:9602,n3=127.0.0.1:9603
python3 -m platform.client --addr 127.0.0.1:9600 add-node n3 127.0.0.1 9603 --partitioning HASH_MOD
for p in 9600 9601 9602; do python3 -m platform.client --addr 127.0.0.1:$p rebalance --partitioning HASH_MOD; done
python3 -m platform.client --addr 127.0.0.1:9603 status
```

Expected: about 18 of the 24 keys change owner, and n3's `key_count` is above 0. A Read on n3 for a key it now owns returns the original value. Repeating steps 1–3 on fresh nodes with `--partitioning SLOTS` moves only about 6 keys.

4. Write two neighboring keys with KEY_RANGE:

```bash
python3 -m platform.client --addr 127.0.0.1:9600 write aa1 x --partitioning KEY_RANGE --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 write ab2 y --partitioning KEY_RANGE --request-routing FORWARD
```

Expected: both report the same `owner=`.

5. On a fresh cluster, write a key, pin it with DIRECTORY, add a node, and rebalance:

```bash
python3 -m platform.client --addr 127.0.0.1:9600 write k pinned --partitioning DIRECTORY --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 pin k n1 --partitioning DIRECTORY
python3 -m platform.client --addr 127.0.0.1:9600 add-node n3 127.0.0.1 9603 --partitioning DIRECTORY
for p in 9600 9601 9602 9603; do python3 -m platform.client --addr 127.0.0.1:$p rebalance --partitioning DIRECTORY; done
python3 -m platform.client --addr 127.0.0.1:9601 read k --partitioning DIRECTORY
```

Expected: the pin returns `ok=True`, and `k` stays owned by n1 after the add. The Read on n1 returns `found=True`, `served_by=n1`, and `value=pinned`. The bytes really moved; they did not just change owner in Status.

6. Try to pin with ITEM:

```bash
python3 -m platform.client --addr 127.0.0.1:9600 pin k n1 --partitioning ITEM
```

Expected: `ok=False`.
