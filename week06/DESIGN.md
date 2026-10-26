# Design: key/value platform

**Group:** To be filled in

## Roster

| Student name | CUNY alias | Plugin filename | Feature branch |
| --- | --- | --- | --- |
| Atif Tausif | atausif000 | `partitioning.py` | TBD |
| Karl Savinskiy | ksavins000 | `request_routing.py` | TBD |
| Evan Haque | ehaque002 | `leader_replication.py` | `Evan-week6` |

## Platform reminder

Values are opaque bytes. Partitioning decides the owner of each key and moves stored bytes when membership changes; request routing decides what happens when a Write or Read arrives at a non-owner node. Leader replication decides which nodes may accept Writes and, in multileader mode, sends copies to other members. Write completion, catch-up, and conflict reconciliation remain separate policies.

## Per owned plugin

### Atif Tausif — partitioning.py

**Plugin file:** `week06/platform/plugins/partitioning.py`  
**Modes we will implement:** `HASH_MOD`, `KEY_RANGE`, `SLOTS`, `DIRECTORY`, `ITEM` (default: `SLOTS`).  
**One history we will demo:** Start n0–n2 and Write 24 distinct key/value pairs using `HASH_MOD` and `FORWARD`. Add n3 and rebalance. The same original value bytes must be readable on each key's new owner; about 18 of 24 keys are expected to change owners under `HASH_MOD`. Repeat on a fresh cluster using `SLOTS`: approximately 6 of 24 keys should change owners, although exact counts vary with the hash distribution.  
**Commands:** From `week06/` (with n0–n2 started as shown in Demo):

```bash
for i in $(seq -w 1 24); do python3 -m platform.client --addr 127.0.0.1:9600 write k$i v$i --partitioning HASH_MOD --request-routing FORWARD; done | tee before.txt
python3 -m platform.node --node-id n3 --port 9603 --peers n0=127.0.0.1:9600,n1=127.0.0.1:9601,n2=127.0.0.1:9602,n3=127.0.0.1:9603
python3 -m platform.client --addr 127.0.0.1:9600 add-node n3 127.0.0.1 9603 --partitioning HASH_MOD
for p in 9600 9601 9602 9603; do python3 -m platform.client --addr 127.0.0.1:$p rebalance --partitioning HASH_MOD; done
python3 -m platform.client --addr 127.0.0.1:9603 status
```

Run the n3 node command in a separate terminal, and repeat the experiment on a fresh cluster with `--partitioning SLOTS`. Use Reads on the newly assigned owners to verify byte-for-byte data movement; `Status` alone reports logical ownership, not proof that bytes moved.  
**A mode combination that should fail, and the error Write or Read returns:** `pin k n1 --partitioning ITEM` returns `ok=False` because `ITEM` disallows pinning. `pin k n9 --partitioning DIRECTORY` also returns `ok=False` because n9 is not a member. These are pin-command errors, not Write/Read errors.

### Karl Savinskiy — request_routing.py

**Plugin file:** `week06/platform/plugins/request_routing.py`  
**Modes we will implement:** `FORWARD`, `DIRECTORY_TIER`, `CLIENT_AWARE`.  
**One history we will demo:** With the other plugins left as starter stubs in a fresh three-node cluster (where n0 owns all keys), send `Write(alpha, one)` to n1 using `FORWARD`. It reaches n0, and a subsequent Read at n0 returns exactly `one`. Isolate n1 and attempt `Write(delta, four)` using `FORWARD`: the Write should fail with `owner unreachable`, and a Read on n0 should report `found=False`. After reconnecting n1 and retrying, n0 should return `four`.  
**Commands:** From `week06/` (with n0–n2 running):

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write alpha one --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read alpha
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate on
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read delta
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate off
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read delta
```

**A mode combination that should fail, and the error Write or Read returns:** A `CLIENT_AWARE` Write to n1 when n0 owns the key returns `not owner`, without forwarding or storing it. The client must contact the reported owner directly. With `FORWARD`, an isolated forwarding node returns `owner unreachable`; a forwarding loop exceeding eight hops returns `hop limit`.

### Evan Haque — leader_replication.py

**Plugin file:** `week06/platform/plugins/leader_replication.py`

**Modes we will implement:** `SINGLE_LEADER`, `LEADERLESS`, `MULTI_LEADER` (default: `SINGLE_LEADER`).

**One history we will demo:** With n0 as leader, send a SINGLE_LEADER Write through n1; n0 stores the value. Isolate n1 and verify that its next SINGLE_LEADER Write returns `not leader` without applying locally. On fresh clusters, split n0 from n1/n2 and write alpha=L on n0 and alpha=R on n1. LEADERLESS permits both local Writes. MULTI_LEADER additionally copies R from n1 to reachable n2.

**Commands:** See Evan's leader replication demo below. Run this plugin independently with the other plugins left as starter stubs for those exact Read results.

**A mode combination that should fail, and the error Write or Read returns:** An isolated follower using SINGLE_LEADER returns `ok=False`, `error="not leader"`. A follower that would forward beyond eight hops returns `hop limit`. MULTI_LEADER with a completed ACK_ALL policy can also fail during a split because local admission does not guarantee all acknowledgments.

## Tradeoffs

### partitioning.py

*Think Distributed Systems* ch. 7; *Designing Data-Intensive Applications* ch. 7. `N = len(node.nodes)`.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| HASH_MOD | Add n3 to a three-node cluster, then rebalance. | Computes owner `nodes[H(key) % N]`; reassigns roughly 3/4 of keys when N changes from 3 to 4. | Following successful rebalance, the new owner returns the same stored bytes. | Simple distribution, but most keys remap on membership changes. |
| KEY_RANGE | Write nearby prefixes `aa1`, `ab2`, plus a key beginning `0xff`. | Chooses `nodes[min(key[0] * N // 256, N-1)]`, placing nearby prefixes together. | `aa1` and `ab2` are served by the same owner; `0xff…` by the last node. | Uneven load for clustered or skewed key prefixes. |
| SLOTS | Add n3 to a three-node cluster, then rebalance. | Assigns key to one of 32 stable slots via `H(key) % 32`, then assigns each slot to the node with highest rendezvous score. | After rebalance, changed keys are readable at n3; keys in unchanged slots remain at their former owner. Approximately 1/4 move. | More complex placement and only 32 slots of balancing granularity. |
| DIRECTORY | Pin `k` to n1, add n3, and rebalance. | Replicates directory entry `k → n1`; unpinned keys follow SLOTS. | Read of `k` on n1 returns original bytes after rebalance; the pin survives the membership change. | Distributed directory state must remain consistent. |
| ITEM | Attempt `pin k n1`. | Rejects pin (`ok=False`); normal Writes use the SLOTS-based owner. | The SLOTS-based owner serves `k`, not necessarily n1. | No manual placement or pinning. |

### request_routing.py

*Designing Data-Intensive Applications* ch. 7 (request routing).

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| FORWARD | Send a request to n1 when n0 owns key; alternatively isolate n1. | n1 forwards to n0 using `node.rpc`; with isolation, returns `owner unreachable`. | When connected, Read via n1 returns n0's bytes; on a failed Write, n0 has no new value. | Extra network hop and dependence on forwarding connectivity. |
| DIRECTORY_TIER | Directory entry points to a stale owner. | Looks up destination, refreshes on `not owner` or connection failure, and retries once. | The refreshed reachable owner returns the value; failure after retry is reported as an error. | Lookup overhead and only a limited retry budget. |
| CLIENT_AWARE | Client Writes to n1 while n0 owns key. | n1 returns `not owner` without storing or forwarding. | Read on n1 returns `not owner`; a direct Read on n0 returns its stored value (or `found=False` if the Write never succeeded). | Clients must track or discover the correct node. |

### leader_replication.py

*Think Distributed Systems* ch. 8, §8.4.5 (Week 4 replication material).

| Mode | Situation | Planned Write behavior | Expected Read behavior | Tradeoff |
| --- | --- | --- | --- | --- |
| SINGLE_LEADER | n0 is leader. Write through n1, then isolate n1 from n0 and try another Write. | The first Write forwards to n0. The isolated follower returns `ok=False`, `error="not leader"`, and does not store the second Write locally. | Read on n0 returns the first value. A follower may have stale or missing data unless another plugin copies or repairs it. | Restricting writers avoids independent follower writes, but an unreachable leader prevents those writes from succeeding. |
| LEADERLESS | Split n0 from n1/n2; write alpha=L on n0 and alpha=R on n1. | Both nodes accept locally with the starter's local completion behavior. | Read on n0 returns L and Read on n1 returns R. | Writes remain available during the split, but the values can conflict and require reconciliation. |
| MULTI_LEADER | Same split and writes. All members are eligible leaders. | Both accept locally. n1 also sends R to reachable n2; failed copies to the other side do not undo local success. | Read on n0 returns L; Reads on n1 and n2 return R with the starter's local read behavior. | Copies add network traffic and latency, and concurrent writes can still conflict. |

These histories assume the other plugins remain starter stubs. With a completed
write_completion plugin, ACK_ALL or ACK_QUORUM can reject a Write even when this
plugin permits local acceptance. Admission and write completion are separate decisions.

## Protocol notes

### partitioning.py: key → partition → node

- **Stable hash:** `H(x) = int.from_bytes(hashlib.sha256(x).digest(), "big")`, rather than Python's per-process-randomized `hash()`.
- **HASH_MOD:** key → `H(key) % N` → `node.nodes[index]`. Adding a member changes the modulus and remaps most keys.
- **SLOTS:** key → `H(key) % node.n_slots` (32 slots) → node maximizing `H(slot || node_id)`. Adding n3 changes only the slots it wins, approximately `1/(N+1)` overall. Do not use `slot % N`, which would reintroduce broad remapping.
- **KEY_RANGE:** first byte → evenly divided byte range → owner; empty key uses the first node.
- **DIRECTORY:** a valid pin overrides SLOTS; distribute pin and membership changes across nodes. **ITEM:** uses SLOTS without consulting pins.
- **Migration:** `owner()` computes logical ownership; `rebalance()` sends stored key/value bytes to the new owner using a Write RPC. Delete the old copy only after `ok=True`; on RPC failure, keep it. Rebalance every relevant node.

### request_routing.py: non-owner requests

- `on_write` and `on_read` return `None` when the receiving node is already the owner, allowing the starter to handle it locally.
- `FORWARD` forwards through `node.rpc`, incrementing a hop count; reject requests requiring more than eight forwarding hops with `hop limit`.
- `DIRECTORY_TIER` refreshes and retries once on `not owner` or connection failure. Return an error if the refreshed destination still cannot complete the request.
- `CLIENT_AWARE` responds with `not owner` instead of forwarding; the client contacts the owner.
- Request routing does not compute ownership, move keys, or replicate values; it relies on partitioning to identify the owner.

### leader_replication.py: write authority and copies

I will implement `admit` and `after_local_write`, and all three modes:
`SINGLE_LEADER`, `LEADERLESS`, and `MULTI_LEADER`.

| Method | Job |
| --- | --- |
| `admit(node, request, owner)` | Decide whether to accept the Write locally, forward it to the leader, or return an error. Return `None` to continue locally, or a `WriteResponse` if the request has already been handled. |
| `after_local_write(node, request, value, owner)` | After a successful local Write, send copies to other members in MULTI_LEADER mode. |

The plugin distinguishes **partition ownership** (where a key belongs) from
**write authority** (which node may accept a Write). A follower can receive a
request without having authority to store it in SINGLE_LEADER mode. It forwards
the request to `node.leader`. In LEADERLESS and MULTI_LEADER modes, a node may
accept locally even when it is not the partition owner.

This follows *Think Distributed Systems* ch. 8, section 8.4.5, assigned in
Brightspace's Week 4 replication material. Single-leader systems restrict write
authority to one leader; multileader and leaderless systems allow independent
writers and therefore need conflict resolution.

#### What the plugin receives

From `node.py`, `base.py`, and the provided protobuf messages:

- Keys and values are opaque `bytes`.
- `node.nodes` contains the current member ids. The implementation uses this list rather than assuming three nodes.
- `node.node_id` is this process's id. `node.leader` is the current single leader; the starter initializes it to the first member.
- The mode comes from `mode(request.modes, "leader_replication")`. An empty string defaults to SINGLE_LEADER.
- `owner` is the partition owner already computed by the node. It does not necessarily identify the leader or the process storing this Write.
- `request.hop` counts forwards. `copy_with_hop` copies the request, preserves its fields, and increases that count.
- `value` passed to `after_local_write` is the resulting value after any conflict merge. `node.last_applied` supplies the local replication index.
- `node.lock` protects shared state while it is read. Network calls happen after releasing the lock.
- `node.rpc(peer_id, "Write" | "Replicate", request)` sends a message and honors the fault flags. A dropped or timed-out call raises `RpcFailed`.

#### Write authority rules

- **SINGLE_LEADER.** Read `node.leader` and check that it is a current member. If this node is that leader, return `None` so the node continues the Write. Otherwise forward to that leader and return its response.
- **Forwarding limit.** A follower with `hop >= 8` returns `hop limit` instead of forwarding. A request can reach its destination at hop 8, but cannot be forwarded a ninth time. This also bounds loops caused by inconsistent leader views.
- **Unavailable leader.** If forwarding raises `RpcFailed`, return `not leader`. A failed call does not cause this node to promote itself. A timeout can be ambiguous: the remote leader might already have applied the Write.
- **LEADERLESS.** Return `None` on every member. This plugin adds no leader-specific broadcast; ordinary replication and write completion belong to their own plugins.
- **MULTI_LEADER.** Return `None` on every member, then copy successful local Writes to the other members. The starter has no separate multileader roster, so all current members are eligible leaders.
- **Unknown mode.** Return `ok=False`, `error="unknown leader replication mode"` without storing the Write.

#### Copying writes and handling a split

`after_local_write` only sends extra copies in MULTI_LEADER mode:

1. Copy the current peer list and local applied index under `node.lock`.
2. Ask the conflicts plugin for metadata if it implements `export_meta`.
3. Build a STATE `ReplicateRequest` with the key, resulting value, local index, source node, request modes, and available metadata.
4. Send that request to every other current member using `node.rpc`.
5. If a call raises `RpcFailed`, continue to the next member. Keep the accepted local value.

Copies use `Replicate`, not `Write`. The receiver applies the copy without
starting another admission/broadcast cycle. This prevents recursive fan-out.
The sender attempts peers sequentially, so unreachable peers can delay the reply
until their RPC timeouts. Their failures do not by themselves turn the local
Write into a failed Write.

This plugin does not automatically reconcile values after healing. Catch-up
belongs to replication, failed-copy repair to write_completion, and conflict
merging to conflicts. Single-leader copies also depend on those plugins.
Leader election is responsible for changing `node.leader`; this plugin only
uses the current value. Read routing belongs to request_routing and may change
which replica a Read observes when that plugin is implemented.

#### Implementation order

| Order | Part | Done when |
| --- | --- | --- |
| 1 | SINGLE_LEADER admission and forwarding | A follower's Write reaches the current leader without a local follower Write. |
| 2 | Forwarding failures and hop limit | An isolated follower rejects the Write, and inconsistent leader views terminate with `hop limit`. |
| 3 | LEADERLESS admission | Nodes on both sides of a split accept independent Writes. |
| 4 | MULTI_LEADER admission and copies | Both sides accept; reachable members receive the resulting bytes without recursive broadcast. |
| 5 | Invalid modes and membership handling | Unknown modes are rejected and copying follows the current member list. |

#### Test checklist

The six automated integration tests passed using five nodes and real gRPC.
Run from `week06/`: `python3 -m unittest discover -s tests -v`.
The supplied generated stubs require protobuf 7.35.1 or a compatible newer
runtime; the successful run used protobuf 7.36.2 and grpcio 1.84.1.

**Single leader**

- [x] A follower forwards a Write to the leader, which stores the bytes.
- [x] The forwarding follower does not apply the Write locally.
- [x] Status reports the current leader.
- [x] An isolated follower rejects a Write without promoting itself.
- [x] A forwarding cycle terminates with `hop limit`.

**Independent writers and copies**

- [x] LEADERLESS accepts different values on both sides of a split.
- [x] MULTI_LEADER accepts different values on both sides of a split.
- [x] Reachable members receive the multileader copy; disconnected sides retain their own values.
- [x] After healing, a new MULTI_LEADER Write reaches all members.

**Validation and integration**

- [x] Unknown modes are rejected without storing a value.
- [ ] Replay the three-node CLI demo below.
- [ ] Check behavior with teammates' completed replication, write_completion, conflicts, and request_routing plugins.

## Demo

Run from `week06/` after installing the requirements. Each node command runs in a separate terminal. For independent experiments, reset or start a fresh cluster; the routing-only demonstration from Karl's plan assumes starter partitioning (n0 owns all keys), whereas the integrated demonstration uses Atif's partitioning plugin.

1. **Start the three initial processes:**

   ```bash
   python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
   python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
   python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
   ```

2. **Check FORWARD with routing-only starter partitioning:**

   ```bash
   python3 -m platform.client --addr 127.0.0.1:9601 write alpha one --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9600 read alpha
   ```

   Expected: Write succeeds; n0 returns `one`.

3. **Check DIRECTORY_TIER and CLIENT_AWARE on a fresh starter-partitioning setup:**

   ```bash
   python3 -m platform.client --addr 127.0.0.1:9601 write beta two --request-routing DIRECTORY_TIER
   python3 -m platform.client --addr 127.0.0.1:9602 read beta --request-routing DIRECTORY_TIER
   python3 -m platform.client --addr 127.0.0.1:9601 write gamma three --request-routing CLIENT_AWARE
   python3 -m platform.client --addr 127.0.0.1:9600 read gamma
   ```

   Expected: `beta` returns `two`, served by n0; `gamma` Write returns `not owner`, and n0 returns `found=False`. A stale directory lookup and its single retry will be tested separately because the provided CLI cannot inject a stale entry.

4. **Test FORWARD failure and recovery:**

   ```bash
   python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate on
   python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9600 read delta
   python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate off
   python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9600 read delta
   ```

   Expected: first Write fails `owner unreachable` and n0 has no `delta`; after recovery, n0 returns `four`.

5. **On a fresh cluster with both plugins, test HASH_MOD and SLOTS:** Write 24 keys with `--request-routing FORWARD` and `--partitioning HASH_MOD`, observe owners, start n3, issue `add-node`, and run `rebalance` across nodes as in the commands above. Check Reads on new owners. Repeat separately with `SLOTS`. Expected: roughly 18 of 24 HASH_MOD owners change, versus roughly 6 of 24 SLOTS owners; counts are illustrative, not exact.

6. **Test nearby prefixes under KEY_RANGE:**

   ```bash
   python3 -m platform.client --addr 127.0.0.1:9600 write aa1 x --partitioning KEY_RANGE --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9600 write ab2 y --partitioning KEY_RANGE --request-routing FORWARD
   ```

   Expected: both report the same `owner=`.

7. **Test DIRECTORY pinning and ITEM rejection on a fresh cluster:**

   ```bash
   python3 -m platform.client --addr 127.0.0.1:9600 write k pinned --partitioning DIRECTORY --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9600 pin k n1 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9600 add-node n3 127.0.0.1 9603 --partitioning DIRECTORY
   for p in 9600 9601 9602 9603; do python3 -m platform.client --addr 127.0.0.1:$p rebalance --partitioning DIRECTORY; done
   python3 -m platform.client --addr 127.0.0.1:9601 read k --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9600 pin k n1 --partitioning ITEM
   ```

   Expected: n1 returns `found=True`, `served_by=n1`, `value=pinned` after rebalance, and the ITEM pin returns `ok=False`. Start n3 before adding it if not already running.

### Evan Haque — leader replication demo

Run from `week06/`. Start three terminals:

```bash
python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
```

Single leader: n1 forwards; n0 stores one. Isolation makes a later follower Write fail.

```bash
python3 -m platform.client --addr 127.0.0.1:9601 status
python3 -m platform.client --addr 127.0.0.1:9601 write alpha one --leader-replication SINGLE_LEADER --write-completion ACK_ONE
python3 -m platform.client --addr 127.0.0.1:9600 read alpha
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate on
python3 -m platform.client --addr 127.0.0.1:9601 write alpha rejected --leader-replication SINGLE_LEADER
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate off
```

Restart the processes for each following mode. Split n0 from n1/n2 in both
directions; run once with MULTI_LEADER, then again with LEADERLESS instead.

```bash
python3 -m platform.client --addr 127.0.0.1:9600 fault --isolate on
python3 -m platform.client --addr 127.0.0.1:9601 fault --drop-to n0
python3 -m platform.client --addr 127.0.0.1:9602 fault --drop-to n0
python3 -m platform.client --addr 127.0.0.1:9600 write alpha L --leader-replication MULTI_LEADER --write-completion ACK_ONE
python3 -m platform.client --addr 127.0.0.1:9601 write alpha R --leader-replication MULTI_LEADER --write-completion ACK_ONE
python3 -m platform.client --addr 127.0.0.1:9600 read alpha
python3 -m platform.client --addr 127.0.0.1:9601 read alpha
python3 -m platform.client --addr 127.0.0.1:9602 read alpha
```

With MULTI_LEADER, reads return L, R, R. With LEADERLESS and starter plugins,
n0/n1 return L/R; n2 has no copy and reports `not owner`. Healing alone does not
reconcile historical conflicts in this plugin. For a simple post-heal copy demo,
clear faults on all nodes (`fault --isolate off`) and issue a new MULTI_LEADER
Write: reachable nodes receive that new value.

## What we are not building this week

The owned plugins are `partitioning.py`, `request_routing.py`, and `leader_replication.py`. They cover ownership and movement, request routing, write authority, and best-effort multileader copies. They do not provide a complete replication/catch-up protocol, quorum acknowledgments, automatic leader election, conflict reconciliation, session guarantees, or distributed transactions. The independent demonstrations assume the other plugins remain starter stubs; combined behavior must be checked when the implementations are integrated.

## Git

Each person develops their own plugin on a separate feature branch and opens a pull request. The team lead merges the reviewed pull requests; the group repository is the submission. Record the actual branch names in the roster when created.
