# Leader Replication Design

**Student:** Evan Haque

**CUNY alias:** ehaque002 

**Plugin:** `week06/platform/plugins/leader_replication.py`

## Plan

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

### What the plugin receives

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

## Modes and tradeoffs

| Mode | Situation | Planned Write behavior | Expected Read behavior | Tradeoff |
| --- | --- | --- | --- | --- |
| SINGLE_LEADER | n0 is leader. Write through n1, then isolate n1 from n0 and try another Write. | The first Write forwards to n0. The isolated follower returns `ok=False`, `error="not leader"`, and does not store the second Write locally. | Read on n0 returns the first value. A follower may have stale or missing data unless another plugin copies or repairs it. | Restricting writers avoids independent follower writes, but an unreachable leader prevents those writes from succeeding. |
| LEADERLESS | Split n0 from n1/n2; write alpha=L on n0 and alpha=R on n1. | Both nodes accept locally with the starter's local completion behavior. | Read on n0 returns L and Read on n1 returns R. | Writes remain available during the split, but the values can conflict and require reconciliation. |
| MULTI_LEADER | Same split and writes. All members are eligible leaders. | Both accept locally. n1 also sends R to reachable n2; failed copies to the other side do not undo local success. | Read on n0 returns L; Reads on n1 and n2 return R with the starter's local read behavior. | Copies add network traffic and latency, and concurrent writes can still conflict. |

These histories assume the other plugins remain starter stubs. With a completed
write_completion plugin, ACK_ALL or ACK_QUORUM can reject a Write even when this
plugin permits local acceptance. Admission and write completion are separate decisions.

## Write authority rules

- **SINGLE_LEADER.** Read `node.leader` and check that it is a current member. If this node is that leader, return `None` so the node continues the Write. Otherwise forward to that leader and return its response.
- **Forwarding limit.** A follower with `hop >= 8` returns `hop limit` instead of forwarding. A request can reach its destination at hop 8, but cannot be forwarded a ninth time. This also bounds loops caused by inconsistent leader views.
- **Unavailable leader.** If forwarding raises `RpcFailed`, return `not leader`. A failed call does not cause this node to promote itself. A timeout can be ambiguous: the remote leader might already have applied the Write.
- **LEADERLESS.** Return `None` on every member. This plugin adds no leader-specific broadcast; ordinary replication and write completion belong to their own plugins.
- **MULTI_LEADER.** Return `None` on every member, then copy successful local Writes to the other members. The starter has no separate multileader roster, so all current members are eligible leaders.
- **Unknown mode.** Return `ok=False`, `error="unknown leader replication mode"` without storing the Write.

## Copying writes and handling a split

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

## A combination that should fail

- With n0 as leader, isolate n1 and issue a SINGLE_LEADER Write through n1. It returns `ok=False`, `error="not leader"`; n1 does not apply the Write locally.
- A follower needing to forward at hop 8 returns `ok=False`, `error="hop limit"`.
- With a completed write_completion plugin, MULTI_LEADER plus ACK_ALL can fail during a split because not every member can acknowledge. Permission to accept locally does not satisfy ACK_ALL.

## Implementation order

| Order | Part | Done when |
| --- | --- | --- |
| 1 | SINGLE_LEADER admission and forwarding | A follower's Write reaches the current leader without a local follower Write. |
| 2 | Forwarding failures and hop limit | An isolated follower rejects the Write, and inconsistent leader views terminate with `hop limit`. |
| 3 | LEADERLESS admission | Nodes on both sides of a split accept independent Writes. |
| 4 | MULTI_LEADER admission and copies | Both sides accept; reachable members receive the resulting bytes without recursive broadcast. |
| 5 | Invalid modes and membership handling | Unknown modes are rejected and copying follows the current member list. |

## Test checklist

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

## Planned demo


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

