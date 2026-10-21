# Design: key/value platform

Group:

The roster is how the group records who owns which plugin.

## Roster

Each person lists a different plugin filename.

| Student name | CUNY alias | Plugin filename | Feature branch |
| --- | --- | --- | --- |
| Karl Savinskiy | ksavins000 | request_routing.py | ksavins000-request_routing |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## Platform reminder

Values are opaque bytes.

## Per owned plugin

Repeat this block for each person.

**Plugin file:** request_routing.py

**Modes we will implement:** FORWARD, DIRECTORY_TIER, CLIENT_AWARE.

**One history we will demo:** A Write arrives at n1 while n0 owns the key. FORWARD and DIRECTORY_TIER send it to n0, and a Read on n0 returns the written bytes. CLIENT_AWARE rejects the Write with `not owner`, leaving the key absent. If n1 is isolated, forwarding fails; after connectivity is restored, retrying succeeds.

**Commands:** See the request-routing demo below.

**A mode combination that should fail, and the error Write or Read returns:** CLIENT_AWARE on a non-owner returns `not owner`. FORWARD while n1 cannot reach n0 returns `owner unreachable`.

## Tradeoffs

For each mode you implement, name a fault or change — pause a node, split the network, add a node, two Writes at once — and fill the columns.

Keep the tables for the plugins your group owns; you can drop the others.

### partitioning.py

*Think Distributed Systems* ch. 7; *Designing Data-Intensive Applications* ch. 7. Where a key lives, and which keys move when you add a node. HASH_MOD remaps most keys; SLOTS moves about `1/(N+1)`; KEY_RANGE keeps neighbors together; DIRECTORY pin holds; ITEM has no pin. `N` is `len(node.nodes)`.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| HASH_MOD |  |  |  |  |
| KEY_RANGE |  |  |  |  |
| SLOTS |  |  |  |  |
| DIRECTORY |  |  |  |  |
| ITEM |  |  |  |  |

### replication.py

*Think Distributed Systems* §8.4.4. What a copy on the wire looks like, and how a node that was down catches up.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| STATE |  |  |  |  |
| LOG |  |  |  |  |
| LOG_WITH_SNAPSHOT |  |  |  |  |

### write_completion.py

*Think Distributed Systems* §8.4.3; *Database Internals* ch. 11. When Write returns vs whether every member has the bytes. W is 1, majority of current membership (`N//2 + 1`), or all `N`.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| ACK_ALL |  |  |  |  |
| ACK_ONE |  |  |  |  |
| ACK_QUORUM |  |  |  |  |

### leader_replication.py

*Think Distributed Systems* §8.4.5. Who may accept a Write if the network splits.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| SINGLE_LEADER |  |  |  |  |
| LEADERLESS |  |  |  |  |
| MULTI_LEADER |  |  |  |  |

### conflicts.py

*Think Distributed Systems* §9.3.3. After a split, two different values for one key.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| OP_CRDT |  |  |  |  |
| STATE_CRDT |  |  |  |  |
| APP_RECONCILE |  |  |  |  |

### processing_guarantees.py

*Think Distributed Systems* ch. 4. A step whose Write is in doubt (timeout or a killed process).

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| AT_MOST_ONCE |  |  |  |  |
| AT_LEAST_ONCE |  |  |  |  |
| EXACTLY_ONCE |  |  |  |  |
| resume LOG |  |  |  |  |
| resume SNAPSHOT |  |  |  |  |

### request_routing.py

*Designing Data-Intensive Applications* ch. 7 (request routing).

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| FORWARD | Request arrives at n1, but n0 owns the key. | n1 forwards to n0; a routing loop ends with `hop limit`. | Reading through n1 reaches n0 and returns the stored bytes. | Extra network hops; the client waits for forwarding. |
| DIRECTORY_TIER | A directory entry points to the wrong node. | The destination rejects it; n1 refreshes the destination and retries once. If the retry reaches the owner, the Write succeeds. | With a refreshed lookup, a Read through n1 returns the owner's bytes; otherwise it fails. | A lookup and possible retry add work; one retry cannot fix every stale map or outage. |
| CLIENT_AWARE | Request arrives at n1, but n0 owns the key. | Returns `not owner`; no forwarding or storage occurs. | A Read on n1 returns `not owner`; a direct Read on n0 returns its existing value or a miss. | The client must find and contact the correct owner. |

### leader_election.py

Leader election (Bully, ring, majority/term). The leader is killed, or the members are split. Majority for RAFT_STYLE is `N//2 + 1` of current membership.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| BULLY |  |  |  |  |
| RING |  |  |  |  |
| RAFT_STYLE |  |  |  |  |

### session_models.py

*Database Internals* ch. 11 (session models). Two Reads of the same key after a Write, possibly on different nodes.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| READ_OWN_WRITES |  |  |  |  |
| MONOTONIC_READS |  |  |  |  |
| NO_SESSION |  |  |  |  |

### atomic_commit.py

*Think Distributed Systems* ch. 6 (2PC); saga compensation. Two keys, and a crash in the middle.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| TWO_PC |  |  |  |  |
| SAGA |  |  |  |  |
| SINGLE_KEY_ONLY |  |  |  |  |

## Protocol notes

If you own one of these files, say a little more:

- **replication.py:** for two Writes, what goes on the wire for state vs log vs snapshot catch-up
- **conflicts.py:** the exact rule that turns two blobs into one, for each policy
- **processing_guarantees.py:** the step list, the idempotency key, how resume from a log differs from resume from a snapshot
- **atomic_commit.py:** the 2PC messages for keys `a` and `b`; how saga compensation restores `a`; what the plugin does if a participant is killed in prepare
- **leader_election.py:** who wins, what a partition does, how Raft-style differs from Bully and Ring
- **partitioning.py:** key → partition → node; which keys move when a node is added under slots vs `mod N`

## Demo

### Karl: request routing

These commands demonstrate Karl's plugin independently, with the other plugins left as starter stubs. The starter assigns every key to n0. Start with fresh processes. Run commands from `week06/`, using Python 3.10+ with the required packages installed.

1. Start each node in a separate terminal:

```bash
python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
```

2. In a fourth terminal, show successful forwarding and directory routing:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write alpha one --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read alpha
python3 -m platform.client --addr 127.0.0.1:9601 write beta two --request-routing DIRECTORY_TIER
python3 -m platform.client --addr 127.0.0.1:9602 read beta --request-routing DIRECTORY_TIER
```

Both Writes return `ok=True`, `owner=n0`. The Reads return `found=True`, `served_by=n0`, with `one` and `two`, respectively.

3. Show CLIENT_AWARE rejection:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write gamma three --request-routing CLIENT_AWARE
python3 -m platform.client --addr 127.0.0.1:9600 read gamma
```

The Write returns `ok=False`, `error='not owner'`. The Read on n0 returns `found=False`.

4. Show a network fault and recovery:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate on
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read delta
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate off
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read delta
```

During isolation, the Write returns `owner unreachable` and the Read reports `found=False`. After recovery, the Write succeeds and the Read returns `four`.

## What we are not building this week

A sentence or two (for example: a full Raft, MVCC, or an application data type).

## Git

Each plugin change is a pull request from the named feature branch. The team lead merges. The group repo is the submission.
