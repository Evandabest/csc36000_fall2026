# Design: key/value platform

**Group:** To be filled in

## Roster

| Student name | CUNY alias | Plugin filename | Feature branch |
| --- | --- | --- | --- |
| Atif Tausif | atausif000 | `partitioning.py` | TBD |
| Karl Savinskiy | ksavins000 | `request_routing.py` | TBD |

## Platform reminder

Values are opaque bytes. Partitioning decides the owner of each key and moves stored bytes when membership changes; request routing decides what happens when a Write or Read arrives at a non-owner node. Neither plugin provides replication.

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

## What we are not building this week

We are implementing only `partitioning.py` and `request_routing.py`: not replication, quorum acknowledgments, leader election, conflict reconciliation, session guarantees, or distributed transactions. Partitioning handles ownership and movement, while routing handles forwarding and owner errors; neither ensures replicated copies or full fault tolerance.

## Git

Each person develops their own plugin on a separate feature branch and opens a pull request. The team lead merges the reviewed pull requests; the group repository is the submission. Record the actual branch names in the roster when created.
