# Write Completion — Yasmin Guerra

## Roster entry

| Student name | CUNY alias | Plugin filename | Feature branch |
| --- | --- | --- | --- |
| Yasmin Guerra | yguerra001 | write_completion.py | yguerra001-write_completion |

## My plugin

**Plugin file:** `week06/platform/plugins/write_completion.py`

**Modes:** ACK_ONE, ACK_QUORUM, ACK_ALL

The write completion plugin determines how many acknowledgments are required before a Write is considered successful. It uses the current cluster membership, where `N = len(node.nodes)`.

- **ACK_ONE:** Requires 1 acknowledgment.
- **ACK_QUORUM:** Requires `N//2 + 1` acknowledgments.
- **ACK_ALL:** Requires `N` acknowledgments.

The plugin records copies that still need delivery and retries them during repair. ACK_QUORUM controls Write completion only; the provided client does not perform quorum Reads.

## Modes and tradeoffs

Assume a three-node cluster (n0, n1, n2), with n2 paused.

| Mode | Fault or change | What Write does | What Read returns on which node | Tradeoff |
| --- | --- | --- | --- | --- |
| ACK_ONE | Pause n2. | Returns `ok=True` after one acknowledgment, even though n2 has not received the bytes. | Before repair, n2 may have stale or missing bytes. After repair, reading `alpha_one` on n2 returns `one`. | Acknowledged data may exist on only one node, risking data loss if that node fails. |
| ACK_QUORUM | Pause n2; separately, pause n1 and n2. | With N=3, succeeds after two acknowledgments. Returns `ack timeout` if fewer than two acknowledge. | An acknowledged replica has the new bytes. After repair, reading `alpha_quorum` on n2 returns `two`. | Requires a majority, increasing latency and reducing availability compared with ACK_ONE. |
| ACK_ALL | Pause n2. | Cannot collect all three acknowledgments, returning `ok=False error='ack timeout'`. n0 or n1 may still have applied the Write. | After n2 resumes and repair runs, reading `alpha_all` on n2 returns `three`. | One unavailable member prevents a successful Write. |

## Implementation

The plugin determines Write completion based on the selected acknowledgment mode.

1. Read the selected mode: ACK_ONE, ACK_QUORUM, or ACK_ALL.
2. Determine the required acknowledgments using current membership.
3. Track which nodes acknowledge the Write using `acked_by`.
4. Return success once the required acknowledgment count is reached.
5. Return `ok=False error='ack timeout'` if the required acknowledgments cannot be collected.
6. Record missing copies so they can be retried during `rebalance`.

A timed-out Write may have already reached some nodes, so failure does not imply rollback. Once an unavailable node recovers, repair can deliver its missing copies.

## Demo

Run from `week06/` with all three nodes started using `cluster.json`.

Start these in three separate terminals:

```bash
python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
```

Pause n2 and perform a Write using each mode:

```bash
python3 -m platform.client --addr 127.0.0.1:9602 fault --pause on

python3 -m platform.client --addr 127.0.0.1:9600 write alpha_one one --write-completion ACK_ONE

python3 -m platform.client --addr 127.0.0.1:9600 write alpha_quorum two --write-completion ACK_QUORUM

python3 -m platform.client --addr 127.0.0.1:9600 write alpha_all three --write-completion ACK_ALL
```

Expected: ACK_ONE succeeds after one acknowledgment, ACK_QUORUM succeeds after two, and ACK_ALL returns `ok=False error='ack timeout'` because n2 cannot acknowledge.

Resume n2, repair missing copies, and verify the stored values:

```bash
python3 -m platform.client --addr 127.0.0.1:9602 fault --pause off

python3 -m platform.client --addr 127.0.0.1:9600 rebalance --write-completion ACK_ONE

python3 -m platform.client --addr 127.0.0.1:9602 read alpha_one
python3 -m platform.client --addr 127.0.0.1:9602 read alpha_quorum
python3 -m platform.client --addr 127.0.0.1:9602 read alpha_all
```

Expected: After repair, the Reads on n2 return `one`, `two`, and `three`, respectively.

### Expected failure

Pause n1 and n2, then attempt a Write using ACK_QUORUM:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 fault --pause on
python3 -m platform.client --addr 127.0.0.1:9602 fault --pause on

python3 -m platform.client --addr 127.0.0.1:9600 write alpha_fail four --write-completion ACK_QUORUM
```

Expected: Only n0 can acknowledge. Since ACK_QUORUM requires two acknowledgments in a three-node cluster, the Write returns `ok=False error='ack timeout'`.