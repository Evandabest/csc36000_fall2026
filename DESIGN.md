# Design: key/value platform

Group: Yasmin Guerra Flores, Evan Haque, Karl Savinskiy, and Atif Tausif.

Instructor: Kurt Brown.

This is a shared draft. Karl's request-routing section is complete. Other members must add their roster details, plugin designs, tradeoffs, and demos before the group submits this document.

## Roster

| Student name | CUNY alias | Plugin filename | Feature branch |
| --- | --- | --- | --- |
| Karl Savinskiy | ksavins000 | request_routing.py | ksavins000-request_routing |
| Yasmin Guerra Flores (Min) | yguerra001 | write_completion.py | Pending |
| Evan Haque | Pending | Pending | Pending |
| Atif Tausif | atausif000 | Pending | Pending |

Evan and Atif still need to confirm their plugin choices and feature branches. Evan's CUNY alias is also pending. Yasmin's write-completion claim is based on the group discussion. Each teammate must add their own design section.

## Platform reminder

Keys and values are bytes. Routing chooses where a request goes; it does not choose key ownership or create replicas.

## Karl: request_routing.py

**Reading:** Designing Data-Intensive Applications, chapter 7, request routing.

**Modes:** FORWARD, DIRECTORY_TIER, CLIENT_AWARE.

**History:** Send a Write to n1 while n0 owns the key. FORWARD and DIRECTORY_TIER send it to n0, where a Read returns the written bytes. CLIENT_AWARE rejects it with `not owner`, and the key remains absent. Then isolate n1: a forwarded Write fails with `owner unreachable`. Restore connectivity and retry; the Write succeeds.

**Commands:** See the demo below.

**Expected failure:** A CLIENT_AWARE Write to a non-owner returns `ok=False`, `error='not owner'`. FORWARD also fails if reaching the owner would require more than eight hops.

## Tradeoffs

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| FORWARD | Request arrives at n1, but n0 owns the key. | n1 forwards to n0; a routing loop ends with `hop limit`. | Reading through n1 reaches n0 and returns the stored bytes. | Extra network hops; the client waits for forwarding. |
| DIRECTORY_TIER | A directory entry points to the wrong node. | The destination rejects it; n1 refreshes the destination and retries once. If the retry reaches the owner, the Write succeeds. | With a refreshed lookup, a Read through n1 returns the owner's bytes; otherwise it fails. | A lookup and possible retry add work; one retry cannot fix every stale map or outage. |
| CLIENT_AWARE | Request arrives at n1, but n0 owns the key. | Returns `not owner`; no forwarding or storage occurs. | A Read on n1 returns `not owner`; a direct Read on n0 returns its existing value or a miss. | The client must find and contact the correct owner. |

## Protocol notes

Both methods return `None` when this node owns the key so the starter handles the operation locally. Otherwise, they return a response or forward using `node.rpc`. Each forwarded copy increments `hop`; no request is forwarded beyond eight hops. An omitted routing mode defaults to FORWARD.

DIRECTORY_TIER checks `node.directory`, falling back to the supplied owner. Its forwarded copy uses CLIENT_AWARE so the destination reports a stale lookup instead of forwarding again. A `not owner` or connection failure causes one retry. Writes can use the owner returned in the response; Reads look up the owner again because ReadResponse has no owner field.

## Demo

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

The CLI has no command to inject a stale routing-directory entry or set a hop count. Separate checks exercised stale-entry retries and forwarding loops using the starter nodes without changing teammates' files.

## What we are not building this week

No web UI, database, or production routing service. Karl's plugin only handles request routing; other policies belong to their claimed plugins.

## Git

Karl's design uses `ksavins000-request_routing`. The plugin implementation will be submitted for the code deadline. The slash form is blocked by the existing local `ksavins000` branch. The team lead reviews and merges a pull request into `main` after the group completes its sections. The README requires DESIGN.md on main by October 20, 2026, and working plugin code by October 27, 2026, both at 11:59pm EST.
