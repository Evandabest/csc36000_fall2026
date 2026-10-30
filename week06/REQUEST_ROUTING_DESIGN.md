# Request Routing — Karl Savinskiy

This is my section for the group's root `DESIGN.md`. Add the roster row below
to the common roster and place this section with the other plugin sections.

| Student name | CUNY alias | Plugin filename | Feature branch |
| --- | --- | --- | --- |
| Karl Savinskiy | ksavins000 | request_routing.py | ksavins000-request_routing |

## My plugin

**Plugin file:** `week06/platform/plugins/request_routing.py`

**Modes:** FORWARD, DIRECTORY_TIER, CLIENT_AWARE

**Reading:** *Designing Data-Intensive Applications*, chapter 7, request routing.

The partitioning plugin decides which node owns a key. My plugin decides what
to do when a Write or Read reaches another node. It does not choose ownership
or make replica copies.

## Modes and tradeoffs

Assume n0 owns the key and the client contacts n1.

| Mode | Situation | What Write does | What Read returns | Tradeoff |
| --- | --- | --- | --- | --- |
| FORWARD | The client contacts the wrong node. | n1 sends the Write to n0. | A Read through n1 gets the value from n0. | Any node can receive the request, but forwarding adds a network call. |
| DIRECTORY_TIER | The directory points to an old owner, n2. | n1 tries n2. If n2 rejects it, n1 retries once using the owner supplied by partitioning. | The retry gets the value from n0 if it is reachable. | A stale entry needs an extra attempt. The request fails if that attempt also fails. |
| CLIENT_AWARE | The client contacts the wrong node. | n1 returns `not owner` and stores nothing. | n1 returns `not owner`. The client must contact n0. | The server does less work, but the client must know the owner. |

## Implementation

`on_write` and `on_read` call one shared method, `_route`, because their routing
steps are the same. Only the RPC name and response type differ.

1. Read the mode. An empty mode means FORWARD.
2. If this node owns the key, return `None` so the starter handles it locally.
3. In CLIENT_AWARE, return `not owner`.
4. If the request already has 8 hops, return `hop limit` instead of forwarding.
5. In FORWARD, send the request to the owner using `node.rpc`.
6. In DIRECTORY_TIER, try `node.directory[key]`, or the supplied owner if there
   is no entry. If that node returns `not owner` or is unreachable, retry once
   using the owner supplied by partitioning.

Each outgoing request is copied with `copy_with_hop`, which adds one hop and
preserves the key, value, and other request fields. DIRECTORY_TIER changes the
copy's routing mode to CLIENT_AWARE so a stale destination rejects it instead
of forwarding again. The retry starts from the same incoming request, so it
has the same hop count as the first attempt.

An unreachable owner returns `owner unreachable`. A local owner may handle a
request at hop 8; it cannot be forwarded a ninth time. The starter also rejects
incoming requests above hop 8. The plugin does not change directory entries,
because partitioning can use them to pin ownership.

## Demo

Run from `week06/`, with the other plugins left as starter stubs. In this setup,
n0 owns all keys. Start these in three separate terminals:

```bash
python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
```

Then run these client commands:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write alpha one --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read alpha
python3 -m platform.client --addr 127.0.0.1:9601 read alpha --request-routing FORWARD
```

Expected: the Write succeeds. Both Reads return `one` from n0.

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write beta two --request-routing DIRECTORY_TIER
python3 -m platform.client --addr 127.0.0.1:9602 read beta --request-routing DIRECTORY_TIER
```

Expected: the Write succeeds and the Read returns `two` from n0.

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write gamma three --request-routing CLIENT_AWARE
python3 -m platform.client --addr 127.0.0.1:9600 read gamma
```

Expected failure: CLIENT_AWARE on a non-owner returns `not owner`. The Read on
n0 reports `found=False`, because that Write was never forwarded.

```bash
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate on
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate off
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
```

Expected: the isolated Write returns `owner unreachable`. After recovery, the
Write succeeds.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

The 11 tests use gRPC nodes and check forwarding, local ownership, rejection,
hop limits, a forwarding loop, isolation, directory retry, and invalid modes.
The stale-directory test sets n1's entry to n2 while n0 owns the key, then
checks that the Write reaches n0 and n2 stores nothing. There is no client
command to inject a stale directory entry, so that case uses the test file.

These tests check my plugin independently. The team should also check the
combined plugins: the starter calls leader admission before write routing,
so an implemented leader plugin can bypass write routing.
