# Request Routing Design

**Student:** Karl Savinskiy

**CUNY alias:** ksavins000

**Plugin:** `week06/platform/plugins/request_routing.py`

## Plan

I will implement `on_write` and `on_read`. They will decide what to do when a request reaches a node that does not own the key. The partitioning plugin will determine the owner.

If the receiving node owns the key, the method will return `None` so the starter can handle the request locally. Otherwise, it will use the selected routing mode.

## Modes and tradeoffs

| Mode | Situation | Planned Write behavior | Expected Read behavior | Tradeoff |
| --- | --- | --- | --- | --- |
| FORWARD | A request reaches n1, but n0 owns the key. | n1 will forward the Write to n0. | A Read through n1 will reach n0 and return the stored value. | The client can contact any node, but forwarding adds a network hop. |
| DIRECTORY_TIER | A directory lookup points to an old owner. | The node will look up the destination and send the Write. If the lookup is stale, it will refresh the destination and retry once. | A Read will use the same lookup and retry rule. If the refreshed owner is reachable, it will return that owner's value. | The lookup and possible retry add work. A second failure will be returned to the client. |
| CLIENT_AWARE | A request reaches n1, but n0 owns the key. | n1 will return `not owner` without forwarding or storing the value. | A Read on n1 will return `not owner`. The client must read directly from n0. | The server avoids forwarding, but the client must contact the correct owner. |

## Forwarding and errors

I will use the provided `node.rpc` helper to contact other nodes. Each forwarded request will be copied with its hop count increased by one. A request that would need more than eight forwarding hops will fail with `hop limit` so it cannot loop forever.

For DIRECTORY_TIER, the selected destination will be asked to handle the request only if it owns the key. If it reports `not owner`, the sending node will refresh the destination and retry once. It will also allow one retry after a connection failure. An unreachable destination after the allowed attempts will return an error.

This plugin will route requests only. It will not choose key ownership, replicate values, or change teammates' plugin files.

## Planned demo

I will test the plugin with fresh nodes and the other plugins left as starter stubs. In this setup, n0 will own all keys. Commands will run from `week06/` after installing the required libraries.

1. Start one node in each of three terminals:

```bash
python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
```

2. Send a Write through n1 using FORWARD, then read from n0:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write alpha one --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read alpha
```

Expected: the Write will succeed, and the Read on n0 will return `one`.

3. Use DIRECTORY_TIER for a Write through n1 and a Read through n2:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write beta two --request-routing DIRECTORY_TIER
python3 -m platform.client --addr 127.0.0.1:9602 read beta --request-routing DIRECTORY_TIER
```

Expected: the Write will succeed, and the Read will be served by n0 with the value `two`. I will separately test a stale directory entry and check that the request retries only once; the provided CLI has no command to inject that entry.

4. Send a CLIENT_AWARE Write to the wrong node:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 write gamma three --request-routing CLIENT_AWARE
python3 -m platform.client --addr 127.0.0.1:9600 read gamma
```

Expected: the Write will return `not owner`, and the Read on n0 will report `found=False`.

5. Isolate n1, try forwarding, then restore connectivity and retry:

```bash
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate on
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read delta
python3 -m platform.client --addr 127.0.0.1:9601 fault --isolate off
python3 -m platform.client --addr 127.0.0.1:9601 write delta four --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9600 read delta
```

Expected: during isolation, the Write will fail with `owner unreachable` and n0 will report `found=False`. After recovery, the Write will succeed and n0 will return `four`.
