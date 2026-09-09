# Week 1: Distributed prime-counting simulation

This folder is a small in-class cluster. You will run a **worker** on your laptop; the instructor runs a **coordinator**. Together they count primes in a numeric range `[low, high)` (low included, high excluded).

The point is not the primes. The point is to feel the difference between:

- **sequential** work on one CPU
- **shared-memory parallelism** on one machine (threads vs processes)
- **distribution** across many machines (coordinator + workers over HTTP)

There are no extra Python packages. Use Python **3.10 or newer**.

## Files

| File | Role |
| --- | --- |
| `primes_in_range.py` | Segmented sieve: the actual CPU work |
| `primes_cli.py` | Client: run locally (`single` / `threads` / `processes`) or send a job to the primary (`distributed`) |
| `primary_node.py` | Coordinator: registry of workers, splits the range, aggregates results (**instructor**) |
| `secondary_node.py` | Worker HTTP server: registers with the primary, computes a slice (**you**) |

## Get the code

Clone or pull the class repository (see the [repo README](../README.md) for Bitbucket / SourceTree). Then work from this directory so the `primes_in_range` import resolves:

```bash
cd week01
```

## What happens in class

```
  your laptop                         instructor laptop
  -----------                         -----------------
  secondary_node.py                   primary_node.py
       |  POST /register                    |
       |----------------------------------->|
       |                                    |
       |                          primes_cli.py --exec distributed
       |                                    |
       |  POST /compute  (your slice)       |
       |<-----------------------------------|
       |  JSON result                       |
       |----------------------------------->|  (aggregates all workers)
```

1. The instructor starts `primary_node.py` and writes its URL on the board (shape only: `http://HOST:9200`). Use the URL announced that day, not an old example.
2. Each student starts `secondary_node.py` pointed at that URL.
3. The instructor checks who is registered (`GET /nodes`), then submits one timed distributed job. **You do not need to run `primes_cli.py` unless asked.** Watch your terminal.

### Your command

`--host 0.0.0.0` is required. The default bind address is `127.0.0.1`, which only accepts connections on your own machine. Registration can still succeed (your laptop talks *out* to the instructor) while the later compute call fails (the instructor cannot talk *in*).

Use your email alias as `--node-id` (for `kbrown6@ccny.cuny.edu`, use `kbrown6`).

```bash
python3 secondary_node.py --host 0.0.0.0 --port 9100 --node-id YOUR_ALIAS --primary http://INSTRUCTOR_HOST:9200
```

You should see something like:

```
[secondary_node] node_id=YOUR_ALIAS
[secondary_node] listening on http://0.0.0.0:9100
[secondary_node] advertised as http://YOUR_LAN_IP:9100
[secondary_node] registering to primary: http://INSTRUCTOR_HOST:9200
```

Leave this process running. Stop it with Ctrl-C when class is done.

If two people share one computer, pick different `--port` values (9100, 9101, …) and different `--node-id` values.

### Optional: confirm your worker is up

In a **second** terminal on the same laptop:

```bash
python3 -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:9100/health').read().decode())"
```

You want `"ok": true`. That only proves the process is listening locally; the instructor still has to be able to reach your advertised LAN IP.

## Try the same work on one laptop

Before or after the cluster run, compare execution modes locally. These do **not** need the primary:

```bash
python3 primes_cli.py --low 0 --high 20_000_000 --exec single --time --mode count
python3 primes_cli.py --low 0 --high 20_000_000 --exec threads --time --mode count
python3 primes_cli.py --low 0 --high 20_000_000 --exec processes --time --mode count
```

Stdout is the prime count. Elapsed time prints on stderr when you pass `--time`.

Use `--mode count` for large ranges so you are not shipping huge lists of primes over the network. Threads often do **not** speed up this CPU-bound work in CPython (the GIL). Processes usually do, on one machine. Distribution adds network and coordination overhead, but can use many laptops at once.

## Troubleshooting

Registration is **outbound** (you → instructor). Compute is **inbound** (instructor → you). The first can work while the second fails.

| Symptom | What to check |
| --- | --- |
| `error when registering node to primary` | Wrong `--primary` URL, primary not started yet, or you are on a different network |
| Registered, but your slice never runs / times out | You forgot `--host 0.0.0.0`; OS firewall blocking inbound TCP on 9100; campus Wi-Fi **client isolation** (peers cannot talk to each other — try the lab network the instructor specifies) |
| Primary reaches the wrong machine | Advertised IP is wrong. Restart with `--public-host YOUR_LAN_IP` (from `ifconfig` / `ipconfig` / Settings) |
| `Address already in use` | Port 9100 is taken. Use `--port 9101` (or another free port) |
| Duplicate `node_id` in the registry | Two people used the same alias. Change `--node-id` |

## What you should take away

- **Coordinator vs workers.** The primary does not sieve the full range. It splits `[low, high)` evenly across whoever is registered, waits for answers, and sums them. Slices are not weighted by CPU count.
- **Registration is crude service discovery.** Your secondary announces `host`, `port`, and `node_id`. If the advertised host is not reachable from the primary, the cluster looks fine until work is assigned.
- **Bind address vs advertised address.** Listening on `127.0.0.1` is not the same as advertising your LAN IP. Other machines cannot connect to localhost on *your* laptop.
- **Parallel is not the same as distributed.** Threads and processes share one machine. Distributed workers share a network. Measure all three; do not assume “more machines” is always faster for a given range.
