# Remote lifecycle test

Runs an observer and provider as independent processes, optionally on different hosts.

Expected observer sequence:

1. `provider-online` with `occurrence: 1`.
2. Stop the provider and wait for HTTP/2 reconnect failures.
3. `provider-offline`.
4. Start the provider as a new process with a new `SPIDERMESH_NODE_ID`.
5. `provider-online` with `occurrence: 2` and `rediscovered: true`.

Both scripts use generic `@ohayo/udp` one-shot discovery, share a `Topology`, and use a real
ephemeral `Http2Rpc` endpoint. Discovery owns membership; TCP reachability produces the observed
online/offline transition without deleting the old node. Set
`OHAYO_UDP_WHITELIST_ADDRESS` when multicast is unavailable.

The harness builds node snapshots directly, so `SPIDERMESH_NODE_ID` is a deterministic test label.
Production `SpiderMesh` instances ignore that environment variable and generate a random ID.

## Run with Bun on two hosts

Use the same values on observer and provider:

```bash
export SPIDERMESH_NAMESPACE=ohayo-lifecycle
export OHAYO_DISCOVERY_KEY=ohayo-lifecycle-key
export OHAYO_DISCOVERY_PORT=28447
export OHAYO_UDP_WHITELIST_ADDRESS=192.168.1.10,192.168.1.20
export SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS=3
export SPIDERMESH_HTTP2_RECONNECT_DELAY_MS=100
export SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS=1000
```

Start the observer on `192.168.1.10`:

```bash
SPIDERMESH_NODE_HOSTNAME=192.168.1.10 \
SPIDERMESH_NODE_ID=observer-local \
PROVIDER_NODE_IDS=provider-sv2-1,provider-sv2-2 \
bun run observer.mjs
```

Start generation 1 on `192.168.1.20`:

```bash
SPIDERMESH_NODE_HOSTNAME=192.168.1.20 \
SPIDERMESH_NODE_ID=provider-sv2-1 \
PROVIDER_GENERATION=1 \
bun run provider.mjs
```

After the observer reports `provider-online`, kill the provider process. The observer must report
`provider-offline` after the configured reconnect threshold. Restart as a new node incarnation:

```bash
SPIDERMESH_NODE_HOSTNAME=192.168.1.20 \
SPIDERMESH_NODE_ID=provider-sv2-2 \
PROVIDER_GENERATION=2 \
bun run provider.mjs
```

Expected observer events:

```json
{"event":"provider-online","occurrence":1,"rediscovered":false,"generation":1}
{"event":"provider-offline","node_id":"provider-sv2-1"}
{"event":"provider-online","node_id":"provider-sv2-2","occurrence":2,"rediscovered":true,"generation":2}
```

The restarted process must use a new node ID and advertise a new ephemeral RPC port. This proves
that the old endpoint became unreachable and the new incarnation arrived through discovery without
periodic UDP heartbeats.

## Last verified

Verified on 2026-07-14 against both `192.168.1.20` and `192.168.1.30` using socket lifecycle, bounded
TCP retry, and Ohayo's own one-shot reply—without active ping: SIGKILL produced offline, then
generation 2 with a new node ID and HTTP/2 port was rediscovered as occurrence 2.
