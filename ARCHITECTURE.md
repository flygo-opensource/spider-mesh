# @spider-mesh/tcp — Architecture

Internal structure of `@spider-mesh/tcp`, for maintainers and contributors.
For *how to use* it, see [README.md](README.md). For build/test/conventions, see [AGENTS.md](AGENTS.md).

This package implements the `@spider-mesh/core` transporter contracts for a **LAN / multicast-capable** environment (Node/Bun only — it uses raw UDP and HTTP/2). Since core is registry-free, tcp **owns its own routing**: the consumer constructs one `Registry` and injects it into all three transporters; `UdpDiscovery` fills it and exposes it to core as a `ServiceDirectory`, while `Http2Rpc`/`Http2Pubsub` read it for routing.

## Module Map

```
src/
├── index.ts          # exports the three transporter CLASSES
├── types.ts          # re-exports core transporter types (core is the contract source of truth)
├── const.ts          # env-var-backed configuration constants
├── UdpDiscovery.ts   # DiscoveryTransporter over UDP multicast
├── Http2Rpc.ts       # RpcTransporter over HTTP/2
└── Http2Pubsub.ts    # PubsubTransporter over HTTP/2
```

All three are `export class … extends Subject<…Event>`. Their **only constructor argument is the shared `Registry`** (`new Http2Rpc(registry)` etc.) — inject the same instance into all three. There are no other per-instance options; every network knob is an env var read once in `const.ts` at module load. `msgpackr` encodes all payloads.

## Configuration model

`const.ts` is the single configuration surface:

| Constant | Source | Default |
| --- | --- | --- |
| `SPIDERMESH_MULTICAST_ADDRESS` | env | `239.0.0.3` |
| `SPIDERMESH_MULTICAST_PORT` | env | `20002` |
| `SPIDERMESH_WHITELIST_ADDRESS` | env | unset |
| `SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE` | `!process.env.…` | `true` when unset |

**HTTP/2 ports are not configurable** — `Http2Rpc` and `Http2Pubsub` call `listen(0)` and take an OS-assigned ephemeral port, advertised through node metadata so peers can connect. Only the multicast socket binds a fixed (configurable) port.

> `SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE` is **dead code**: it is read into a local `auto` in `Http2Rpc.ts` but never used. There is no host-based load-balancing branch. Either wire it up or delete it in a future change; do not document it as functional.

## `UdpDiscovery` — discovery over multicast

`extends Subject<DiscoveryEvent>`, implements `broadcast(MdnsMessage<NodeMetadata>)` and (since the registry-free refactor) `ServiceDirectory`. It **receives the shared `Registry` via its constructor** (`new UdpDiscovery(registry)`), **ingests** each discovered node into it (`registry.upsertPeer`), and exposes it to core via `watchService`/`listNodes`. This is the glue that used to live in core.

Behavioral rules baked into the socket handler:

- **Self-filtering:** packets whose `node_id` is our own are dropped.
- **Namespace isolation:** packets whose `namespace` differs from ours are dropped — so one multicast group can host several isolated meshes.
- **Host normalization:** a peer's `host` is rewritten from the UDP sender address when needed (peers don't have to know their own routable address).
- **`hi` probe / reply:** new nodes probe; existing nodes reply (unicast to the sender when remote, multicast when local).
- **Forward-once relay:** remote packets are re-emitted to multicast a single time, tagged by `forwarder_id` to prevent loops — extends reach across nodes that can't all see each other.
- **Whitelist fan-out (`SPIDERMESH_WHITELIST_ADDRESS`):** comma-separated; a 4-octet entry is a literal target, a **3-octet prefix expands to the whole `.1`–`.254` /24**. Use when multicast is blocked.

Teardown: `unsubscribe()` stops the loop and closes the socket.

## `Http2Rpc` — RPC over HTTP/2

`extends Subject<RpcEvent>`, receives the shared `Registry` via its constructor (`new Http2Rpc(registry)`) and implements `send(packet) → { cancel }`. Accepts `RpcRequestPacket | RpcCancelPacket | RpcResponsePacket` (a superset of the core contract — it handles cancel frames on the wire itself).

- Exposes local HTTP/2 endpoint metadata (`metadata` getter) so peers learn the ephemeral port.
- **Routing:** by `destination_node_id` when present; otherwise `registry.pickRpcNode(service, { filter })` (round-robin) where `filter` = `#hasRpcEndpoint` — so a provider that advertised the service but not yet its Http2Rpc port is skipped (no "metadata missing"). Requires a registry for `request` packets — throws `MICROSERVICE_OFFLINE` if none.
- **Eviction:** on a connection close it calls `registry.removePeer(node_id)` (this moved out of core).
- **Streaming:** request/response streams are preserved across the HTTP/2 connection.
- **Cancel:** the `cancel()` returned for a `request` sends a `RpcCancelPacket` to the same destination over the existing connection; the provider unsubscribes its Observable on receipt.

Teardown: `unsubscribe()` closes streams, destroys connections, closes the server.

## `Http2Pubsub` — pub/sub over HTTP/2

`extends Subject<PubsubEvent>`, receives the shared `Registry` via its constructor (`new Http2Pubsub(registry)`), and implements `publish(topic, data)`, `listen(topic): Subject`. `publish` resolves subscribers via `registry.listTopicNodes(topic)` and pushes encoded payloads to each. `listen` returns an RxJS stream per topic.

Teardown: `close()` — **note the inconsistency**: this transporter uses `close()` while the other two use `unsubscribe()`. Worth unifying.

## Known coupling: the `Http2Rpc` transporter name

RPC-readiness is detected by looking for an **`Http2Rpc`** entry (with a `port`) in a peer's
`transporters` metadata — in two places: `UdpDiscovery#isRpcReady` (filters the
`ServiceDirectory` and the `pickRpcNode` candidates) and `Http2Rpc#getTransporterMetadata`
(resolves the port to dial). The default registered name is the class name `Http2Rpc`, so a
standard setup is consistent. **Registering `Http2Rpc` under a custom name breaks both lookups**
(the metadata would be stored under that custom key). If per-name flexibility is ever needed,
thread the rpc transporter name through both classes.

## Cross-cutting notes for future work

- **Teardown is not uniform** (`unsubscribe()` vs `close()`). A single `dispose()`/`Symbol.dispose` contract across all three would be cleaner.
- **The only constructor argument is the shared `Registry`.** Network config still lives in module-load constants; if per-instance network config is ever needed (e.g. multiple meshes in one process on different ports), that is a separate structural change.
- **Browser/RN are out of scope** by design; that's `@spider-mesh/ws`'s job.
- **Contract drift:** `Http2Rpc.send()` accepts a 3-packet union but the core `RpcTransporter` contract only types 2. Keep them aligned when editing `core/src/types.ts`.

## Test layout

E2e suites live in `tests/` and are run individually by `test:e2e`; standalone smoke/matrix runners live in `examples/`. See [AGENTS.md](AGENTS.md) for commands.
