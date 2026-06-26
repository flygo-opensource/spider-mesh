# @spider-mesh/ws — Architecture

Internal structure of `@spider-mesh/ws`, for maintainers and contributors.
For *how to use* it, see [README.md](README.md). For build/test/conventions, see [AGENTS.md](AGENTS.md).

This package implements all three `@spider-mesh/core` transporter contracts over WebSocket, routed through a central **relay**. Unlike `@spider-mesh/tcp`, nodes do not talk peer-to-peer or rely on multicast — they all connect to a relay that forwards frames. This is what makes it work across subnets, cloud VPCs, browsers, and mobile.

## Module Map

```
src/
├── BaseWebsocketTransporter.ts     # the real transporter: implements all 3 contracts, connection mgmt
├── WebsocketTransporter.ts         # Node/Bun subclass — sockets via the `ws` npm library
├── GlobalWebsocketTransporter.ts   # browser/RN subclass — sockets via globalThis.WebSocket
├── WebsocketRelayServer.ts         # the relay process: routing + discovery fan-out
├── websocketProtocol.ts            # frame definitions + msgpack encode/decode
│
├── node.ts            # entry → re-exports WebsocketTransporter (the `ws`-based class)
├── browser.ts         # entry → re-exports GlobalWebsocketTransporter
├── react-native.ts    # entry → identical to browser.ts (byte-for-byte)
└── relay-server.ts    # entry → re-exports WebsocketRelayServer
```

`package.json` exposes **four subpath exports** (`./node`, `./browser`, `./react-native`, `./relay-server`) and **no root `.` export**. All three transporter entries export a class literally named `WebsocketTransporter`; only the socket backend differs.

### Inheritance

```
BaseWebsocketTransporter   (implements RpcTransporter, DiscoveryTransporter, PubsubTransporter)
        ├── WebsocketTransporter        (node.ts)        → new WebSocket via `ws`
        └── GlobalWebsocketTransporter  (browser/RN)     → new globalThis.WebSocket (throws if absent)
```

The base class is abstract over *how a socket is created*; subclasses only supply that. Everything else — reconnect, heartbeat, frame routing, status tracking — lives in the base.

## `BaseWebsocketTransporter`

One instance serves RPC, discovery, and pubsub simultaneously (that is why `mesh.registerTransporter(transporter)` links all three capabilities on a single object).

- **Multi-relay connections:** holds a `#connections` map keyed by URL. `connect(url)` is idempotent per URL and may be called for several relays for redundancy; `close(url)` drops one.
- **`status$`:** `BehaviorSubject<Map<url, status>>`, status ∈ `connecting | connected | error | not_connected`. Per-URL, not global — monitor the map.
- **Options (with defaults):** `heartbeatIntervalMs` (30000), `reconnectIntervalMs` (1000), `unsubscribeDelayMs` (10000). Reconnect is automatic on the interval.
- **RPC:** `send(RpcRequestPacket | RpcResponsePacket) → { cancel }`. The transporter is **not registry-aware** — it forwards every frame to the relay and lets the relay route. `cancel()` sends a cancel frame; the relay maps it back to the right provider socket.
- **Reachability (`canRoute`):** implements core's now-**required** `RpcTransporter.canRoute(service, node_id?)`. Returns `true` only while at least one relay socket is open **and** the relay has reported a node serving `service` (honoring an explicit `node_id`) — mirrors `#selectRpcSocket`. Lets core's `#selectRpcTransport` prefer this transporter over a LAN one for relay-only providers.
- **Discovery:** `broadcast(data)` announces local identity (sets the local node id, pushes into the local-node subject). Inbound `hello` frames re-emit a `discovered` event **only when the peer's advertised service list changed** (`#hasSameServices` dedup); `offline` frames evict.
- **Pubsub:** `publish(topic, data)` / `listen(topic)`.

## `WebsocketRelayServer`

The relay is the routing brain; it hosts **no application services**.

- **Options:** `port` (default 8787, `server.port` returns the bound port or `null`), `host`, `path` (HTTP upgrade path passed to the underlying `ws` server), and `isServerConnection(socket, request) → boolean`.
- **`isServerConnection` is the access-control / classification hook** — there is no separate `auth` option. Return `true` for connections that participate in discovery and RPC routing (real mesh nodes), `false` for passive clients. On an exposed relay, validate a token from the upgrade `request` here.
- **Routing:** by `destination_node_id` when present; otherwise `#pickServiceNode(service)` round-robin. A `#pendingRequests` map (`request_id → socket`) lets responses and cancels return to the originating socket.
- **Offline handling:** when no provider exists for a requested service, the relay **emits a `MICROSERVICE_OFFLINE` RPC response** back to the caller. This is asynchronous — the transporter does not throw synchronously.
- Teardown: `close()`.

## Wire protocol (`websocketProtocol.ts`)

msgpack-encoded frames. `type` ∈ `rpc` (with `data.kind` of `request | response | cancel`), `publish`, `subscribe`, `unsubscribe`, `hello`, `offline`. `@msgpack/msgpack` does encode/decode. (`RelayHelloFrame.target_id` exists but is currently unused — dead field.)

## Topology

```
                ┌─────────────────────────┐
   node A ◄────►│   WebsocketRelayServer  │◄────► node C (browser)
   node B ◄────►│  (routing + discovery)  │◄────► node D (react-native)
                └─────────────────────────┘
```

Every node opens a WebSocket to the relay (or to several relays). The relay forwards discovery/RPC/pubsub frames between them. Nodes never connect to each other directly.

## Invariants & notes for future work

- **Relay must be up first.** With no relay, `connect()` simply retries on `reconnectIntervalMs`; nothing discovers anything.
- **`MICROSERVICE_OFFLINE` is a relay-originated response, not a transporter throw.** Don't move that logic into the transporter without re-checking call sites that expect an async response packet.
- **`browser.ts` and `react-native.ts` are identical.** If they ever diverge (e.g. RN-specific socket quirks), split `GlobalWebsocketTransporter` accordingly.
- **No environment variables.** All config is constructor options — unlike `@spider-mesh/tcp`, which is env-only. Keep this distinction in mind when unifying config.
- **Relay is server-runtime only** (`ws` library); the transporter is the only part that runs in browser/RN.
- **Auth is just `isServerConnection`.** If richer auth is needed (per-message, capabilities), this hook is the extension point.

## Test layout

E2e suites live in `tests/` (run individually by `test:e2e`); smoke/matrix/example runners live in `examples/`. See [AGENTS.md](AGENTS.md) for commands.
