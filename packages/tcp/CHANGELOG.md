# Changelog

## 3.0.1

### Fixed
- A node started without `SPIDERMESH_NODE_HOSTNAME` announced an empty `host`, and calling it made
  `Http2Rpc` build `http://:<port>`: the call failed with a bare `ERR_INVALID_URL` (no service, no
  node). `TopologyDiscoveryAdapter` now fills an empty `host` from the discovery sender address
  (`remote_host`), as 2.x did, so the variable is optional with UDP discovery.
- `Http2Rpc` reports a node without a host, or an address `http2.connect()` rejects, as
  `MICROSERVICE_OFFLINE` with the node id and address. `Http2Pubsub` names the node too.
- CHANGELOG 3.0.0: removed the reference to `bindMeshDiscovery()` / `@spider-mesh/discovery` (never
  published; use `TopologyDiscoveryAdapter`) and the leftover note that reconnect stops and removes
  the peer (it keeps retrying; `Topology.removeUnreachableAfterMs` removes it).

### Changed
- `@simple-discovery/udp` peer range is `^3.0.2`: from that release `remote_host` is the real sender
  even when the packet reached this process through the same-host relay.
- README: "Chuyển từ 2.x" with before/after code and the renamed environment variables.

## 3.0.0 — generic discovery and TCP-owned liveness

- `Http2Rpc.name = 'http2'`; metadata và routing không còn suy luận từ tên class.
- Nhận Topology qua SpiderMesh lifecycle, hỗ trợ infrastructure `resolveService` và probe.
- Tách membership khỏi HTTP/2 reachability để transporter không xóa node thay Discovery.
- `Http2Pubsub.name = 'http2-pubsub'` và đọc topic membership từ Topology.
- Xử lý `ERR_STREAM_DESTROYED` phát sinh sau khi Node đã gửi terminal frame, tránh provider Linux
  crash khi caller dùng RPC như một Promise.
- Xác nhận matrix ba host theo cả 6 hướng và round-robin qua hai provider Linux.
- `Http2Rpc` báo endpoint `suspect`/`unreachable`/`reachable` vào Topology; không tự sở hữu hoặc
  xóa membership.

### Breaking
- `Http2Pubsub` remains exported here, but is registered on `EventBus` from
  `@spider-mesh/events` instead of `SpiderMesh`.
- Removed `UdpDiscovery` and all UDP configuration from `@spider-mesh/tcp`. Use
  `UdpDiscovery<SpiderMeshNode>` from `@simple-discovery/udp`, wrapped in `TopologyDiscoveryAdapter`
  (exported here) and passed to `new Topology({ discovery })`. `SPIDERMESH_WHITELIST_ADDRESS`,
  `SPIDERMESH_MULTICAST_ADDRESS` and `SPIDERMESH_MULTICAST_PORT` become
  `SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS`, `SIMPLE_DISCOVERY_UDP_MULTICAST_ADDRESS` and
  `SIMPLE_DISCOVERY_PORT`. See "Chuyển từ 2.x" in the README.
- `SpiderMesh` takes `{ topology, transporters }`; `Registry` is no longer passed to transporters.
  Without a `topology`, `Http2Rpc` finds no nodes.
- The legacy `{ hi, node }` UDP wire format is no longer supported by this package.

### Changed
- Declares `@simple-discovery/udp@^3.0.0` as an optional peer dependency: the documented UDP setup pairs the two,
  so package managers now warn about a mismatched major. `msgpackr` is pinned to the tested `^1.12.1`.
- Added an RPC contract e2e matrix (`examples/contract/`, shared verbatim by `@spider-mesh/ws` and
  `@spider-mesh/tcp`), 74 checks: 17 method shapes (sync, async, sync/async observable with immediate
  or delayed values, empty streams, and every error position — sync throw, async reject, observable
  method throwing before returning, async observable rejecting, observable erroring immediately,
  after values or inside an operator) each consumed with both `subscribe` and `await`; 5 error kinds
  (`Error`, `{ code, message }`, string, `Error` with `code`, `Error` subclass) × 4 error paths; data
  types; unsubscribe and `await` both stopping the provider stream; not found, timeout, Promise
  chaining and concurrency.
- **`Http2Rpc` never gives up on a node that is still in Topology.** After
  `SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS` failures it reports the node `unreachable` (so it is not
  chosen for calls) but keeps retrying with exponential backoff capped at
  `SPIDERMESH_HTTP2_RECONNECT_MAX_DELAY_MS` (default 30 s). Previously it stopped for good and only a
  new `node_id` or address restarted it; UDP heartbeats re-announcing the same node never did, so a
  few seconds of network trouble left a live node uncallable until a process restarted.
- A node announced with a new address or port (process restart) is retried immediately instead of
  waiting out the current backoff.
- The recommended UDP setup no longer uses heartbeats or `staleAfterMs`: UDP only finds nodes, and
  `Topology.removeUnreachableAfterMs` removes nodes whose HTTP/2 connection stays down.
- `TopologyDiscoveryAdapter` (plus `DiscoveryMessage`, `DiscoveryTransporter` and the adapter
  options) is now exported from `@spider-mesh/tcp`. It plugs a discovery such as `@simple-discovery/udp` into
  Topology, which is the setup this package is used with, so the separate `@spider-mesh/discovery`
  package is not needed and is not published.
- `Http2Rpc` proactively connects to every routable discovered peer. HTTP/2 session and underlying
  TCP socket `close`/`error`, followed by reconnect, own online/offline state; UDP has no periodic
  heartbeat.
- Added `SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS` (default `2000`).

### Validation
- Standard TCP E2E: 16/16 pass.
- TCP resilience: 6/6 pass, including unreachable-then-recover, restart soak, SIGKILL
  eviction/recovery, full-snapshot replacement, duplicate-ID isolation, and interrupted RPC streams.
- Three-host Bun test: Service A discovered and called both Service B providers and both Service C
  providers across two remote servers; lifecycle tests passed against each remote server.

## 2.0.154 — UDP discovery survives ICMP errors on Linux

### Fixed
- `UdpDiscovery` no longer tears down its socket when a send triggers an ICMP
  "destination/port unreachable". On Linux, sending a discovery datagram to a host/port with no
  listener surfaces on the next `recv` as a socket `'error'` event (`ECONNREFUSED` / `ENETUNREACH` /
  `EHOSTUNREACH` / `ECONNRESET`). The old handler treated **any** socket error as fatal, closing the
  discovery socket — so unicast peer discovery (whitelist scan of an IP range, where most addresses
  have no listener) killed the whole mesh after the first probe. These specific codes are now
  ignored; genuine errors still propagate and tear down as before.

### Why
macOS never delivers these ICMP-derived errors to the UDP socket, so the bug only manifested on
Linux — discovery worked in local dev but died on Linux servers behind a VPC without multicast
(forcing the unicast `SPIDERMESH_WHITELIST_ADDRESS` path).

## 2.0.152 — reachability-aware routing (`canRoute`)

Pairs with the **breaking** `@spider-mesh/core` 2.0.152 (`RpcTransporter.canRoute` is now
required). For users of the provided transporters this release is **additive** — `Http2Rpc`
already implements the new contract, so no action is needed.

### Added
- `Http2Rpc#canRoute(service, node_id?)` — implements the now-**required**
  `RpcTransporter.canRoute` contract. Returns `true` only when a peer serving `service` exists in
  the registry **and** has advertised its HTTP/2 endpoint port (`#hasRpcEndpoint`); honors an
  explicit `node_id`. Side-effect free — uses `registry.listPeers`, so it does **not** advance the
  `pickRpcNode` round-robin index.

### Why
Core's `#selectRpcTransport` now prefers a transporter whose `canRoute` is `true` before falling
back to registration order. This fixes a bug where, with several RPC transporters registered (e.g.
`Http2Rpc` for LAN + a relay `WebsocketTransporter`), a default RPC to a relay-only provider was
dispatched through `Http2Rpc` — which had no LAN route — and threw a misleading
`MICROSERVICE_OFFLINE` even though `wait()`/`nodes` reported the provider online. With `canRoute`,
`Http2Rpc` declines when it has no routable peer, so routing follows the same source of truth as
availability.

## 2.0.145 (BREAKING) — tcp owns its discovery

Pairs with `@spider-mesh/core` 2.0.145 (registry-free core).

### Breaking
- Transporter constructors now take the **shared `Registry`**:
  `new UdpDiscovery(registry)`, `new Http2Rpc(registry)`, `new Http2Pubsub(registry)`.
  Construct one `Registry` and inject it into all three.
- `linkRegistry()` was **removed** from all three transporters (replaced by the constructor argument).

### Changed
- `UdpDiscovery` now **ingests** discovered nodes into the registry (`upsertPeer`) and
  implements core's `ServiceDirectory` (`watchService` / `listNodes`). This logic moved
  out of core.
- `Http2Rpc` now evicts peers from the registry (`removePeer`) on connection close — this
  also moved out of core.
- **Routing only targets RPC-ready peers**: `UdpDiscovery`'s `ServiceDirectory` and
  `Http2Rpc`'s `pickRpcNode({ filter })` skip a provider that advertised the service but
  not yet its Http2Rpc port — avoids a spurious `MICROSERVICE_OFFLINE` ("metadata missing")
  when a peer is discovered mid-startup.

### Migration
```ts
// before
const mesh = new SpiderMesh(registry)
mesh.registerTransporter(new UdpDiscovery())
mesh.registerTransporter(new Http2Rpc())
mesh.registerTransporter(new Http2Pubsub())

// after
const mesh = new SpiderMesh()
const registry = new Registry()
mesh.registerTransporter(new UdpDiscovery(registry))
mesh.registerTransporter(new Http2Rpc(registry))
mesh.registerTransporter(new Http2Pubsub(registry))
```
