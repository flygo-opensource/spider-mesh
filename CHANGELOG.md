# Changelog

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
