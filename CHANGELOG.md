# Changelog

## 2.0.152 — reachability-aware routing (`canRoute`)

Pairs with the **breaking** `@spider-mesh/core` 2.0.152 (`RpcTransporter.canRoute` is now
required). For users of the provided transporters this release is **additive** — the WebSocket
transporters already implement the new contract, so no action is needed.

### Added
- `BaseWebsocketTransporter#canRoute(service, node_id?)` — implements the now-**required**
  `RpcTransporter.canRoute` contract (inherited by `WebsocketTransporter` /
  `GlobalWebsocketTransporter`). Returns `true` only when at least one relay socket is open **and**
  the relay has reported a node serving `service` (honoring an explicit `node_id`) — mirroring
  `#selectRpcSocket`.

### Why
Core's `#selectRpcTransport` now prefers a transporter whose `canRoute` is `true` before falling
back to registration order, so when several RPC transporters are registered a default RPC reaches
the provider through the transporter that actually sees it (the relay), instead of a LAN transporter
that cannot. Fixes a misleading `MICROSERVICE_OFFLINE` for relay-only providers.

## 2.0.145 — relay-backed availability (no registry)

Pairs with `@spider-mesh/core` 2.0.145 (registry-free core).

### Changed
- `WebsocketTransporter` now implements core's `ServiceDirectory`
  (`watchService` / `listNodes`), sourced from the relay `hello` / `offline` frames it
  already tracks in its node map. So `new SpiderMesh()` works with **no `Registry`** — the
  relay does routing and the transporter supplies availability.

### Migration
```ts
// before
const registry = new Registry()
const mesh = new SpiderMesh(registry)
mesh.registerTransporter(transporter)

// after
const mesh = new SpiderMesh()
mesh.registerTransporter(transporter)
```
No other changes — drop the `Registry` import and the constructor argument.
