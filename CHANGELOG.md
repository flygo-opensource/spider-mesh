# Changelog

## 3.0.0 — core 3 compatibility

- WebSocket transporter có wire name `websocket` và nhận trực tiếp trong SpiderMesh constructor.
- Cùng instance có thể implement `TopologyDiscovery` khi ứng dụng cần enumerate/watch node.
- Không có Topology, relay directory cung cấp probe và relay tự route RPC.
- Khi dùng làm Discovery, client có thể verify membership từ relay directory; mất kết nối trả
  `unknown` để không xóa node nhầm.
- Mất toàn bộ relay được báo thành endpoint `unreachable`; relay disconnect không còn bị hiểu sai
  thành tất cả node đều offline.

### Breaking
- Requires `@spider-mesh/core@^3.0.0` and `@spider-mesh/discovery@^3.0.0`.
- Event methods remain on the shared WebSocket transporter, but the instance is registered on
  `EventBus` from `@spider-mesh/events`; `SpiderMesh` no longer owns pub/sub.
- Availability is exposed directly through transporter `watchService()` / `listNodes()` methods;
  the separate core `ServiceDirectory` type no longer exists.

### Fixed
- The relay no longer abandons an in-flight RPC when one end disconnects. `#pendingRequests` now
  holds `{ caller, provider, service }`, and `on_close` closes both directions: a dead provider
  sends `MICROSERVICE_OFFLINE` back to its caller, and a dead caller sends `cancel` to its
  provider. For round-robin requests the relay is the only party that knows the pair, so a
  long-lived `Observable` used to hang silently forever.
- A request routed to a node whose socket just closed answers `MICROSERVICE_OFFLINE` instead of
  being dropped without a reply.
- In-flight RPCs no longer hang when the connection to the relay itself drops. The transporter
  tracks which socket carried each request and, when that socket is torn down, ends every
  request still pending on it with `MICROSERVICE_OFFLINE`. Previously both streams and awaited
  calls stayed silent forever unless the caller had set a `timeout`. Membership is untouched —
  losing the relay still does not mean a node is offline — and requests are not resent through
  another relay, since they may already have run on the provider.
- `BaseWebsocketTransporter.send()` reports the `destination_node_id` it resolved through
  Topology, letting core close the matching stream when that node goes offline.
- WebSocket discovery transporters implement `@spider-mesh/discovery`'s generic envelope contract;
  applications bind local announcements with `bindMeshDiscovery()`.
- Explicit transporter close tears down Node `ws` connections and clears connection status
  deterministically; synchronous socket creation no longer defers listener registration.

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
