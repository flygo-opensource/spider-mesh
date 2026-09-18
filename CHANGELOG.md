# Changelog

## 3.0.0 — modular integrations, registry-free runtime, and random identity

- `SpiderMesh` nhận `transporters` và optional `topology` trong constructor.
- Mỗi RPC transporter bắt buộc có wire name ổn định qua `readonly name`.
- Thêm `Topology`, request-level routing, lifecycle `start/stop` và probe fallback.
- `Registry` trở thành alias compatibility của `Topology`; bỏ availability registration riêng.
- Không gửi cancel thừa khi một terminal response vừa có `data` vừa có `completed`; sửa race
  giữa `firstValueFrom()` và HTTP/2 response stream trên Node/Linux.
- Topology quản lý reachability theo node/transporter, loại endpoint lỗi khỏi routing và phát
  event `suspect`/`unreachable`/`recovered` tức thời.
- Thêm optional `TopologyDiscovery.verify()`; chỉ Discovery được xác nhận `dead` mới xóa node.

### Breaking
- Event linking and pub/sub contracts moved to `@spider-mesh/events`. Replace
  `mesh.linkEvent(...)` with `events.link(...)`, and register event transporters on `EventBus`.
- Discovery contracts and outbound binding moved to `@spider-mesh/discovery`. Replace
  `mesh.registerTransporter(discovery)` with `bindMeshDiscovery(mesh, discovery)`.
- Node identity can no longer be forced through `SPIDERMESH_NODE_ID` or `HOSTNAME`; every
  `SpiderMesh` instance receives a fresh random ID.
- Consumers and companion packages must use the 3.x transporter contracts.
- `SpiderMesh` no longer accepts a Registry constructor argument or exposes `mesh.registry`.
  Transporters provide availability directly through optional `watchService()` / `listNodes()`.
- Removed the separate `ServiceDirectory` type. Availability is now part of transporter capability.

### Fixed
- `retry: N` now retries an offline call exactly `N` times. RxJS counts retries from 1 and the
  check used `count < retry`, so `retry: 1` never retried and `retry: N` retried only `N - 1` times.
- An in-flight RPC whose provider node goes offline now errors with `MICROSERVICE_OFFLINE`
  instead of hanging. `#rpc.pending` tracks the node serving each request (pinned `node_id`, the
  `destination_node_id` a transporter resolved, or `sender_node_id` from the first response), and
  an `offline` event errors every stream bound to that node. Previously a long-lived `Observable`
  stayed silent forever unless the caller had set a `timeout`.
- `cancel` is no longer dropped when `unsubscribe()` runs before `transporter.send()` resolves;
  the request is cancelled as soon as the send settles, so provider streams stop leaking.
- Declares `reflect-metadata` as a runtime dependency, so production installs no longer fail
  while loading `BeforeMicroserviceOnline`.
- Mixed-capability transporters remain authoritative for their own peer lifecycle; core does not
  interpret inbound discovery/RPC/pubsub events as peer storage.
- Node identity is always random per `SpiderMesh` instance. Core no longer reads identity from
  `SPIDERMESH_NODE_ID`, `HOSTNAME`, or any constant derived from environment state.

### Changed
- `Topology` accepts `removeUnreachableAfterMs`: a remote node is removed once every transporter has
  reported it `unreachable` continuously for that long (event reason `'unreachable'`). Repeated
  `unreachable` reports do not restart the window. This lets a discovery be used only for finding
  nodes (UDP) while real connections decide liveness, without heartbeats or `staleAfterMs`.
- `RpcResponsePacket.sender_node_id` (optional) reports the node that answered, so a caller can
  close the right stream when that node disappears.
- `RpcTransporter.send()` may return `destination_node_id` alongside `cancel`, reporting the node
  a transporter actually routed to. Both additions are optional and backward-compatible.
- `SpiderMesh.localNode` and `localNode$` expose the current node snapshot to external integrations.
- Added explicit `AvailabilitySource` and `registerAvailabilitySource()`; RPC transporters that
  implement the availability methods are still registered automatically.
- Discovery envelopes, outbound broadcast binding, reply/convergence policy, Registry updates, and
  liveness now live outside core.
- `Registry` remains exported as a standalone helper for transporters; core runtime does not use it.
- `Registry.watch(service)` emits when an existing peer's metadata version changes, allowing
  half-ready service announcements to become RPC-routable without changing `node_id`.
- `Registry.upsertPeer()` now replaces a full discovery snapshot instead of merging stale fields.
  Partial updates use the explicit `Registry.patchPeer()` API.

### Validation
- Core contract/process E2E: 13/13 pass.
- Cross-host TCP/Ohayo tests confirmed that a third node discovers two providers for each of two
  services across two servers, and that stop/restart emits Registry offline then rediscovery.

## 2.0.152 — reachability-aware RPC routing (BREAKING)

Default RPC routing now follows the same source of truth as availability. Previously, with
more than one RPC transporter registered, a default call (no explicit `transporter` bound) was
dispatched through the **first-registered** transporter — which might have no route to the
provider — throwing a misleading `MICROSERVICE_OFFLINE` even though `wait()` / `nodes` reported
the service online. `#selectRpcTransport` now asks each transporter whether it can actually
reach the target.

### Breaking
- **`RpcTransporter.canRoute(service, node_id?): boolean` is now a required member** of the
  contract. Any custom `RpcTransporter` must implement it (TypeScript flags it at compile time;
  at runtime core calls it unconditionally). A transporter that cannot enumerate reachability
  may simply `return true` to stay a candidate and rely on `send` to surface the real error.

### Changed
- `#selectRpcTransport` (no explicit `transporter`): when ≥1 RPC transporter is registered, it
  returns the first whose `canRoute(service, node_id)` is `true`; only if none claims a route
  does it fall back to registration order. Selection is re-evaluated on retry, so a transporter
  that becomes routable later is picked up automatically.
- A sole RPC transporter (or one whose `canRoute` reports no route yet) still routes via the
  registration-order fallback, so `send` surfaces the real error instead of "no route at all".

### Removed
- The dead self-declared `transporters.rpc` hint path (`#rpcTransporterNameForService`) — it
  read a field no transporter populates and is superseded by `canRoute`.

### Migration
- **core:** none for default usage; `wait()`/`watch()`/`nodes` and explicit `transporter:`
  bindings are unchanged. Multi-transporter setups no longer need a manual `transporter:` bind
  to reach a relay-only provider.
- **custom transporters:** implement `canRoute`. See `@spider-mesh/tcp` (`Http2Rpc#canRoute`,
  registry-backed) and `@spider-mesh/ws` (`BaseWebsocketTransporter#canRoute`, relay-backed)
  for reference implementations.

### Notes
- `canRoute` MUST be side-effect free (e.g. it must not advance a round-robin index).

## 2.0.145 — registry-free core (BREAKING)

`SpiderMesh` no longer takes or owns a `Registry`. Availability (`wait()` / `watch()` /
`nodes`) now comes from each transporter's new `ServiceDirectory` contract, and routing is
owned by the transport.

### Breaking
- `new SpiderMesh(registry)` → **`new SpiderMesh()`**. The constructor takes no arguments.
- `linkRegistry?` was **removed** from the `RpcTransporter` / `PubsubTransporter` /
  `DiscoveryTransporter` contracts. Transports that need a registry receive it via their
  own constructor.
- `ServiceChecker` now receives `NodeRef[]` (a structural subset of `SpiderMeshNode`).

### Added
- `ServiceDirectory` and `NodeRef` types. A transporter that implements
  `watchService(service): Observable<NodeRef[]>` + `listNodes(service): NodeRef[]` becomes
  an availability source. Core merges all of them (via `combineLatest`, de-duped by
  `node_id`) to answer `wait()` / `watch()` / `nodes`.
- `Registry.pickRpcNode(service, { filter? })` — optional routability predicate so a
  transport can exclude peers it can't reach yet (round-robin only over the filtered set).

### Migration
- **core:** drop the registry argument — `new SpiderMesh()`.
- **tcp:** inject one shared registry into all three transporters —
  `new UdpDiscovery(registry)`, `new Http2Rpc(registry)`, `new Http2Pubsub(registry)`.
- **ws:** nothing — the relay-backed transporter already provides `ServiceDirectory`.

### Notes
- `Registry` is still exported, now purely as a **helper for client-side-routing transports**.
- `ServiceDirectory.watchService` must emit current state promptly (BehaviorSubject
  semantics); core does **not** `startWith([])`, so a synthetic empty would be read as
  "no providers" and break availability gating.
