# Changelog

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
