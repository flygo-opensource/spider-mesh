# Changelog

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
