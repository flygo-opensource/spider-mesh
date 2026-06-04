# Changelog

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
