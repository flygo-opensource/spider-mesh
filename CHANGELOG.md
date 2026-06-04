# Changelog

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
