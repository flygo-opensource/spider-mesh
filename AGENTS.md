# Spider Mesh TCP Agent Guide

Use this file as the canonical implementation guide for `@spider-mesh/tcp`.

## Package Purpose

`@spider-mesh/tcp` provides three separate transporters for `@spider-mesh/core`:

- `UdpDiscovery`
- `Http2Rpc`
- `Http2Pubsub`

These transporters are registered individually on `SpiderMesh`.

## Canonical Runtime Setup

```ts
import { Registry, SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

const registry = new Registry()
const mesh = new SpiderMesh(registry)

mesh.registerTransporter(new UdpDiscovery())
mesh.registerTransporter(new Http2Rpc())
mesh.registerTransporter(new Http2Pubsub())
```

`Registry` is part of the expected TCP runtime because RPC and pubsub routing use peer and topic lookups.

## Transporter Contracts

### `UdpDiscovery`

- observable discovery transporter
- broadcasts full node metadata
- preserves multicast fan-out
- normalizes remote `host` from packet source address

### `Http2Rpc`

- observable RPC transporter
- exposes endpoint metadata for local HTTP/2 port
- resolves remote targets from `Registry.getPeer(node_id)`
- current send signature is `send(packet, node_id?)`

### `Http2Pubsub`

- pubsub transporter for `publish()` and `listen()`
- resolves remote subscribers from `Registry.listTopicNodes(topic)` when linked

## Source Of Truth

Prefer these files:

- `src/UdpDiscovery.ts`
- `src/Http2Rpc.ts`
- `src/Http2Pubsub.ts`
- `src/runtime.ts`
- `src/types.ts`

For behavior references, prefer:

- `examples/tcp-smoke-test.ts`
- `examples/tcp-e2e-test.ts`
- `examples/tcp-e2e-reverse-test.ts`
- `examples/tcp-e2e-matrix-test.ts`
- `examples/tcp-e2e-round-robin-test.ts`

## Important Notes

- `src/types.ts` re-exports transporter contracts from `@spider-mesh/core`.
- Keep `@spider-mesh/tcp` and `@spider-mesh/core` on matching versions.
- Keep ESM `.js` specifiers in TypeScript source.
- `examples/` and `tests/` are intentionally excluded from the package build.
- For matrix tests, use the linked core RxJS copy when observable identity matters.

## Validation

Use the narrowest check that matches the change:

- `bun run build`
- `bun test tests/tcp-transporters.e2e.test.ts`
- `bun test tests/tcp-contracts.e2e.test.ts`
- `bun test tests/tcp-spidermesh.e2e.test.ts`
- `bun test tests/tcp-spidermesh-reverse.e2e.test.ts`
- `bun test tests/tcp-spidermesh-matrix.e2e.test.ts`
- `bun test tests/tcp-spidermesh-round-robin.e2e.test.ts`
- `bun run test:e2e`