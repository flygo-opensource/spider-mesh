# Spider Mesh Core Agent Guide

Use this file as the canonical implementation guide when generating code for this repository or when consuming this package in another codebase.

## Purpose

`@spider-mesh/core` is a microservice runtime for:

- remote procedure calls between services
- node discovery
- pubsub events
- transporter-driven communication

Prefer the APIs and imports described here over inference from symbol names alone.

## Canonical Imports

Use these imports exactly.

### Runtime-agnostic core APIs

Import from the root package:

```ts
import {
  SpiderMesh,
  RemoteServiceLinker,
  Microservice,
  BeforeMicroserviceOnline,
  NestJSExposeMicroservice,
  NestJSLinkMicroservice,
  NestJSLinkEvent,
} from '@spider-mesh/core'
```

### Transporter contracts from core

If you are implementing or typing against transporter contracts, use the core package:

```ts
import type {
  DiscoveryTransporter,
  PubsubTransporter,
  RpcTransporter,
} from '@spider-mesh/core'
```

### Companion transport packages

If you need a companion transport package, use the package that matches your runtime and transport choice.

TCP package:

```ts
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'
```

WebSocket package:

```ts
import { WebsocketTransporter } from '@spider-mesh/ws'
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'
```

## Import Rules

- Use `@spider-mesh/core` for runtime-agnostic APIs only.
- Use `@spider-mesh/core` for shared transporter contracts and runtime creation.
- Use `@spider-mesh/tcp` or `@spider-mesh/ws` when you explicitly want a concrete companion transport package.
- Keep companion package versions aligned with the published `@spider-mesh/core` version when updating dependencies or release instructions.
- Do not assume the core root package exports every concrete transporter implementation.
- This package is ESM-only; when editing repository source, keep relative TypeScript imports on emitted `.js` specifiers.
- Do not introduce new CommonJS runtime code such as `require()`, `module.exports`, or `__dirname` unless the file is intentionally bridging runtimes.
- When documenting transporter contracts or packet shapes, treat `src/types.ts` as the source of truth.

## Runtime Rules

### Node.js

Supported:

- `@spider-mesh/core`
- `@spider-mesh/tcp`
- `@spider-mesh/ws`

### Bun

Supported:

- `@spider-mesh/core`
- `@spider-mesh/tcp`
- `@spider-mesh/ws`

### React Native

Use:

- `@spider-mesh/core`

Do not assume support for:

- `@spider-mesh/tcp`
- `@spider-mesh/ws`

If the runtime is React Native, prefer core APIs plus a runtime-appropriate custom transporter.

## Canonical Usage Patterns

### Provider

1. Import `Microservice` and `SpiderMesh` from `@spider-mesh/core`.
2. Choose a transporter implementation, for example from `@spider-mesh/tcp`, `@spider-mesh/ws`, or your own implementation of the core transporter contracts.
3. Decorate the local service class with `@Microservice()`.
4. Instantiate the service class.
5. Create `new SpiderMesh({ transporters: [transporter] })`.

### Client

1. Import `SpiderMesh` and `RemoteServiceLinker` from `@spider-mesh/core`.
2. Choose a transporter implementation, for example from `@spider-mesh/tcp`, `@spider-mesh/ws`, or your own implementation of the core transporter contracts.
3. Create `new SpiderMesh({ transporters: [transporter] })`.
4. Create a typed proxy with `RemoteServiceLinker.link()`.
5. Call `await proxy.wait()` before the first remote call.

### Companion Packages

TCP:

1. Import `UdpDiscovery`, `Http2Rpc`, and `Http2Pubsub` from `@spider-mesh/tcp`.
2. Add those transporters to `new SpiderMesh({ transporters: [...] })`.
3. Let discovery, RPC, and pubsub operate through the TCP package runtime pieces.

WebSocket:

1. Import `WebsocketTransporter` from `@spider-mesh/ws`.
2. Create `new SpiderMesh({ transporters: [transporter] })` in providers and clients.
3. Run `WebsocketRelayServer` from `@spider-mesh/ws/relay-server` as the relay process when using that transport.

## Startup Order

When using any discovery-based transport package, prefer this order:

1. Start provider processes.
2. Start client processes.
3. Wait for discovery to converge.
4. In clients, wait for service discovery before the first call.

## API Map

- `SpiderMesh`: runtime coordinator
- `RemoteServiceLinker.link()`: typed remote proxy
- `@Microservice()`: expose a local class instance as a remote service
- `SpiderMesh.linkEvent()`: event publish and subscribe binding
- `RpcTransporter`: RPC transporter contract
- `DiscoveryTransporter`: discovery transporter contract
- `PubsubTransporter`: pubsub transporter contract

## Behavioral Notes

- Remote calls are stream-first.
- Remote methods may return observables, promises, or plain values.
- RPC target selection is round-robin unless `node_id` or `ip` is forced.
- Local services become available after class instantiation and any `@BeforeMicroserviceOnline()` hooks complete.
- `randomUUID()` is expected to stay ESM-safe across Node.js, browser, and React Native runtimes.

## Examples To Prefer

When generating code, prefer these repository examples as canonical references:

- `../tcp/examples/tcp-smoke-test.ts`
- `../tcp/examples/tcp-e2e-test.ts`
- `../tcp/examples/tcp-e2e-reverse-test.ts`
- `../tcp/examples/tcp-e2e-matrix-test.ts`
- `../tcp/examples/tcp-e2e-round-robin-test.ts`
- `../ws/examples/websocket-smoke-test.ts`
- `../ws/examples/websocket-e2e-test.ts`
- `../ws/examples/websocket-e2e-reverse-test.ts`
- `../ws/examples/websocket-e2e-matrix-test.ts`
- `../ws/examples/websocket-e2e-round-robin-test.ts`

## Do Not Infer

- Do not assume the root package exports every transporter.
- Do not assume the TCP package is the only valid transport strategy.
- Do not assume the WebSocket package is the only valid transport strategy.
- Do not invent helper APIs that are not present in this repository.

## Preferred Agent Behavior

If you are unsure which import to use:

1. Default to `@spider-mesh/core`.
2. Use transporter contracts exported by `@spider-mesh/core` when the implementation package is not yet decided.
3. Add `@spider-mesh/tcp` or `@spider-mesh/ws` only when the code explicitly needs that concrete companion transport package.
