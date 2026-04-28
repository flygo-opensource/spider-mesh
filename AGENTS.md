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

### Built-in WebSocket transporter

Import from the WebSocket subpath:

```ts
import { WebsocketTransporter } from '@spider-mesh/core/websocket'
```

### Built-in relay server

Import from the relay-server subpath:

```ts
import { WebsocketRelayServer } from '@spider-mesh/core/relay-server'
```

## Import Rules

- Do not import `WebsocketTransporter` from `@spider-mesh/core`.
- Do not import `WebsocketRelayServer` from `@spider-mesh/core`.
- Use `@spider-mesh/core` for runtime-agnostic APIs only.
- Use `@spider-mesh/core/websocket` only when the target runtime is Node.js or Bun.
- Use `@spider-mesh/core/relay-server` only for a dedicated relay process on Node.js or Bun.

## Runtime Rules

### Node.js

Supported:

- `@spider-mesh/core`
- `@spider-mesh/core/websocket`
- `@spider-mesh/core/relay-server`

### Bun

Supported:

- `@spider-mesh/core`
- `@spider-mesh/core/websocket`
- `@spider-mesh/core/relay-server`

### React Native

Use:

- `@spider-mesh/core`

Do not assume support for:

- `@spider-mesh/core/websocket`
- `@spider-mesh/core/relay-server`

If the runtime is React Native, prefer core APIs plus a runtime-appropriate custom transporter.

## Canonical Usage Patterns

### Provider

1. Import `Microservice` and `SpiderMesh` from `@spider-mesh/core`.
2. Import `WebsocketTransporter` from `@spider-mesh/core/websocket`.
3. Decorate the local service class with `@Microservice()`.
4. Instantiate the service class.
5. Create `new SpiderMesh({ transporters: [transporter] })`.

### Client

1. Import `SpiderMesh` and `RemoteServiceLinker` from `@spider-mesh/core`.
2. Import `WebsocketTransporter` from `@spider-mesh/core/websocket`.
3. Create `new SpiderMesh({ transporters: [transporter] })`.
4. Create a typed proxy with `RemoteServiceLinker.link()`.
5. Call `await proxy.wait()` before the first remote call.

### Relay Server

1. Import `WebsocketRelayServer` from `@spider-mesh/core/relay-server`.
2. Run it in a dedicated Node.js or Bun process.
3. Connect provider and client nodes to that relay URL.

## Startup Order

When using the built-in WebSocket transport, prefer this order:

1. Start the relay server.
2. Start provider processes.
3. Start client processes.
4. In clients, wait for service discovery before the first call.

## API Map

- `SpiderMesh`: runtime coordinator
- `RemoteServiceLinker.link()`: typed remote proxy
- `@Microservice()`: expose a local class instance as a remote service
- `SpiderMesh.linkEvent()`: event publish and subscribe binding
- `WebsocketTransporter`: built-in Node/Bun transport
- `WebsocketRelayServer`: built-in Node/Bun relay

## Behavioral Notes

- Remote calls are stream-first.
- Remote methods may return observables, promises, or plain values.
- RPC target selection is round-robin unless `node_id` or `ip` is forced.
- Local services become available after class instantiation and any `@BeforeMicroserviceOnline()` hooks complete.

## Examples To Prefer

When generating code, prefer these repository examples as canonical references:

- `examples/websocket-provider.ts`
- `examples/websocket-client.ts`
- `examples/websocket-server.ts`
- `examples/websocket-e2e-test.ts`
- `examples/websocket-e2e-round-robin-test.ts`

## Do Not Infer

- Do not assume the root package exports every transporter.
- Do not assume relay-server code is safe for browser or React Native runtimes.
- Do not assume WebSocket support is the only valid transport strategy.
- Do not invent helper APIs that are not present in this repository.

## Preferred Agent Behavior

If you are unsure which import to use:

1. Default to `@spider-mesh/core`.
2. Add `@spider-mesh/core/websocket` only when the code explicitly needs the built-in WebSocket transport on Node.js or Bun.
3. Add `@spider-mesh/core/relay-server` only when creating a dedicated relay process.
