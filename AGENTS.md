# Spider Mesh WS Agent Guide

Use this file as the canonical implementation guide when generating code for `@spider-mesh/ws` or when consuming this package from another codebase.

## Purpose

`@spider-mesh/ws` provides the built-in WebSocket transport layer for Spider Mesh.

This package contains:

- `WebsocketTransporter` for application nodes
- `WebsocketRelayServer` for a dedicated relay process

This package does not replace `@spider-mesh/core`.

Use `@spider-mesh/core` for the runtime, service decorators, and remote service linking.

## Canonical Imports

### Core runtime APIs

Always import runtime-agnostic APIs from the core package:

```ts
import {
  SpiderMesh,
  RemoteServiceLinker,
  Microservice,
  BeforeMicroserviceOnline,
} from '@spider-mesh/core'
```

### WebSocket transporter

Import the transporter from the runtime-appropriate entry point:

```ts
import { WebsocketTransporter as NodeWebsocketTransporter } from '@spider-mesh/ws/node'
import { WebsocketTransporter as BrowserWebsocketTransporter } from '@spider-mesh/ws/browser'
import { WebsocketTransporter as ReactNativeWebsocketTransporter } from '@spider-mesh/ws/react-native'
```

### Relay server

Import the relay server from the relay subpath:

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'
```

## Import Rules

- Do not import `WebsocketTransporter` from `@spider-mesh/core`.
- Do not import anything from `@spider-mesh/ws` root.
- Use `@spider-mesh/ws/node` when you want an explicit Node.js or Bun transporter import.
- Use `@spider-mesh/ws/browser` for browser runtimes.
- Use `@spider-mesh/ws/react-native` for React Native runtimes that expose `globalThis.WebSocket`.
- Do not import `WebsocketRelayServer` from `@spider-mesh/core`.
- Do not import `WebsocketRelayServer` from `@spider-mesh/ws` root.
- Use `@spider-mesh/ws/relay-server` for relay processes only.
- Use `@spider-mesh/core` for service decorators, runtime creation, and remote linking.

## Runtime Rules

### Node.js

Supported:

- `@spider-mesh/core`
- `@spider-mesh/ws/node`
- `@spider-mesh/ws/relay-server`

### Bun

Supported:

- `@spider-mesh/core`
- `@spider-mesh/ws/node`
- `@spider-mesh/ws/relay-server`

### Browser

Supported:

- `@spider-mesh/core`
- `@spider-mesh/ws/browser`

Do not assume support for:

- `@spider-mesh/ws/relay-server`

### React Native

Use:

- `@spider-mesh/core`
- `@spider-mesh/ws/react-native`

Do not assume support for:

- `@spider-mesh/ws/relay-server`

## Package Responsibilities

`WebsocketTransporter` is responsible for:

- opening and maintaining WebSocket connections
- exposing `status$` connection state for each relay URL
- reconnect and heartbeat behavior
- forwarding RPC packets through relay frames
- forwarding discovery hello and offline events
- forwarding pubsub publish and subscription traffic

`WebsocketRelayServer` is responsible for:

- accepting WebSocket client connections
- tracking node identity from hello frames
- forwarding RPC and cancel frames to the target node
- synchronizing discovery state to server-role connections
- tracking topic listeners and forwarding pubsub messages
- broadcasting offline events when a node disconnects

## Canonical Usage Patterns

### Provider node

1. Import `Microservice` and `SpiderMesh` from `@spider-mesh/core`.
2. Import `WebsocketTransporter` from `@spider-mesh/ws`.
3. Create a transporter and call `connect(url)`.
4. Decorate the service class with `@Microservice()`.
5. Instantiate the service class.
6. Create `new SpiderMesh({ transporters: [transporter] })`.

### Client node

1. Import `SpiderMesh` and `RemoteServiceLinker` from `@spider-mesh/core`.
2. Import `WebsocketTransporter` from `@spider-mesh/ws`.
3. Create a transporter and call `connect(url)`.
4. Create `new SpiderMesh({ transporters: [transporter] })`.
5. Create a typed proxy with `RemoteServiceLinker.link()`.
6. Call `await proxy.wait()` before the first RPC call.

### Relay process

1. Import `WebsocketRelayServer` from `@spider-mesh/ws/relay-server`.
2. Run it in a dedicated Node.js or Bun process.
3. Do not mix relay responsibilities with application services unless that is explicitly intended.

## Startup Order

When using the built-in WebSocket transport, prefer this order:

1. Start the relay server.
2. Start provider processes.
3. Start client processes.
4. Wait for discovery before the first remote call.

## Behavior To Preserve

When editing this package, preserve these behaviors unless the task explicitly changes them:

- RPC request, response, and cancel frames are binary MsgPack payloads.
- `status$` remains a public `BehaviorSubject<Map<string, string>>` that reflects `connecting`, `connected`, `error`, and `not_connected` for each configured relay URL.
- Observable-returning RPC methods must work correctly across package boundaries.
- Discovery synchronization must emit `hello` and `offline` state consistently.
- Pubsub listeners are tracked by topic and unsubscribed with delay semantics.
- Relay logic must not permit client-role connections to send server-only RPC initiation frames unless explicitly designed.

## Examples To Prefer

When generating code, prefer these package examples as canonical references:

- `examples/websocket-server.ts`
- `examples/websocket-provider.ts`
- `examples/websocket-client.ts`
- `examples/websocket-smoke-test.ts`
- `examples/websocket-e2e-test.ts`
- `examples/websocket-e2e-matrix-test.ts`
- `examples/websocket-e2e-round-robin-test.ts`

## Do Not Infer

- Do not assume the root core package exports WebSocket classes.
- Do not assume the package root is available as an import target.
- Do not treat the relay as an application service host.
- Do not invent additional transport frame types without checking `src/websocketProtocol.ts`.
- Do not replace binary frame encoding with JSON unless the task explicitly changes protocol semantics.

## Preferred Agent Behavior

If you are unsure where a feature belongs:

1. Put service/runtime concerns in `@spider-mesh/core`.
2. Put WebSocket transport concerns in `@spider-mesh/ws`.
3. Put relay-only process concerns in `@spider-mesh/ws/relay-server`.
4. Preserve the separation between runtime-agnostic APIs and Node/Bun-specific transport code.