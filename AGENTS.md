# Spider Mesh WS Agent Guide

Use this file as the canonical implementation guide for `@spider-mesh/ws`.

## Package Purpose

`@spider-mesh/ws` provides:

- `WebsocketTransporter` for application nodes
- `WebsocketRelayServer` for relay processes

Use `@spider-mesh/core` for runtime creation, decorators, and remote linking.

## Canonical Imports

```ts
import { Registry, SpiderMesh, RemoteServiceLinker, Microservice } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'
```

Browser and React Native use their runtime-specific transporter subpaths.

## Canonical Runtime Setup

```ts
const transporter = new WebsocketTransporter({
  heartbeatIntervalMs: 5000,
  reconnectIntervalMs: 1000,
})

transporter.connect('ws://127.0.0.1:8787')

const registry = new Registry()
const mesh = new SpiderMesh(registry)

mesh.registerTransporter(transporter)
```

`SpiderMesh.registerTransporter()` links all supported capabilities on the shared WebSocket transporter instance.

## Runtime Responsibilities

### `WebsocketTransporter`

- maintains relay connections
- exposes `status$`
- sends and receives binary MsgPack frames
- serves RPC, discovery, and pubsub through one shared transporter instance

### `WebsocketRelayServer`

- tracks connected nodes from `hello` frames
- routes targeted RPC and cancel frames
- synchronizes discovery state
- tracks topic listeners and forwards pubsub payloads
- emits offline events on disconnect

## Runtime Support

- Node.js: `@spider-mesh/ws/node` and `@spider-mesh/ws/relay-server`
- Bun: `@spider-mesh/ws/node` and `@spider-mesh/ws/relay-server`
- Browser: `@spider-mesh/ws/browser`
- React Native: `@spider-mesh/ws/react-native`

The package root is not exported. Use subpath imports.

## Source Of Truth

Prefer these files:

- `src/BaseWebsocketTransporter.ts`
- `src/WebsocketTransporter.ts`
- `src/GlobalWebsocketTransporter.ts`
- `src/WebsocketRelayServer.ts`
- `src/websocketProtocol.ts`

For behavior references, prefer:

- `examples/websocket-smoke-test.ts`
- `examples/websocket-e2e-test.ts`
- `examples/websocket-e2e-reverse-test.ts`
- `examples/websocket-e2e-matrix-test.ts`
- `examples/websocket-e2e-round-robin-test.ts`

## Wire Protocol

All frames are MsgPack-encoded `RelayFrame` objects (source of truth: `src/websocketProtocol.ts`).

Frame types:

| `type`        | Direction            | Purpose |
|---------------|----------------------|---------|
| `hello`       | node → relay → nodes | node discovery |
| `offline`     | relay → nodes        | node disconnect |
| `rpc`         | node ↔ relay ↔ node  | request / response / cancel |
| `publish`     | node → relay → nodes | pubsub payload |
| `subscribe`   | node → relay         | topic subscription |
| `unsubscribe` | node → relay         | topic unsubscription |

`sender_id` is **not** included in any wire frame. The relay resolves the sender from the socket state.

### RPC frame shape

```ts
type RelayRpcFrame = {
  type: 'rpc'
  data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket
}
```

The full packet (including `kind`, `request_id`, `service`, `destination_node_id`) lives inside `data`. No top-level routing fields are duplicated.

### Cancel routing

The relay maintains `#pendingRequests: Map<string, WebSocket>` — a map from `request_id` to the provider socket currently handling that request. When a cancel frame arrives the relay looks up the provider socket and forwards the cancel directly. When the provider responds with `completed: true` or `error` the entry is removed.

The `cancel()` function returned by `send()` reuses the same relay socket that sent the original request; no `destination_node_id` is needed.

### Discovery deduplication

`BaseWebsocketTransporter.on_hello` only emits a `discovered` event when the node is genuinely new or its service list has changed. Re-sends caused by relay sync (`#syncServerConnections`) are silently absorbed. A node never emits a `discovered` event for itself.

## Important Notes

- Keep binary frame encoding with `@msgpack/msgpack`.
- Preserve `status$` behavior for connection states.
- Preserve delayed unsubscribe behavior for pubsub listeners.
- Relay logic rejects request and cancel frames from non-server connections (`isServerConnection === false`).
- Use `port: 0` in tests so the OS assigns a free port; the relay constructor uses `options.port ?? 8787`.

## Validation

Use the narrowest check that matches the change:

- `bun run build`
- `bun test tests/websocket-transporter.e2e.test.ts`
- `bun test tests/websocket-spidermesh.e2e.test.ts`
- `bun test tests/websocket-spidermesh-reverse.e2e.test.ts`
- `bun test tests/websocket-spidermesh-matrix.e2e.test.ts`
- `bun test tests/websocket-spidermesh-round-robin.e2e.test.ts`
- `bun test tests/node-online-spam.e2e.test.ts`
- `bun run test:e2e`