# Spider Mesh WS Copilot Instructions

Use these instructions when editing or generating code in this repository.

## Current API

- Import the transporter from a runtime subpath such as `@spider-mesh/ws/node`.
- Create one `WebsocketTransporter`, call `connect(url)`, then register that instance with `mesh.registerTransporter(transporter)`.
- Use `WebsocketRelayServer` from `@spider-mesh/ws/relay-server` for relay processes.

## Runtime Semantics

- `WebsocketTransporter` owns a shared WebSocket client state.
- `SpiderMesh.registerTransporter()` links RPC, discovery, and pubsub behavior from the same transporter instance.
- `status$` tracks relay connection status per URL.
- Relay frames stay binary and MsgPack-encoded.

## Source Of Truth

- `src/BaseWebsocketTransporter.ts`
- `src/WebsocketTransporter.ts`
- `src/GlobalWebsocketTransporter.ts`
- `src/WebsocketRelayServer.ts`
- `src/websocketProtocol.ts`

## Testing Guidance

- Validate with `bun run build`.
- Prefer the e2e tests under `tests/` for runtime behavior.
- Run `bun run test:e2e` when changing websocket transport or relay behavior.