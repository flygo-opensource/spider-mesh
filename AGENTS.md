# @spider-mesh/ws — Agent / Contributor Guide

How to build, test, and work inside this package. This file is the operating
guide for anyone (human or agent) **editing** `@spider-mesh/ws`.

- **Using the package?** → [README.md](README.md)
- **Understanding the internals?** → [ARCHITECTURE.md](ARCHITECTURE.md)

## Build & Test

```bash
bun run build        # tsc -b
bun run test:e2e     # runs the tests/*.e2e suites in sequence
```

There is **no bare `test` script**. Narrower commands:

```bash
bun run test:websocket             # smoke runner (examples/)
bun run test:websocket:e2e         # full e2e runner (examples/)
bun run test:websocket:e2e:matrix  # sync/async/observable/error return paths
bun run test:websocket:e2e:reverse # reverse RPC

# manual three-process run:
bun run example:websocket:server
bun run example:websocket:provider
bun run example:websocket:client
```

Individual e2e files (use the narrowest that matches your change):

```bash
bun test tests/websocket-transporter.e2e.test.ts
bun test tests/websocket-spidermesh.e2e.test.ts
bun test tests/websocket-spidermesh-reverse.e2e.test.ts
bun test tests/websocket-spidermesh-matrix.e2e.test.ts
bun test tests/websocket-spidermesh-round-robin.e2e.test.ts
```

## Conventions

- **ESM-only**, `.js` relative specifiers in TypeScript source.
- **No root export** — four subpaths only: `./node`, `./browser`, `./react-native`, `./relay-server`. Always edit/import via a subpath.
- **RPC/node contracts come from `@spider-mesh/core`; discovery contracts come from
  `@spider-mesh/discovery`.** Keep the packages on matching major versions.
- **Keep binary frames on `@msgpack/msgpack`.** Don't switch encoders without updating both transporter and relay.
- **Preserve `status$` semantics** (per-URL connection state) and the **delayed-unsubscribe** behavior for event listeners.
- Transporter có wire name `websocket`; cùng instance chỉ bind Topology Discovery khi ứng dụng cần
  enumerate/watch node. Không có Topology thì relay tự route và transporter cung cấp probe.
- **The relay runs on a server runtime only** (`ws` library). Browser/RN code paths live in `GlobalWebsocketTransporter`; keep `browser.ts` and `react-native.ts` in sync (currently identical).
- **Tests bind `port: 0`** so the OS assigns a free port (relay uses `options.port ?? 8787`).
- The relay **rejects RPC/cancel frames from non-server connections** (`isServerConnection === false`); preserve that gate.

## Where things live

| Concern | File |
| --- | --- |
| Shared RPC/discovery/event-compatible transporter and connection mgmt | `src/BaseWebsocketTransporter.ts` |
| Node/Bun socket backend (`ws`) | `src/WebsocketTransporter.ts` |
| Browser/RN socket backend (`globalThis.WebSocket`) | `src/GlobalWebsocketTransporter.ts` |
| Relay routing + discovery | `src/WebsocketRelayServer.ts` |
| Frame definitions + msgpack | `src/websocketProtocol.ts` |
| Entry points | `src/{node,browser,react-native,relay-server}.ts` |

## Known issues to keep in mind

- `browser.ts` and `react-native.ts` are byte-identical — if they diverge, split `GlobalWebsocketTransporter`.
- `MICROSERVICE_OFFLINE` is produced by the **relay** as an async RPC response, not thrown by the transporter — don't relocate it without checking call sites. Core also raises it locally for in-flight RPCs when an `offline` event names the node that was serving them; the two layers are deliberately redundant (see below).
- `RelayHelloFrame.target_id` is currently an unused field.
- The relay owns `#pendingRequests` as a `{ caller, provider, service }` pair, and `on_close` **must** close both directions: a dead provider sends `MICROSERVICE_OFFLINE` back to its caller, and a dead caller sends `cancel` to its provider. For round-robin requests the relay is the only party that knows the pair, so don't reduce this map back to a bare socket.

For the full module map, topology, and protocol detail, see [ARCHITECTURE.md](ARCHITECTURE.md).
