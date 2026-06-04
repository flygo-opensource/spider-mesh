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
- **`src/types`/contracts come from `@spider-mesh/core`** — keep both packages on matching versions.
- **Keep binary frames on `@msgpack/msgpack`.** Don't switch encoders without updating both transporter and relay.
- **Preserve `status$` semantics** (per-URL connection state) and the **delayed-unsubscribe** behavior for pubsub listeners.
- **The relay runs on a server runtime only** (`ws` library). Browser/RN code paths live in `GlobalWebsocketTransporter`; keep `browser.ts` and `react-native.ts` in sync (currently identical).
- **Tests bind `port: 0`** so the OS assigns a free port (relay uses `options.port ?? 8787`).
- The relay **rejects RPC/cancel frames from non-server connections** (`isServerConnection === false`); preserve that gate.

## Where things live

| Concern | File |
| --- | --- |
| Shared transporter (all 3 contracts, connection mgmt) | `src/BaseWebsocketTransporter.ts` |
| Node/Bun socket backend (`ws`) | `src/WebsocketTransporter.ts` |
| Browser/RN socket backend (`globalThis.WebSocket`) | `src/GlobalWebsocketTransporter.ts` |
| Relay routing + discovery | `src/WebsocketRelayServer.ts` |
| Frame definitions + msgpack | `src/websocketProtocol.ts` |
| Entry points | `src/{node,browser,react-native,relay-server}.ts` |

## Known issues to keep in mind

- `browser.ts` and `react-native.ts` are byte-identical — if they diverge, split `GlobalWebsocketTransporter`.
- `MICROSERVICE_OFFLINE` is produced by the **relay** as an async RPC response, not thrown by the transporter — don't relocate it without checking call sites.
- `RelayHelloFrame.target_id` is currently an unused field.

For the full module map, topology, and protocol detail, see [ARCHITECTURE.md](ARCHITECTURE.md).
