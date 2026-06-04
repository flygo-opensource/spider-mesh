# @spider-mesh/tcp — Agent / Contributor Guide

How to build, test, and work inside this package. This file is the operating
guide for anyone (human or agent) **editing** `@spider-mesh/tcp`.

- **Using the package?** → [README.md](README.md)
- **Understanding the internals?** → [ARCHITECTURE.md](ARCHITECTURE.md)

## Build & Test

```bash
bun run build        # tsc -b
bun run test:e2e     # runs the tests/*.e2e suites in sequence
```

There is **no bare `test` script**. Narrower commands:

```bash
bun run test:tcp                 # smoke runner (examples/)
bun run test:tcp:e2e             # full e2e runner (examples/)
bun run test:tcp:e2e:matrix      # sync/async/observable/error return paths
bun run test:tcp:e2e:reverse     # reverse RPC
```

Individual e2e files (use the narrowest that matches your change):

```bash
bun test tests/tcp-transporters.e2e.test.ts
bun test tests/tcp-contracts.e2e.test.ts
bun test tests/tcp-spidermesh.e2e.test.ts
bun test tests/tcp-spidermesh-reverse.e2e.test.ts
bun test tests/tcp-spidermesh-matrix.e2e.test.ts
bun test tests/tcp-spidermesh-round-robin.e2e.test.ts
```

## Conventions

- **ESM-only**, `.js` relative specifiers in TypeScript source.
- **Node/Bun only** — this package uses raw UDP and HTTP/2; never add browser/RN code here (that is `@spider-mesh/ws`).
- **`src/types.ts` re-exports core contracts** — `@spider-mesh/core` is the source of truth. Keep both packages on matching versions.
- **All network config is env-backed constants in `src/const.ts`** (ports/multicast). The only constructor argument is the shared `Registry` (`new UdpDiscovery(registry)` etc.) — construct one and inject it into all three so they share routing state. Do not add other per-instance options without discussing the structural impact (see [ARCHITECTURE.md](ARCHITECTURE.md#configuration-model)).
- **`examples/` and `tests/` are excluded from the package build.** For matrix tests, use the linked core RxJS copy when observable identity matters (`examples/helpers/coreRxjs.ts`).
- `msgpackr` is the encoder for all three transports.

## Where things live

| Concern | File |
| --- | --- |
| Multicast discovery | `src/UdpDiscovery.ts` |
| HTTP/2 RPC | `src/Http2Rpc.ts` |
| HTTP/2 pub/sub | `src/Http2Pubsub.ts` |
| Env config constants | `src/const.ts` |
| Core contract re-exports | `src/types.ts` |

## Known issues to keep in mind

- `SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE` is **dead code** (read, never used). Don't document it as working.
- Teardown is **not uniform**: `UdpDiscovery`/`Http2Rpc` use `unsubscribe()`, `Http2Pubsub` uses `close()`.

For the full module map, data flow, and gotchas, see [ARCHITECTURE.md](ARCHITECTURE.md).
