# @spider-mesh/tcp — Agent / Contributor Guide

How to build, test, and work inside this package. This file is the operating
guide for anyone (human or agent) **editing** `@spider-mesh/tcp`.

- **Using the package?** → [README.md](README.md)
- **Understanding the internals?** → [ARCHITECTURE.md](ARCHITECTURE.md)

## Build & Test

```bash
bun run build        # tsc -b
bun run test:e2e     # runs the tests/*.e2e suites in sequence
bun run test:resilience # fault, restart, soak, duplicate-id, interrupted-stream coverage
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
bun test tests/tcp-session-recovery.e2e.test.ts
bun test tests/tcp-resilience.e2e.test.ts
```

## Conventions

- **ESM-only**, `.js` relative specifiers in TypeScript source.
- **Node/Bun only** — this package uses HTTP/2; never add browser/RN code here (that is `@spider-mesh/ws`).
- **`src/types.ts` re-exports core RPC contracts.** `src/TopologyDiscoveryAdapter.ts` owns the
  discovery envelope types and the adapter that plugs a discovery (e.g. `@ohayo/udp`) into Topology;
  it was moved here from `@spider-mesh/discovery`, which is not published. Keep the envelope
  structurally identical to `@ohayo/udp`'s `DiscoveryMessage`.
- **`Http2Rpc.name` luôn là `http2`.** Endpoint metadata chỉ đọc/ghi bằng wire name này.
- **Topology thuộc Core và optional trên SpiderMesh.** `Http2Rpc` nhận nó qua lifecycle; Discovery là
  nguồn membership duy nhất, còn transporter chỉ giữ connection reachability.
- Hạ tầng tự route thì dùng `resolveService` và không cần Topology.
- **`examples/` and `tests/` are excluded from the package build.** For matrix tests, use the linked core RxJS copy when observable identity matters (`examples/helpers/coreRxjs.ts`).
- `msgpackr` is the RPC/pubsub encoder.

## Where things live

| Concern | File |
| --- | --- |
| HTTP/2 RPC | `src/Http2Rpc.ts` |
| HTTP/2 events (registered on `EventBus`) | `src/Http2Pubsub.ts` |
| Env config constants | `src/const.ts` |
| Core contract re-exports | `src/types.ts` |

## Known issues to keep in mind

- Core gọi `start/stop`; API `unsubscribe()` của `Http2Rpc` và `close()` của `Http2Pubsub` vẫn tồn tại
  cho standalone lifecycle.

For the full module map, data flow, and gotchas, see [ARCHITECTURE.md](ARCHITECTURE.md).
