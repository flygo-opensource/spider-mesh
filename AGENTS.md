# @spider-mesh/core — Agent / Contributor Guide

How to build, test, and work inside this package. This file is the operating
guide for anyone (human or agent) **editing** `@spider-mesh/core`.

- **Using the package?** → [README.md](README.md)
- **Understanding the internals?** → [ARCHITECTURE.md](ARCHITECTURE.md)

## Build & Test

```bash
bun run build        # rm -rf build && tsc -b
bun run test:e2e     # bun test tests/*.test.ts
```

Behavior references when changing runtime/routing:

- `tests/mock-e2e.test.ts` — single-process mock RPC / linking / events
- `tests/process-e2e.test.ts` — RPC across isolated child processes
- `tests/fixtures/` — shared test services

## Conventions

- **ESM-only.** Source uses emitted `.js` relative specifiers (`moduleResolution: NodeNext`). Import `./Foo.js`, not `./Foo`.
- **`src/types.ts` is the transporter contract source of truth.** Change contracts there; keep companion packages (`@spider-mesh/tcp`, `@spider-mesh/ws`) aligned in the same change.
- **Keep this package runtime-agnostic.** No sockets, UDP, HTTP, or transport-specific code in `core`. Concrete transports belong in companion packages.
- **Register transporter instances, never classes** (`mesh.registerTransporter(new T())`). Capability is inferred by instance shape (`send`/`publish`/`broadcast`).
- **Service & event identity is the class name.** Treat renames as breaking; avoid build steps that mangle class names.
- **`LOCAL_SERVICES$` is process-global** and not re-exported — do not rely on multiple meshes per process being isolated. See [ARCHITECTURE.md](ARCHITECTURE.md#invariants--gotchas).

## Where things live

| Concern | File |
| --- | --- |
| Runtime lifecycle, RPC routing, events | `src/SpiderMesh.ts` |
| Peer / topic / RPC-routing state | `src/Registry.ts` |
| Remote proxy + linker | `src/RemoteService.ts` |
| Contracts, packets, node shape, error codes | `src/types.ts` |
| Local-service registration | `src/decorators/Microservice.ts` |
| Public API surface | `src/index.ts` |

For the full module map and data flow, see [ARCHITECTURE.md](ARCHITECTURE.md).
