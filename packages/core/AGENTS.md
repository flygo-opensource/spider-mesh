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

- `tests/mock-e2e.test.ts` — single-process mock RPC, linking, local-node state, and availability
- `tests/process-e2e.test.ts` — RPC across isolated child processes
- `tests/fixtures/` — shared test services

## Conventions

- **ESM-only.** Source uses emitted `.js` relative specifiers (`moduleResolution: NodeNext`). Import `./Foo.js`, not `./Foo`.
- **`src/types.ts` is the RPC/Topology contract source of truth**, including `TopologyDiscovery`. The
  generic discovery envelope and `TopologyDiscoveryAdapter` live in `@spider-mesh/tcp` (the former
  `@spider-mesh/discovery` package was dropped); `@spider-mesh/ws` keeps a structural copy of the envelope
  types. Keep companion packages aligned in the same change.
- **Keep this package runtime-agnostic.** No sockets, UDP, HTTP, or transport-specific code in `core`. Concrete transports belong in companion packages.
- **Transporter tự hardcode `readonly name`** và được truyền vào `new SpiderMesh({ transporters })`.
  Không suy luận bằng tên class và không có availability registration riêng.
- **Topology là optional**; Topology nhận Discovery trong constructor, còn routing config thuộc request.
- **Service identity is the class name.** Treat renames as breaking; avoid build steps that mangle class names.
- **`LOCAL_SERVICES$` is process-global** and not re-exported — do not rely on multiple meshes per process being isolated. See [ARCHITECTURE.md](ARCHITECTURE.md#invariants--gotchas).

## Where things live

| Concern | File |
| --- | --- |
| Runtime lifecycle, RPC routing, metadata bridge | `src/SpiderMesh.ts` |
| Node / topic / RPC-routing state | `src/Topology.ts` (`Registry.ts` chỉ là alias cũ) |
| Remote proxy + linker | `src/RemoteService.ts` |
| Contracts, packets, node shape, error codes | `src/types.ts` |
| Local-service registration | `src/decorators/Microservice.ts` |
| Stateless runtime utilities and guards | `src/helpers/` |
| Public API surface | `src/index.ts` |

For the full module map and data flow, see [ARCHITECTURE.md](ARCHITECTURE.md).
