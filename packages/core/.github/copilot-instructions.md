# Spider Mesh Core Copilot Instructions

Use these instructions when editing or generating code in this repository.

## Core Rules

- Treat `src/types.ts` as the source of truth for contracts.
- Treat `src/SpiderMesh.ts`, `src/Registry.ts`, and `src/RemoteService.ts` as the runtime source of truth.
- Prefer the behavior already covered by `tests/mock-e2e.test.ts` and `tests/process-e2e.test.ts`.

## Current Public API

- Create runtimes with `new SpiderMesh(registry?)`.
- Register transporters with `mesh.registerTransporter(instance, name?)`.
- Link remote services with `RemoteServiceLinker.link(mesh, options)`.
- `RpcOptions.transporter` accepts either a string or an object/class with a `name`.

## Runtime Semantics

- `Registry` stores remote peers and RPC routing state only.
- `SpiderMesh` stores local runtime state and registered transporter instances.
- Local services are emitted through the shared `LOCAL_SERVICES$` stream.
- `linkEvent()` adds local topics on first subscribe and removes them on last unsubscribe.
- Discovery transporters rebroadcast local node metadata whenever it changes.

## Transporters

Infer transporter kind by shape:

- `send()` => RPC transporter
- `publish()` => pubsub transporter
- `broadcast()` => discovery transporter

Always register transporter instances. Do not register classes.

`Registry` exposes peer and routing queries including `listTopicNodes(topic)`.

## Documentation Guidance

- When updating docs, align examples with the current API in `src/SpiderMesh.ts`.
- If you mention companion packages, make clear that they are concrete transport implementations outside this package.
- Keep examples ESM-compatible and use emitted `.js` specifiers in repository source.

## Testing Guidance

- Prefer mock e2e tests over broad unit tests for runtime behavior.
- If you change RPC/discovery/pubsub behavior, update or add tests under `tests/`.
- Validate with `bun run test:e2e` and `bun run build`.