# AGENTS.md

This file is for AI coding agents working in this repository.

## Repo Purpose

`@spider-mesh/tcp` provides three transporters for `@spider-mesh/core`:

- `UdpDiscovery`: UDP multicast discovery.
- `Http2Rpc`: HTTP/2 RPC transporter.
- `Http2Pubsub`: HTTP/2 pubsub transporter.

The codebase is small and transport-focused. Prefer minimal, local changes.

## Tooling

- Package manager: `bun`.
- Module system: ESM only.
- TypeScript mode: `module: NodeNext`, `moduleResolution: NodeNext`, strict mode on.
- Build command: `bun run build`.
- Full e2e suite: `bun run test:e2e`.

Prefer `bun` commands over `npm` unless the user explicitly asks otherwise.

## Important Constraints

- Do not reintroduce `@spider-mesh/types`.
- Keep internal imports ESM-safe with explicit `.js` extensions in TypeScript source.
- `examples/` and `tests/` are excluded from the package build on purpose.
- `src/types.ts` re-exports transporter contract types from `@spider-mesh/core`; core is the source of truth for those contracts.
- `examples/` has its own `tsconfig.json` for editor/typecheck support. Do not pull example scripts into the root package build just to satisfy editor diagnostics.
- In example scripts, prefer explicit Node imports such as `import process from 'node:process'` over relying on ambient globals.

## Transporter Semantics

### `UdpDiscovery`

- Discovery is best-effort and multicast-based.
- Core may call `broadcast(...)` with only interface IPs.
- Always preserve multicast delivery; do not narrow sending to only the passed IP list.
- Discovery metadata is the source of remote node addresses and transporter ports.

### `Http2Rpc`

- RPC is packet-based, not request-shape based.
- The transporter contract is `Observable<RpcEvent> & { send(packet, node): Promise<void> }`.
- Keep request/response/cancel packet handling aligned with `@spider-mesh/core` runtime behavior.
- Never overwrite remote node transporter metadata with local transporter metadata.
- Response packets are sent back on the original HTTP/2 request stream; do not reintroduce reverse-dial reply routing.
- RPC events emitted here carry `node_id`, not a fabricated `SpiderMeshNode`.

### `Http2Pubsub`

- Current core event linking does not advertise topic subscriptions into `node.topics` metadata.
- Publish to known nodes that expose the pubsub transporter instead of relying on per-topic discovery metadata.
- Use HTTP/2 compat request handling carefully; `req.url` is safer than reading `:path` directly in the server request callback.

## Linked Core Gotcha

When `@spider-mesh/core` is linked locally, `rxjs` may exist in both repos. That can break `instanceof Observable` checks inside core.

For e2e scenarios that must return real `Observable` instances recognized by core, use the linked core's `rxjs` copy. The current matrix example does this through `examples/helpers/coreRxjs.ts`.

If matrix-style tests suddenly fail on observable-returning methods while promises still work, check RxJS identity first.

## Where To Start

- Transport implementation bugs: inspect `src/UdpDiscovery.ts`, `src/Http2Rpc.ts`, `src/Http2Pubsub.ts`, and `src/runtime.ts`.
- Contract/type drift with core: inspect `src/types.ts` and compare with the linked `@spider-mesh/core` runtime.
- Example/e2e failures: inspect `examples/` first, then `tests/` wrappers.

## Validation Strategy

Use the narrowest command that can falsify the change:

- Build/type changes: `bun run build`
- Smoke transporter behavior: `bun test tests/tcp-transporters.e2e.test.ts`
- RPC scenario: `bun test tests/tcp-spidermesh.e2e.test.ts`
- Reverse RPC: `bun test tests/tcp-spidermesh-reverse.e2e.test.ts`
- Matrix behavior: `bun test tests/tcp-spidermesh-matrix.e2e.test.ts`
- Round-robin behavior: `bun test tests/tcp-spidermesh-round-robin.e2e.test.ts`

Run the full suite only after the narrow failing slice passes.

## Editing Guidance

- Prefer fixing the runtime cause over weakening tests.
- Keep transport behavior explicit and easy to trace.
- Avoid broad refactors unless the user asks for them.
- If you add new examples/tests, keep them aligned with the scenario structure used by `@spider-mesh/core`.