# @spider-mesh/tcp

`@spider-mesh/tcp` provides HTTP/2 and UDP transporters for `@spider-mesh/core`.

It packages three runtime pieces:

- `UdpDiscovery`: multicast-based node discovery.
- `Http2Rpc`: point-to-point RPC transport over HTTP/2.
- `Http2Pubsub`: topic-based publish/subscribe transport over HTTP/2.

The package targets the current transporter runtime contract used by `@spider-mesh/core`.

The repository is ESM-only and built with TypeScript `moduleResolution: NodeNext`.

## Installation

```bash
bun add @spider-mesh/core @spider-mesh/tcp
```

or

```bash
npm install @spider-mesh/core @spider-mesh/tcp
```

Keep `@spider-mesh/tcp` on the same published version as `@spider-mesh/core` so the transporter contracts and runtime stay aligned.

## Exports

```ts
import { Http2Rpc, Http2Pubsub, UdpDiscovery } from '@spider-mesh/tcp'
```

## Compatibility Notes

- Discovery uses UDP multicast and remains best-effort.
- RPC and pubsub use HTTP/2 over TCP.
- This package no longer depends on `@spider-mesh/types`.
- `src/types.ts` re-exports transporter contract types directly from `@spider-mesh/core`, so core remains the source of truth for RPC, discovery, and pubsub typing.
- When `@spider-mesh/core` is linked locally, `Observable` identity can differ across package boundaries. The matrix e2e example uses the linked core's local `rxjs` copy on purpose to keep `instanceof Observable` checks in core working.
- `SpiderMeshNode` metadata no longer carries `ips` or `online` fields. TCP transporters should rely on `host` and transporter endpoint metadata instead.

## Components

### `UdpDiscovery`

Discovers peers on the local network using UDP multicast and forwards discovery packets when a node is seen from a remote address.

Main responsibilities:

- Broadcast local node metadata.
- Ignore self-originated discovery packets.
- Filter nodes by namespace.
- Reply to first-contact `hi` messages.
- Fill `host` from the sender address when needed.
- Preserve multicast delivery without depending on a separate interface IP list from core.

Public surface:

```ts
class UdpDiscovery {
	constructor()
	broadcast<T extends NodeMetadata>(data: MdnsMessage<T>): Promise<void>
}
```

`UdpDiscovery` is itself an observable discovery transporter. `SpiderMesh` subscribes to it directly and calls `broadcast(...)` whenever local metadata changes.

### `Http2Rpc`

Implements the Spider Mesh RPC transporter contract over HTTP/2.

Main responsibilities:

- Open an HTTP/2 server for inbound RPC packets.
- Dial peer nodes discovered by core.
- Encode `RpcPacket` payloads with `msgpackr`.
- Emit RPC events and transporter metadata directly as an observable.
- Preserve remote transporter metadata instead of overwriting it with local ports.
- Route RPC responses back over the original HTTP/2 request stream keyed by `request_id`.
- Emit `rpc.node_id` instead of fabricating remote node objects inside the transporter.

Public surface:

```ts
class Http2Rpc extends Subject<RpcEvent> {
	readonly metadata?: RpcEvent['metadata']
	send(packet: RpcPacket, node: SpiderMeshNode): Promise<void>
}
```

### `Http2Pubsub`

Implements topic publish/subscribe over HTTP/2 between Spider Mesh nodes.

Main responsibilities:

- Open an HTTP/2 endpoint for inbound topic events.
- Maintain in-process topic subscriptions.
- Publish encoded payloads to discovered peer nodes that expose the pubsub transporter.

`Http2Pubsub` does not depend on `node.topics` metadata because current `@spider-mesh/core` event linking does not advertise subscriptions back into discovery metadata.

Public surface:

```ts
class Http2Pubsub {
	listen<T>(topic: string): Observable<T>
	publish<T>(topic: string, data: T): Promise<void>
}
```

## Environment Variables

The package reads the following runtime settings:

| Variable | Default | Description |
| --- | --- | --- |
| `SPIDERMESH_MULTICAST_ADDRESS` | `239.0.0.3` | UDP multicast group used for discovery. |
| `SPIDERMESH_MULTICAST_PORT` | `20002` | UDP multicast port used for discovery. |
| `SPIDERMESH_WHITELIST_ADDRESS` | unset | Optional comma-separated IPv4 targets or prefixes used for additional broadcast addresses. |
| `SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE` | enabled | When not explicitly disabled, RPC prefers the discovered node host for routing decisions. |

## Usage Sketch

This package is usually wired by `@spider-mesh/core`. A minimal setup looks like this:

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

const mesh = new SpiderMesh({
	transporters: [
		new UdpDiscovery(),
		new Http2Rpc(),
		new Http2Pubsub()
	]
})
```

## End-To-End Tests

The repository includes Bun-based e2e scenarios that mirror the structure used in `@spider-mesh/core`:

- `examples/tcp-smoke-test.ts`: transporter-level smoke test for discovery, RPC, and pubsub.
- `examples/tcp-e2e-test.ts`: provider/client RPC.
- `examples/tcp-e2e-reverse-test.ts`: reverse RPC.
- `examples/tcp-e2e-matrix-test.ts`: sync, async, observable, and error matrix.
- `examples/tcp-e2e-round-robin-test.ts`: routing across multiple providers.

Run them via scripts:

```bash
bun run test:tcp
bun run test:tcp:e2e
bun run test:tcp:e2e:reverse
bun run test:tcp:e2e:matrix
bun run test:e2e
```

The `tests/` folder contains `bun:test` wrappers around those example runners.

## Serialization

Network payloads are encoded with `msgpackr`.

- RPC packets are packed as full `RpcPacket` envelopes.
- Pub/sub messages are packed before being sent to subscribers.
- UDP discovery messages are packed before broadcast.

## Development

Build the package with:

```bash
bun run build
```

This outputs compiled artifacts into `build/`.

Useful commands during local work:

```bash
bun run build
bun test tests/tcp-transporters.e2e.test.ts
bun test tests/tcp-spidermesh.e2e.test.ts
bun test tests/tcp-spidermesh-reverse.e2e.test.ts
bun test tests/tcp-spidermesh-matrix.e2e.test.ts
bun test tests/tcp-spidermesh-round-robin.e2e.test.ts
```

`examples/` and `tests/` are intentionally excluded from the package TypeScript build.

`examples/` also has its own `tsconfig.json` for editor tooling and ad-hoc script work. Example scripts import Node globals such as `process` explicitly from `node:*` modules instead of relying on ambient globals.
