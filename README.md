# Spider Mesh WS

`@spider-mesh/ws` is the official WebSocket package for Spider Mesh.

It contains two pieces:

- `WebsocketTransporter`: the built-in transporter used by application nodes for RPC, discovery, and pubsub.
- `WebsocketRelayServer`: the relay process that forwards WebSocket traffic between nodes.

This package is intentionally separate from `@spider-mesh/core`.

The core package stays runtime-agnostic, while `@spider-mesh/ws` contains the Node.js/Bun-specific WebSocket implementation based on `ws` and MsgPack.

## What This Package Does

Use `@spider-mesh/ws` when you want the built-in WebSocket transport instead of writing a custom transporter.

The package provides:

- binary WebSocket frames encoded with `@msgpack/msgpack`
- RPC request, response, and cancel forwarding
- node discovery propagation through relay hello and offline events
- pubsub publish, subscribe, and unsubscribe forwarding
- reconnect and heartbeat handling in the client transporter
- a dedicated relay server entry at `@spider-mesh/ws/relay-server`

## When To Use It

Use this package when:

- your runtime is Node.js or Bun
- you want an official ready-to-use Spider Mesh transport
- you want one relay process that many providers and clients can connect to

Do not use this package as the default import source for runtime-agnostic APIs.

- Import `SpiderMesh`, `Microservice`, and `RemoteServiceLinker` from `@spider-mesh/core`
- Import `WebsocketTransporter` from `@spider-mesh/ws`
- Import `WebsocketRelayServer` from `@spider-mesh/ws/relay-server`

## Install

```bash
bun add @spider-mesh/core @spider-mesh/ws rxjs reflect-metadata
```

This package is ESM-only.

## Export Surface

Main entry:

```ts
import { WebsocketTransporter } from '@spider-mesh/ws'
```

Relay server entry:

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'
```

Recommended full import shape:

```ts
import { SpiderMesh, Microservice, RemoteServiceLinker } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws'
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'
```

## Runtime Support

| Runtime | `@spider-mesh/ws` | `@spider-mesh/ws/relay-server` |
| --- | --- | --- |
| Node.js | Supported | Supported |
| Bun | Supported | Supported |
| Browser | Not recommended | Not supported |
| React Native | Not recommended | Not supported |

If you need React Native or browser support, keep using `@spider-mesh/core` and provide a runtime-appropriate custom transporter.

## Architecture

Typical deployment shape:

1. Start one relay process with `WebsocketRelayServer`.
2. Start provider nodes with `WebsocketTransporter` and local `@Microservice()` classes.
3. Start client nodes with `WebsocketTransporter` and `RemoteServiceLinker.link()`.
4. In clients, wait for discovery before the first RPC call.

Conceptually:

- `@spider-mesh/core` owns the runtime and service model
- `@spider-mesh/ws` owns WebSocket transport and relay behavior
- the relay server forwards frames, but does not host application services

## Quick Start

### 1. Start a relay server

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'

const server = new WebsocketRelayServer({
	host: '127.0.0.1',
	port: 8787,
})

console.log(`WebSocket relay listening on ws://127.0.0.1:${server.port}`)
```

### 2. Start a provider

```ts
import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws'

const transporter = new WebsocketTransporter({
	heartbeatIntervalMs: 5000,
	reconnectIntervalMs: 1000,
})

transporter.connect('ws://127.0.0.1:8787')

@Microservice()
class GreetingService {
	async hello(name: string) {
		return `hello ${name}`
	}
}

new GreetingService()
new SpiderMesh({ transporters: [transporter] })
```

### 3. Start a client

```ts
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws'

const transporter = new WebsocketTransporter({
	heartbeatIntervalMs: 5000,
	reconnectIntervalMs: 1000,
})

transporter.connect('ws://127.0.0.1:8787')

type GreetingService = {
	hello(name: string): Promise<string>
}

const mesh = new SpiderMesh({ transporters: [transporter] })
const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
	service: 'GreetingService',
	timeout: 5000,
})

await greeter.wait()
console.log(await greeter.hello('world'))
```

## Transporter Options

`WebsocketTransporter` supports these options:

- `heartbeatIntervalMs`: interval between WebSocket ping frames
- `reconnectIntervalMs`: delay before reconnect attempts after disconnect
- `unsubscribeDelayMs`: delay before unsubscribe is sent after the last local subscriber leaves a topic

Example:

```ts
const transporter = new WebsocketTransporter({
	heartbeatIntervalMs: 5000,
	reconnectIntervalMs: 1000,
	unsubscribeDelayMs: 10000,
})
```

## Behavioral Notes

- RPC frames are forwarded as binary MsgPack payloads.
- Observable-returning RPC handlers are supported.
- Discovery is driven by relay `hello` and `offline` synchronization.
- Pubsub subscriptions are tracked by topic on the relay.
- Relay startup should happen before providers and clients.
- `WebsocketRelayServer` is a relay, not an application runtime.

## Development Scripts

From this package directory:

```bash
bun run build
bun run test:websocket
bun run test:websocket:e2e
bun run test:websocket:e2e:matrix
bun run test:websocket:e2e:reverse
bun run test:e2e
```

Examples:

```bash
bun run example:websocket:server
bun run example:websocket:provider
bun run example:websocket:client
```

## Examples To Read First

Canonical examples in this package:

- `examples/websocket-server.ts`
- `examples/websocket-provider.ts`
- `examples/websocket-client.ts`
- `examples/websocket-e2e-test.ts`
- `examples/websocket-e2e-matrix-test.ts`
- `examples/websocket-e2e-round-robin-test.ts`

Use these as the preferred implementation references when writing app code or generating code with AI tools.

## Relationship With Core

Keep these responsibilities separate:

- `@spider-mesh/core`: service runtime, decorators, remote linking, shared contracts
- `@spider-mesh/ws`: built-in WebSocket transporter
- `@spider-mesh/ws/relay-server`: dedicated relay process

If you are not specifically implementing WebSocket transport, default to `@spider-mesh/core`.
