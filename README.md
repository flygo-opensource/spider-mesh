# Spider Mesh WS

`@spider-mesh/ws` provides the WebSocket transport package for `@spider-mesh/core`.

It contains:

- `WebsocketTransporter` for application nodes
- `WebsocketRelayServer` for the relay process

The package is ESM-only.

## Install

```bash
bun add @spider-mesh/core @spider-mesh/ws rxjs reflect-metadata
```

Keep `@spider-mesh/ws` and `@spider-mesh/core` on matching versions.

## Export Surface

Node/Bun transporter:

```ts
import { WebsocketTransporter } from '@spider-mesh/ws/node'
```

Browser transporter:

```ts
import { WebsocketTransporter } from '@spider-mesh/ws/browser'
```

React Native transporter:

```ts
import { WebsocketTransporter } from '@spider-mesh/ws/react-native'
```

Relay server:

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'
```

## Runtime Setup

Create one `WebsocketTransporter`, connect it to the relay, then register that shared transporter directly on `SpiderMesh`.

```ts
import { Registry, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

const transporter = new WebsocketTransporter({
  heartbeatIntervalMs: 5000,
  reconnectIntervalMs: 1000,
})

transporter.connect('ws://127.0.0.1:8787')

const registry = new Registry()
const mesh = new SpiderMesh(registry)

mesh.registerTransporter(transporter)
```

`SpiderMesh.registerTransporter()` now links all supported capabilities on the same transporter instance, so one `WebsocketTransporter` can serve RPC, discovery, and pubsub together.

## Relay Server

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'

const server = new WebsocketRelayServer({
  host: '127.0.0.1',
  port: 8787,
})

console.log(`WebSocket relay listening on ws://127.0.0.1:${server.port}`)
```

The relay forwards discovery, RPC, and pubsub frames. It does not host application services.

## Provider Example

```ts
import { Microservice } from '@spider-mesh/core'

@Microservice()
class GreetingService {
  async hello(name: string) {
    return `hello ${name}`
  }
}

new GreetingService()
```

## Client Example

```ts
import { RemoteServiceLinker } from '@spider-mesh/core'

type GreetingService = {
  hello(name: string): Promise<string>
}

const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
  service: 'GreetingService',
  timeout: 5000,
})

await greeter.wait()
console.log(await greeter.hello('world'))
```

## WebsocketTransporter

`WebsocketTransporter` owns one shared WebSocket connection layer.

Current responsibilities:

- maintain relay connections
- expose `status$` for per-URL connection status
- send and receive RPC frames
- propagate discovery `hello` and `offline` frames
- forward pubsub messages and subscription changes
- integrate directly with `mesh.registerTransporter(transporter)`

Supported options:

- `heartbeatIntervalMs`
- `reconnectIntervalMs`
- `unsubscribeDelayMs`

`status$` is a `BehaviorSubject<Map<string, string>>` with values such as `connecting`, `connected`, `error`, and `not_connected`.

## Runtime Support

| Runtime | Transporter Entry | Relay Server |
| --- | --- | --- |
| Node.js | `@spider-mesh/ws/node` | supported |
| Bun | `@spider-mesh/ws/node` | supported |
| Browser | `@spider-mesh/ws/browser` | not supported |
| React Native | `@spider-mesh/ws/react-native` | not supported |

## Tests

The package includes:

- binary transporter smoke coverage
- connection status coverage
- SpiderMesh RPC e2e coverage
- reverse RPC coverage
- matrix coverage for sync/async/observable/error paths
- multi-provider routing coverage

Run the full suite with:

```bash
bun run test:e2e
```

Build with:

```bash
bun run build
```

## Notes

- The package root is not exported; use runtime-specific subpaths.
- RPC and relay frames are encoded with `@msgpack/msgpack`.
- Start the relay before providers and clients.
