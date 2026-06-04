# Spider Mesh WS

`@spider-mesh/ws` provides the WebSocket transport package for `@spider-mesh/core`.

It contains:

- `WebsocketTransporter` for application nodes
- `WebsocketRelayServer` for the relay process

The package is ESM-only.

> **New here?** Start with the [Getting Started walkthrough](GETTING_STARTED.md) — a 3-terminal runnable example (relay + provider + client, no registry).

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

No registry is needed — the relay does the routing, and the transporter exposes relay
discovery to core as a `ServiceDirectory`.

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

const transporter = new WebsocketTransporter({
  heartbeatIntervalMs: 5000,
  reconnectIntervalMs: 1000,
})

transporter.connect('ws://127.0.0.1:8787')

const mesh = new SpiderMesh()
mesh.registerTransporter(transporter)
```

`SpiderMesh.registerTransporter()` links all supported capabilities on the same transporter instance, so one `WebsocketTransporter` serves RPC, discovery, pubsub, and `ServiceDirectory` (availability) together — no `Registry` required.

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

Options:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `port` | `number` | `8787` | Port the relay listens on. `server.port` returns the bound port (`number \| null`). |
| `host` | `string` | unset | Host/interface to bind. |
| `path` | `string` | unset | HTTP path the underlying `ws` server accepts upgrades on. |
| `isServerConnection` | `(socket, request) => boolean` | unset | Gate / classify each incoming connection. Return `true` for connections that participate in discovery and RPC routing (i.e. real mesh nodes), `false` for passive clients. This is the relay's access-control hook — there is no separate `auth` option. |

Call `server.close()` to stop the relay.

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

- maintain relay connections with automatic reconnect
- expose `status$` for per-URL connection status
- send and receive RPC frames; the relay routes by `destination_node_id` when present, otherwise selects a provider by service name with round-robin
- propagate discovery `hello` and `offline` frames; re-emits a `discovered` event only when a known node's advertised service list actually changes (duplicate `hello`s with an unchanged service list are suppressed)
- forward pubsub messages and subscription changes
- integrate directly with `mesh.registerTransporter(transporter)`

Current send contract:

```ts
send(packet: RpcRequestPacket | RpcResponsePacket): Promise<{ cancel: () => void }>
```

For `request` packets, the returned `cancel()` sends a cancel frame to the relay over the same socket. The relay routes it to the provider via an internal `request_id → socket` map. The provider stops the running Observable on receipt. `SpiderMesh` calls `cancel()` automatically when a subscriber unsubscribes before the stream completes.

`MICROSERVICE_OFFLINE` is produced by the **relay**, not the transporter: when the relay cannot route a `request` to any provider for the service, it returns a `MICROSERVICE_OFFLINE` RPC response. It therefore arrives **asynchronously as a response packet**. The transporter itself is not registry-aware and does not throw synchronously — it forwards every frame to the relay over an open socket.

Supported options (all optional, with defaults):

- `heartbeatIntervalMs` — default `30000`
- `reconnectIntervalMs` — default `1000`
- `unsubscribeDelayMs` — default `10000`

`connect(url)` is idempotent per URL and may be called for **multiple relay URLs**; the transporter maintains one connection per URL and keys `status$` by URL. Call `close(url)` to drop a specific relay connection.

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
- WebSocket connection status coverage
- SpiderMesh RPC e2e coverage
- reverse RPC coverage
- matrix coverage for sync / async / Observable / error return paths
- multi-provider round-robin routing coverage
- RPC timeout coverage
- fallback value coverage
- provider disconnect / offline detection coverage
- concurrent RPC coverage
- provider reconnect coverage
- failover coverage (3 nodes → kill 1 → 2 nodes continue serving)

Run the e2e suite with:

```bash
bun run test:e2e
```

Build with:

```bash
bun run build
```

> There is no bare `test` script. The canonical command is `bun run test:e2e`; individual scripts are available as `test:websocket`, `test:websocket:e2e`, `test:websocket:e2e:matrix`, and `test:websocket:e2e:reverse`.

## Usage Guidance

### Should

- **Start the relay before any provider or client.** Nodes discover each other through the relay; with no relay up, `connect()` just retries on `reconnectIntervalMs`.
- **Use WebSocket transport when nodes cannot share multicast** — across subnets, cloud VPCs, browsers, mobile apps, or the public internet. This is the right choice where `@spider-mesh/tcp` cannot reach.
- **Import the entry point that matches the runtime:** `/node` for Node and Bun, `/browser` for web, `/react-native` for RN, `/relay-server` for the relay process (Node/Bun only). All three transporter entries export a class named `WebsocketTransporter`.
- **Gate connections with `isServerConnection`** on any relay exposed beyond localhost. Treat it as the authentication/authorization hook — validate a token from the upgrade `request` and return `false` to reject or down-classify untrusted sockets.
- **Reuse one `WebsocketTransporter` per node** for RPC, discovery, and pubsub; `registerTransporter()` links all three capabilities on the same instance. You may `connect()` it to several relays for redundancy.
- **Call `transporter.close(url)` / `server.close()` on shutdown** to release sockets cleanly.

### Should not

- **Do not run the relay in the browser or React Native.** `/relay-server` needs a server runtime (Node/Bun); only the transporter runs in those environments.
- **Do not expect `send()` to throw `MICROSERVICE_OFFLINE` synchronously.** Offline is reported by the relay as an asynchronous RPC response — handle it via the RPC result/timeout path, not a `try/catch` around the call site.
- **Do not expose a relay without `isServerConnection` on an untrusted network.** Without it, any client that can reach the port joins routing.
- **Do not assume a single relay.** The transporter supports multiple relay URLs and per-URL `status$`; design reconnection/monitoring around the per-URL map, not one global flag.
- **Do not import from the package root.** There is no `.` export — always use a runtime-specific subpath.

## Notes

- The package root is not exported; use runtime-specific subpaths.
- RPC and relay frames are encoded with `@msgpack/msgpack`.
- Start the relay before providers and clients.
