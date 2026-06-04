# Getting Started — `@spider-mesh/ws`

A minimal, runnable walkthrough of the WebSocket transport. Use this transport when nodes
**cannot share UDP multicast** — across subnets, cloud VPCs, browsers, or mobile. A central
**relay** routes between nodes; **no `Registry` is required** (the relay does routing, and
the transporter supplies availability to core as a `ServiceDirectory`).

Runnable sources: [`examples/getting-started/`](examples/getting-started/).

## Install

```bash
bun add @spider-mesh/core @spider-mesh/ws rxjs reflect-metadata
```

## Run it (3 terminals)

The relay must be up before providers and clients.

```bash
# terminal 1 — the relay (routes frames; hosts no services)
bun run examples/getting-started/relay.ts

# terminal 2 — a provider (exposes GreetingService)
bun run examples/getting-started/provider.ts

# terminal 3 — a client (calls GreetingService)
bun run examples/getting-started/client.ts
# → hello world
```

## What each piece does

### 1. Relay — [`relay.ts`](examples/getting-started/relay.ts)

```ts
const relay = new WebsocketRelayServer({ host: '127.0.0.1', port: 8787 })
```

The relay tracks connected nodes (from `hello` frames), routes RPC by service
(round-robin) or by `destination_node_id`, and fans out discovery. It runs on a server
runtime (Node/Bun) only.

### 2. Provider — [`provider.ts`](examples/getting-started/provider.ts)

```ts
@Microservice()
class GreetingService {
  async hello(name: string) { return `hello ${name}` }
}
new GreetingService()                  // self-registers on construction; identity = class name

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

const mesh = new SpiderMesh()          // ← no Registry
mesh.registerTransporter(transporter)  // one transporter = rpc + discovery + pubsub + availability
```

### 3. Client — [`client.ts`](examples/getting-started/client.ts)

```ts
const greeter = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })
await greeter.wait()                    // resolves once the relay reports a provider (ServiceDirectory)
console.log(await greeter.hello('world'))
```

`greeter.hello(...)` returns an RxJS Observable that is also awaitable.

## Why there's no `Registry`

`SpiderMesh` is registry-free. Availability (`wait()` / `watch()` / `nodes`) comes from the
transporter's `ServiceDirectory`, which the WS transporter derives from the relay's
`hello` / `offline` frames. Routing is done by the relay. So a node holds no peer table.

## Next steps

- **Multiple providers** → the relay round-robins across them; `wait(() => mesh.listRpcNodes('GreetingService').length >= 2)`.
- **Browser / React Native** → import from `@spider-mesh/ws/browser` or `@spider-mesh/ws/react-native` (same `WebsocketTransporter` API).
- **Securing the relay** → pass `isServerConnection(socket, request)` to `WebsocketRelayServer` to gate which connections join routing.
- **Multiple relays** → call `transporter.connect(url)` for each; `status$` is keyed per URL.

See the [README](README.md) for the full API and [ARCHITECTURE.md](ARCHITECTURE.md) for internals.
