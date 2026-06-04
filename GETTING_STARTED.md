# Getting Started — `@spider-mesh/tcp`

A minimal, runnable walkthrough of the TCP transport. Use this transport on a **trusted
LAN / multicast-capable network** (bare metal, a single VPC subnet, Docker host network).
It discovers peers over **UDP multicast** — there is **no relay process**. Node/Bun only
(it uses raw UDP + HTTP/2).

Because core is registry-free, **tcp owns its own routing**: you construct one shared
`Registry` and inject it into all three transporters.

Runnable sources: [`examples/getting-started/`](examples/getting-started/).

## Install

```bash
bun add @spider-mesh/core @spider-mesh/tcp
```

## Run it (2 terminals, same host / LAN)

```bash
# terminal 1 — a provider (exposes GreetingService)
bun run examples/getting-started/provider.ts

# terminal 2 — a client (discovers the provider, then calls it)
bun run examples/getting-started/client.ts
# → hello world
```

## The construction pattern

Both nodes build the mesh the same way — [`provider.ts`](examples/getting-started/provider.ts) / [`client.ts`](examples/getting-started/client.ts):

```ts
const mesh = new SpiderMesh()    // ← core holds no registry
const registry = new Registry()  // ← tcp's own routing table, shared by all three transporters

mesh.registerTransporter(new UdpDiscovery(registry))  // fills the registry + exposes ServiceDirectory to core
mesh.registerTransporter(new Http2Rpc(registry))      // routes RPC via the registry
mesh.registerTransporter(new Http2Pubsub(registry))   // routes pubsub via the registry
```

- **`UdpDiscovery(registry)`** broadcasts this node's metadata over multicast, ingests
  discovered peers into the shared `registry`, and exposes that registry to core as a
  `ServiceDirectory` (so `wait()` / `watch()` / `nodes` work). It also evicts peers when
  their RPC connection drops.
- **`Http2Rpc(registry)`** / **`Http2Pubsub(registry)`** read the same registry to pick a
  target node and connect over an OS-assigned HTTP/2 port.

The provider exposes a service the usual way:

```ts
@Microservice()
class GreetingService {
  async hello(name: string) { return `hello ${name}` }
}
new GreetingService()
```

And the client calls it, waiting for discovery first:

```ts
const greeter = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })
await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)
console.log(await greeter.hello('world'))
```

## Configuration (environment variables)

All network config is via env vars (constructors take only the registry):

| Variable | Default | Purpose |
| --- | --- | --- |
| `SPIDERMESH_NAMESPACE` | `default` | Isolate meshes sharing one multicast group. |
| `SPIDERMESH_MULTICAST_ADDRESS` | `239.0.0.3` | Multicast group. |
| `SPIDERMESH_MULTICAST_PORT` | `20002` | Multicast port. |
| `SPIDERMESH_WHITELIST_ADDRESS` | unset | Extra IPv4 targets / `/24` prefixes when multicast is blocked. |

HTTP/2 RPC/pubsub ports are OS-assigned (ephemeral) and not configurable.

## When NOT to use tcp

If nodes can't share multicast (across subnets, cloud VPCs, browsers, mobile), use
[`@spider-mesh/ws`](../ws/GETTING_STARTED.md) with a relay instead.

See the [README](README.md) for the full API and [ARCHITECTURE.md](ARCHITECTURE.md) for internals.
