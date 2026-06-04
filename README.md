# @spider-mesh/tcp

`@spider-mesh/tcp` provides the TCP transport implementations for `@spider-mesh/core`.

It exports three transporter **classes** for the current core runtime model:

- `UdpDiscovery` for multicast discovery
- `Http2Rpc` for point-to-point RPC over HTTP/2
- `Http2Pubsub` for topic publish/subscribe over HTTP/2

Instantiate each with `new`, passing a **shared `Registry`** (see [Runtime Setup](#runtime-setup)). Network configuration (ports/multicast) is via environment variables (see [Environment Variables](#environment-variables)).

The package is Node/Bun-only (it uses UDP sockets and HTTP/2), ESM-only, and built with TypeScript `moduleResolution: NodeNext`.

> **New here?** Start with the [Getting Started walkthrough](GETTING_STARTED.md) — a 2-terminal runnable example.

## Install

```bash
bun add @spider-mesh/core @spider-mesh/tcp
```

Keep `@spider-mesh/tcp` and `@spider-mesh/core` on matching versions so transporter contracts stay aligned.

## Exports

```ts
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'
```

## Runtime Setup

TCP transport owns its routing: construct **one shared `Registry`** and inject it into all three transporters via their constructors. `SpiderMesh` itself takes no registry. The HTTP/2 RPC and pubsub servers bind to an **OS-assigned ephemeral port** (`listen(0)`); only the UDP multicast port is configurable, via env var.

```ts
import { Registry, SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

const mesh = new SpiderMesh()
const registry = new Registry()

mesh.registerTransporter(new UdpDiscovery(registry))
mesh.registerTransporter(new Http2Rpc(registry))
mesh.registerTransporter(new Http2Pubsub(registry))
```

`UdpDiscovery(registry)` ingests discovered nodes into the shared registry and exposes it to core as a `ServiceDirectory` (powering `wait()`/`watch()`/`nodes`). `Http2Rpc(registry)` and `Http2Pubsub(registry)` read the same registry for routing.

`Http2Rpc` resolves target nodes from `Registry.getPeer(...)` and sends `RpcPacket` frames by `node_id`.

`Http2Pubsub` resolves publish targets from `Registry.listTopicNodes(topic)`.

## Transporters

### `UdpDiscovery`

`UdpDiscovery` is an observable discovery transporter.

Responsibilities:

- broadcast local node metadata over UDP multicast
- ignore self-originated discovery packets
- normalize `host` from the sender address when needed
- reply to `hi` probes
- forward remote discovery packets once across multicast

### `Http2Rpc`

`Http2Rpc` is an observable RPC transporter.

Responsibilities:

- expose local HTTP/2 endpoint metadata
- accept inbound RPC packets
- connect to remote nodes discovered by core
- preserve streaming request/response behavior
- route packets by `destination_node_id` embedded in the packet; falls back to `registry.pickRpcNode(service)` for round-robin when `destination_node_id` is absent

Current send contract:

```ts
send(packet: RpcRequestPacket | RpcCancelPacket | RpcResponsePacket): Promise<{ cancel: () => void }>
```

For `request` packets, the returned `cancel()` sends a `RpcCancelPacket` to the same destination node over the existing HTTP/2 connection. The provider stops the running Observable on receipt. `SpiderMesh` calls `cancel()` automatically when a subscriber unsubscribes before the stream completes.

Requires a `Registry` injected via the constructor (`new Http2Rpc(registry)`) before sending `request` packets. Throws `MICROSERVICE_OFFLINE` immediately when no registry is present.

### `Http2Pubsub`

`Http2Pubsub` is the pubsub transporter.

Responsibilities:

- expose `listen(topic)` as an RxJS stream
- publish encoded payloads to remote topic subscribers
- use registry topic lookups when a registry is linked

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `SPIDERMESH_MULTICAST_ADDRESS` | `239.0.0.3` | UDP multicast group used by discovery. |
| `SPIDERMESH_MULTICAST_PORT` | `20002` | UDP multicast port used by discovery. |
| `SPIDERMESH_WHITELIST_ADDRESS` | unset | Extra IPv4 targets or prefixes added to the discovery broadcast fan-out, comma-separated. A full 4-octet entry (`10.0.0.5`) is used as-is; a 3-octet prefix (`10.0.0`) is auto-expanded to the whole `.1`–`.254` /24 range. Use this when multicast is blocked (e.g. cloud VPCs). |

> Namespace (`SPIDERMESH_NAMESPACE`, from `@spider-mesh/core`) is respected by discovery: packets from a different namespace are dropped, so the same multicast group can host isolated meshes.
>
> Note: `SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE` is read by the code but currently has **no effect** on routing (the value is computed and never used). Do not rely on it; treat it as reserved.

## Example Flow

Provider:

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

Client:

```ts
import { RemoteServiceLinker } from '@spider-mesh/core'

type GreetingService = {
  hello(name: string): Promise<string>
}

const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
  service: 'GreetingService',
})

await greeter.wait()
console.log(await greeter.hello('tcp'))
```

## Tests

The package includes:

- transporter smoke coverage (RPC + PubSub)
- RPC transporter contract coverage
- discovery transporter contract coverage
- SpiderMesh RPC e2e coverage
- reverse RPC coverage
- matrix coverage for sync / async / Observable / error return paths
- multi-provider round-robin routing coverage
- RPC timeout coverage
- fallback value coverage
- provider crash / offline detection coverage
- concurrent RPC coverage
- failover coverage (3 nodes → kill 1 → 2 nodes continue serving)

Run the e2e suite with:

```bash
bun run test:e2e
```

Build with:

```bash
bun run build
```

> There is no bare `test` script. The canonical command is `bun run test:e2e`; individual smoke/e2e scripts are available as `test:tcp`, `test:tcp:e2e`, `test:tcp:e2e:matrix`, and `test:tcp:e2e:reverse`.

## Lifecycle / Teardown

Release sockets and HTTP/2 servers on shutdown. The teardown method is not uniform across the three transporters:

- `UdpDiscovery.unsubscribe()` — stops the socket and discovery loop.
- `Http2Rpc.unsubscribe()` — closes streams, destroys connections, closes the server.
- `Http2Pubsub.close()` — closes connections and the server.

## Usage Guidance

### Should

- **Use TCP transport on a trusted LAN / multicast-capable network** (bare metal, single VPC subnet, Docker host network). It is the right default for co-located services that can speak UDP multicast + HTTP/2.
- **Register all three transporters** (`UdpDiscovery`, `Http2Rpc`, `Http2Pubsub`) on `SpiderMesh`. Discovery populates the registry that RPC and pubsub route against; dropping it leaves the other two with no peers.
- **Set `SPIDERMESH_NAMESPACE`** (from core) per environment so dev/staging/prod meshes sharing a multicast group stay isolated.
- **Use `SPIDERMESH_WHITELIST_ADDRESS` when multicast is unavailable** — add explicit peer IPs or a `/24` prefix so discovery still reaches them.
- **Call the teardown method on shutdown** (see [Lifecycle / Teardown](#lifecycle--teardown)) to free ports and connections.

### Should not

- **Do not use this package in the browser or React Native.** It needs raw UDP and HTTP/2 — use `@spider-mesh/ws` there.
- **Construct one shared `Registry` and inject it into all three** transporters; don't give each its own. Beyond the registry, the constructors take no options — ports/addresses are not per-instance configurable (HTTP/2 ports are OS-ephemeral; multicast address/port via env vars).
- **Do not expect multicast to cross subnets, VPC boundaries, or most cloud networks.** When it cannot, fall back to `SPIDERMESH_WHITELIST_ADDRESS` or use `@spider-mesh/ws` with a relay.
- **Do not set `SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE` expecting routing to change** — it is currently inert.
- **Do not share one multicast group/port across unrelated meshes without setting distinct namespaces** — discovery filters by namespace, not by group.

## Notes

- `src/types.ts` re-exports core transporter types; `@spider-mesh/core` is the contract source of truth.
- `msgpackr` is used for RPC, discovery, and pubsub payload encoding.
- `examples/helpers/coreRxjs.ts` intentionally uses the linked core RxJS copy for observable identity in matrix tests.
