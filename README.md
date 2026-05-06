# @spider-mesh/tcp

`@spider-mesh/tcp` provides the TCP transport implementations for `@spider-mesh/core`.

It exports three transporter instances for the current core runtime model:

- `UdpDiscovery` for multicast discovery
- `Http2Rpc` for point-to-point RPC over HTTP/2
- `Http2Pubsub` for topic publish/subscribe over HTTP/2

The package is ESM-only and built with TypeScript `moduleResolution: NodeNext`.

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

TCP transport relies on `Registry` for peer routing. Register the three transporter instances explicitly on `SpiderMesh`.

```ts
import { Registry, SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

const registry = new Registry()
const mesh = new SpiderMesh(registry)

mesh.registerTransporter(new UdpDiscovery())
mesh.registerTransporter(new Http2Rpc())
mesh.registerTransporter(new Http2Pubsub())
```

`UdpDiscovery` keeps the registry populated with remote nodes.

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
- route packets by `node_id`

Current send contract:

```ts
send(packet: RpcPacket, node_id?: string): Promise<void>
```

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
| `SPIDERMESH_WHITELIST_ADDRESS` | unset | Extra IPv4 targets or prefixes added to discovery broadcast fan-out. |
| `SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE` | enabled | Enables host-based RPC connection selection for HTTP/2 routing. |

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

- transporter smoke coverage
- RPC contract coverage
- discovery contract coverage
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

- `src/types.ts` re-exports core transporter types; `@spider-mesh/core` is the contract source of truth.
- `msgpackr` is used for RPC, discovery, and pubsub payload encoding.
- `examples/helpers/coreRxjs.ts` intentionally uses the linked core RxJS copy for observable identity in matrix tests.
