# Spider Mesh Core

`@spider-mesh/core` is the runtime-agnostic Spider Mesh package.

It provides:

- local microservice registration with decorators
- typed remote service linking through proxies
- RPC, discovery, and pubsub transporter contracts
- a `Registry` for remote peer state and RPC routing
- a `SpiderMesh` runtime for local services, transporters, and node metadata
- NestJS helper adapters

Concrete transport implementations live in companion packages such as `@spider-mesh/tcp` and `@spider-mesh/ws`, or in your own custom transporters.

## Install

```bash
bun add @spider-mesh/core rxjs reflect-metadata
```

For TypeScript projects using decorators, enable decorator metadata in your compiler settings.

## Runtime Setup

Create a registry when you need remote peer discovery and RPC routing.

```ts
import { Registry, SpiderMesh } from '@spider-mesh/core'

const registry = new Registry()
const mesh = new SpiderMesh(registry)
```

Register transporter instances on the runtime:

```ts
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

mesh.registerTransporter(new UdpDiscovery())
mesh.registerTransporter(new Http2Rpc())
mesh.registerTransporter(new Http2Pubsub())
```

Transporter capability is inferred by instance shape:

- `send()` => RPC transporter
- `publish()` => pubsub transporter
- `broadcast()` => discovery transporter

## Local Services

Use `@Microservice()` on local service classes and instantiate them normally.

```ts
import { BeforeMicroserviceOnline, Microservice } from '@spider-mesh/core'

@Microservice({ version: '1.0.0' })
export class UserService {
  private ready = false

  @BeforeMicroserviceOnline()
  async warmup() {
    this.ready = true
  }

  async getUser(id: string) {
    if (!this.ready) throw new Error('Service not ready')
    return { id, name: 'Ada' }
  }
}

new UserService()
```

`@Microservice()` emits the constructed instance into the shared `LOCAL_SERVICES$` stream. `SpiderMesh` subscribes to that stream and adds the service to local node metadata.

## Remote Services

Create a typed remote client with `RemoteServiceLinker.link()`.

```ts
import { RemoteServiceLinker } from '@spider-mesh/core'

type UserServiceContract = {
  getUser(id: string): Promise<{ id: string; name: string }>
}

const users = RemoteServiceLinker.link<UserServiceContract>(mesh, {
  service: 'UserService',
})

await users.wait()

const user = await users.getUser('42')
```

Remote methods return RxJS observables and can also be awaited.

```ts
const user = await users.getUser('42')

users.getUser('42').subscribe(value => {
  console.log(value)
})
```

### Waiting and watching

```ts
await users.wait(nodes => nodes.length > 0)

users.watch().subscribe(nodes => {
  console.log(nodes.map(node => node.node_id))
})

console.log(users.nodes)
```

### Fan-out calls

```ts
users.__batch__getUser('42').subscribe(result => {
  console.log(result)
})
```

Each emission is either `{ node, data }` or `{ node, error }`.

## RPC Options

The current `RpcOptions` shape is:

```ts
type RpcOptions<T = any> = {
  service: string
  method: string
  args: any[]
  fallback?: T
  timeout?: number
  retry?: number
  node_id?: string
  transporter?: string | { name?: string }
}
```

Examples:

```ts
await users.set({ timeout: 3000, retry: 2 }).getUser('42')

await mesh.callRemoteService({
  service: 'UserService',
  method: 'getUser',
  args: ['42'],
  transporter: 'Http2Rpc',
})
```

`transporter` may be:

- a registered transporter name string
- a class or object with a `name`

## Events

Use `SpiderMesh.linkEvent()` to bind a topic by event class name.

```ts
class UserCreatedEvent {
  constructor(
    public readonly id: string,
    public readonly email: string,
  ) {}
}

const userCreated = mesh.linkEvent(UserCreatedEvent)

await userCreated.publish(new UserCreatedEvent('42', 'ada@example.com'))

const sub = userCreated.listen().subscribe(event => {
  console.log(event.id)
})

sub.unsubscribe()
```

Current event behavior:

- `publish()` fans out through all registered pubsub transporters
- `listen()` merges all transporter listeners into one shared stream
- first subscribe adds the topic to local node metadata
- last unsubscribe removes the topic from local node metadata
- discovery transporters rebroadcast node metadata after topic changes

## Registry

`Registry` stores remote peer and RPC routing state.

Current public methods:

- `getPeer(nodeId)`
- `upsertPeer(node)`
- `removePeer(nodeId)`
- `listPeers({ service? })`
- `watch(service?)`
- `pickRpcNode(service, { node_id? })`
- `getRpcTransporterName(service, { node_id? })`
- `listTopicNodes(topic)`

## Transporter Contracts

The contract source of truth is `src/types.ts`.

### RPC transporter

```ts
type RpcTransporter = Observable<RpcEvent> & {
  linkRegistry?(registry: Registry): void
  send(data: RpcPacket, node_id?: string): Promise<void>
}
```

### Pubsub transporter

```ts
type PubsubTransporter = {
  publish<T>(topic: string, data: T): Promise<void>
  listen<T>(topic: string): Observable<T>
  linkRegistry?(registry: Registry): void
}
```

### Discovery transporter

```ts
type DiscoveryTransporter = Observable<DiscoveryEvent> & {
  linkRegistry?(registry: Registry): void
  broadcast(data: MdnsMessage<NodeMetadata>): Promise<void>
}
```

## NestJS Helpers

The package exports:

- `NestJSExposeMicroservice(factory, metadata?)`
- `NestJSLinkMicroservice(factory, transporter)`
- `NestJSLinkEvent(factory)`

`NestJSLinkMicroservice(factory, transporter)` forwards the transporter selector into `RemoteServiceLinker.link()`.

## Companion Packages

Use companion packages when you need concrete transport implementations.

- `@spider-mesh/tcp`
- `@spider-mesh/ws`

Keep runtime logic in `@spider-mesh/core` and import concrete transporters explicitly from the companion package.

## Tests And Validation

The repository includes mock e2e coverage for:

- RPC routing by transporter selector
- async `RemoteServiceLinker.wait()` behavior
- topic metadata lifecycle for `linkEvent()`
- RPC routing across isolated child processes

Run tests with:

```bash
bun run test:e2e
```

Build with:

```bash
bun run build
```

## Notes

- This package is ESM-only.
- Repository source uses emitted `.js` relative specifiers.
- `LOCAL_SERVICES$` is process-global inside one process.

await userCreated.publish(new UserCreatedEvent('42', 'ada@example.com'))

userCreated.listen().subscribe(event => {
  console.log(event.id)
})
```

## NestJS Integration

### Register `SpiderMesh` as a provider

```ts
import { Module } from '@nestjs/common'
import { SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

@Module({
  providers: [
    SpiderMesh.asProvider({
      transporters: [
        UdpDiscovery,
        Http2Rpc,
        Http2Pubsub,
      ],
    }),
  ],
  exports: [SpiderMesh],
})
export class MeshModule {}
```

### Expose a NestJS service as a microservice

```ts
import { Injectable, Module } from '@nestjs/common'
import { NestJSExposeMicroservice } from '@spider-mesh/core'

@Injectable()
export class BillingService {
  async charge(orderId: string) {
    return { orderId, status: 'ok' }
  }
}

@Module({
  providers: [
    BillingService,
    NestJSExposeMicroservice(BillingService, { boundedContext: 'billing' }),
  ],
})
export class BillingModule {}
```

### Inject a remote service proxy in NestJS

```ts
import { Inject, Injectable, Module } from '@nestjs/common'
import { NestJSLinkMicroservice } from '@spider-mesh/core'

class BillingService {
  charge(orderId: string): Promise<{ orderId: string; status: string }> {
    throw new Error('typing only')
  }
}

@Injectable()
export class CheckoutService {
  constructor(
    @Inject(BillingService)
    private readonly billing: BillingService,
  ) {}

  async checkout(orderId: string) {
    return this.billing.charge(orderId)
  }
}

@Module({
  providers: [
    NestJSLinkMicroservice(BillingService),
    CheckoutService,
  ],
})
export class CheckoutModule {}
```

### Inject an event binding in NestJS

```ts
import { Inject, Injectable, Module } from '@nestjs/common'
import { NestJSLinkEvent } from '@spider-mesh/core'

class UserCreatedEvent {
  constructor(public readonly id: string) {}
}

@Injectable()
export class AuditService {
  constructor(
    @Inject(UserCreatedEvent)
    private readonly userCreated: {
      publish(data: UserCreatedEvent): Promise<void>
      listen(): any
    },
  ) {}
}

@Module({
  providers: [
    NestJSLinkEvent(UserCreatedEvent),
    AuditService,
  ],
})
export class AuditModule {}
```

## Transporter Contract

Concrete transport implementations can live in companion packages such as `@spider-mesh/tcp` and `@spider-mesh/ws`, or in your own application code.

You can also provide your own classes that implement one or more transporter contracts exported by `@spider-mesh/core`.

### Public API map

- `SpiderMesh`: runtime coordinator for services, events, discovery, and transporters
- `RemoteServiceLinker.link()`: creates a typed remote proxy
- `@Microservice()`: exposes a local class instance as a remote service
- `SpiderMesh.linkEvent()`: creates a topic binding for publish and subscribe
- `RpcTransporter`: RPC transporter contract
- `DiscoveryTransporter`: discovery transporter contract
- `PubsubTransporter`: pubsub transporter contract

### RPC transporter

```ts
type RpcMessage = {
  node_id: string
  packet: RpcPacket
}

type RpcEvent = Partial<{
  rpc: RpcMessage
  offline: string
  endpoints: Record<string, string | boolean | number>
}>

type RpcTransporter = Observable<RpcEvent> & {
  send(data: RpcPacket, node: SpiderMeshNode): Promise<void>
}
```

The RPC observable can emit:

- `rpc`: inbound RPC message shaped as `{ node_id, packet }`
- `offline`: node offline event
- `endpoints`: transporter metadata to attach to the current node

The internal RPC wire protocol supports:

- `request`
- `response`
- `cancel`

`response` packets can carry:

- `data`
- `error`
- `completed`

### Discovery transporter

```ts
type DiscoveryEvent = {
  discovered: SpiderMeshNode
}

type DiscoveryTransporter = Observable<DiscoveryEvent> & {
  broadcast(
    data: MdnsMessage<NodeMetadata>,
    ips: string[],
  ): Promise<void>
}
```

Discovery transporters stream remote node snapshots into the mesh, and `SpiderMesh` itself calls `broadcast()` whenever local node metadata changes.

### Pubsub transporter

```ts
type PubsubTransporter = {
  publish<T>(topic: string, data: T): Promise<void>
  listen<T>(topic: string): Observable<T>
}
```

## Error Model

The core defines these error codes for RPC flows:

- `MICROSERVICE_OFFLINE`
- `MICROSERVICE_NOT_FOUND`
- `MICROSERVICE_RPC_TIMEOUT`

## Environment Variables

- `SPIDERMESH_NAMESPACE`: namespace of the current node, default `default`
- `SPIDERMESH_NODE_HOSTNAME`: optional hostname attached to node metadata

## Helpers

The package also exports:

- `LimitConcurrency(limit)` and `LimitConcurrentRunning(limit)` for throttling async method execution
- `randomUUID()` for Node.js, browser, and React Native compatible UUID generation using ESM-safe runtime detection
- `MicroserviceException` types for common RPC error codes

## Build

```bash
bun run build
```

## Notes

- Local services are registered when their class instances are constructed.
- Service identity is based on the class name.
- Event topic identity is based on the event class name.
- RPC target selection is round-robin unless you force `node_id` or `ip`.
- `SpiderMesh` owns RPC stream lifecycle, timeout, retry, and cancel behavior.
- Transporters focus on byte transport, pubsub topic IO, and discovery broadcasts.
- The root package entry intentionally focuses on runtime-agnostic APIs and shared contracts.
- If an AI agent is uncertain which import to use, prefer `@spider-mesh/core` first, then opt into a companion transport package such as `@spider-mesh/tcp` or `@spider-mesh/ws` only when a concrete transport is needed.