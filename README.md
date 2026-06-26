# Spider Mesh Core

`@spider-mesh/core` is the runtime-agnostic Spider Mesh package.

It provides:

- local microservice registration with decorators
- typed remote service linking through proxies
- RPC, discovery, pubsub, and `ServiceDirectory` transporter contracts
- a `SpiderMesh` runtime for local services, transporters, and node metadata (no registry required)
- a `Registry` helper for transports that route client-side
- NestJS helper adapters

Concrete transport implementations live in companion packages such as `@spider-mesh/tcp` and `@spider-mesh/ws`, or in your own custom transporters.

## Install

```bash
bun add @spider-mesh/core rxjs reflect-metadata
```

For TypeScript projects using decorators, enable decorator metadata in your compiler settings.

## Runtime Setup

`SpiderMesh` takes no constructor arguments — it does not own a registry. Availability
(`wait()`/`watch()`/`nodes`) is sourced from each transporter's `ServiceDirectory`, and
routing is owned by the transport.

```ts
import { SpiderMesh } from '@spider-mesh/core'

const mesh = new SpiderMesh()
```

Register transporter instances on the runtime. Transports that route client-side (like
`@spider-mesh/tcp`) keep their own peer table — construct one `Registry` and inject it
into them; relay-based transports (like `@spider-mesh/ws`) need nothing:

```ts
import { Registry } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

const registry = new Registry()
mesh.registerTransporter(new UdpDiscovery(registry))
mesh.registerTransporter(new Http2Rpc(registry))
mesh.registerTransporter(new Http2Pubsub(registry))
```

Transporter capability is inferred by instance shape:

- `send()` => RPC transporter
- `publish()` => pubsub transporter
- `broadcast()` => discovery transporter
- `watchService()` + `listNodes()` => `ServiceDirectory` (availability source for `wait()`/`watch()`/`nodes`)

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
import { firstValueFrom } from 'rxjs'

await users.set({ timeout: 3000, retry: 2 }).getUser('42')

await firstValueFrom(mesh.callRemoteService({
  service: 'UserService',
  method: 'getUser',
  args: ['42'],
  transporter: 'Http2Rpc',
}))
```

`transporter` may be:

- a registered transporter name string
- a class constructor (e.g. `Http2Rpc`)
- an object with a `name` property

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

## Registry (transport helper)

`SpiderMesh` no longer owns or depends on a `Registry`. It is exported as an optional
**helper for transports that route client-side**: such a transport constructs its own
`Registry`, feeds it from its discovery stream, and exposes it to core as a
`ServiceDirectory`. `@spider-mesh/tcp` does exactly this; relay transports
(`@spider-mesh/ws`) need no registry at all.

Public methods:

- `getPeer(nodeId)`
- `upsertPeer(node)`
- `removePeer(nodeId)`
- `listPeers(service?)`
- `watch(service?)`
- `pickRpcNode(service, { node_id?, filter? })` — round-robin; `filter(node)` lets a transport exclude peers it cannot route to yet (e.g. a provider whose RPC endpoint port isn't advertised)
- `getRpcTransporterName(service)`
- `listTopicNodes(topic)`

## ServiceDirectory

A transporter MAY implement `ServiceDirectory` to tell core which nodes serve a service.
Core merges this across all registered transporters to answer `wait()` / `watch()` / `nodes`,
without owning any peer state itself.

```ts
type ServiceDirectory = {
  watchService(service: string): Observable<NodeRef[]>  // emit current state promptly (BehaviorSubject semantics)
  listNodes(service: string): NodeRef[]                  // sync snapshot; [] if the transport cannot enumerate
}
```

## Transporter Contracts

The contract source of truth is `src/types.ts`.

### RPC transporter

```ts
type RpcTransporter = Observable<RpcEvent> & {
  send(data: RpcRequestPacket | RpcResponsePacket): Promise<{ cancel: () => void }>
  canRoute(service: string, node_id?: string): boolean
}
```

`canRoute` is **required**. When no explicit `transporter` is bound and more than one RPC
transporter is registered, core's `#selectRpcTransport` dispatches through the first transporter
whose `canRoute(service, node_id)` returns `true` (falling back to registration order if none
claims a route), so routing follows the same source of truth as `wait()` / `watch()` / `nodes`.
It MUST be side-effect free. A transporter that cannot enumerate reachability may `return true`
to stay a candidate and let `send()` surface the real error.

The core contract `send()` accepts only `request` and `response` packets. Cancellation is exposed through the `cancel()` function returned by `send()`, not by passing a `RpcCancelPacket` to `send()`. A concrete transporter may translate that `cancel()` into a `RpcCancelPacket` on the wire (for example `@spider-mesh/tcp`), but that is a transport-internal detail.

`send()` returns a `cancel` function. For `request` packets, calling `cancel()` sends a `RpcCancelPacket` to the destination node, which causes the provider to stop any running stream. For `response` and `cancel` packets the returned `cancel` is a no-op.

`SpiderMesh` calls `cancel()` automatically when a subscriber unsubscribes from a stream that has not yet completed.

### Packet types

```ts
type RpcRequestPacket = {
  kind: 'request'
  request_id: string
  service: string
  method: string
  args: any[]
  sender_node_id: string
  destination_node_id?: string  // explicit target; transporter falls back to registry round-robin when omitted
}

type RpcResponsePacket = {
  kind: 'response'
  request_id: string
  data?: any
  error?: SpiderMeshError | { code?: string; message: string }
  completed?: boolean
  destination_node_id?: string
}

type RpcCancelPacket = {
  kind: 'cancel'
  request_id: string
  destination_node_id?: string
}
```

### Pubsub transporter

```ts
type PubsubTransporter = Observable<PubsubEvent> & {
  publish<T>(topic: string, data: T): Promise<void>
  listen<T>(topic: string): Observable<T>
}
```

### Discovery transporter

```ts
type DiscoveryTransporter = Observable<DiscoveryEvent> & {
  broadcast(data: MdnsMessage<NodeMetadata>): Promise<void>
}
```

## NestJS Helpers

The package exports:

- `NestJSExposeMicroservice(factory, metadata?)`
- `NestJSLinkMicroservice(factory, transporter?)`
- `NestJSLinkEvent(factory)`

`NestJSLinkMicroservice(factory, transporter?)` forwards the optional transporter selector into `RemoteServiceLinker.link()`.

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

## NestJS Integration

### Register `SpiderMesh` as a provider

```ts
import { Module } from '@nestjs/common'
import { Registry, SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

@Module({
  providers: [
    {
      provide: SpiderMesh,
      useFactory: () => {
        const mesh = new SpiderMesh()
        const registry = new Registry()

        mesh.registerTransporter(new UdpDiscovery(registry))
        mesh.registerTransporter(new Http2Rpc(registry))
        mesh.registerTransporter(new Http2Pubsub(registry))

        return mesh
      },
    },
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

Pass a transporter only when you need to force a specific RPC transporter:

```ts
NestJSLinkMicroservice(BillingService, 'Http2Rpc')
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

- `LimitConcurrency(limit)` and `LimitConcurrentRunning(limit)` for throttling async method execution (`LimitConcurrentRunning` is an alias of `LimitConcurrency`)
- `MicroserviceError` — a TypeScript **type only** describing the RPC error shape (`{ code: SpiderMeshErrorCode; message?: string }`). There is no runtime class or error-code constants object; the code strings live in the `SpiderMeshErrorCode` union (see [Error Model](#error-model)). Do not `import` it as a value.

## Usage Guidance

### Should

- **Keep `@spider-mesh/core` runtime-agnostic.** Put RPC/business contracts and runtime wiring here; import a concrete transport (`@spider-mesh/tcp`, `@spider-mesh/ws`, or your own) only at the composition root where you build `SpiderMesh`.
- **Construct local services to register them.** `@Microservice()` self-registers on construction — just call `new UserService()` (or let your DI container instantiate it). There is no separate `register()` call.
- **`await linker.wait()` before the first remote call.** Linking is lazy; `wait()` resolves once at least one provider node is known (or your predicate passes). Calling a remote method before a provider exists relies on `timeout`/`fallback` instead.
- **Define a typed contract for `RemoteServiceLinker.link<T>()`.** The proxy is only as safe as the `T` you give it; the runtime does not validate the remote signature.
- **Tune reliability per call with `.set({ timeout, retry, fallback })`** rather than hard-coding values, and provide a `fallback` when a degraded result is acceptable — `callRemoteService` short-circuits to `of(fallback)` on *any* caught error when `fallback !== undefined`.
- **Use `linkEvent(EventClass)` for pub/sub fan-out** and treat the event class name as the topic identity. Keep event classes stable across services.
- **Unsubscribe from remote streams you no longer need.** `SpiderMesh` cancels the in-flight provider stream when the last subscriber unsubscribes — leaked subscriptions keep provider work alive.
- **Force `node_id` only when you need sticky routing** (e.g. a stateful provider). Default routing is round-robin and that is what you want most of the time.

### Should not

- **Do not put socket/wire logic in core.** Transporters own byte transport, topic IO, and discovery broadcasts; core owns lifecycle, routing, timeout, retry, and cancel.
- **Do not `import { LOCAL_SERVICES$ }` or `MicroserviceList` from the package root** — they are internal module exports, not part of the public API.
- **Do not treat `MicroserviceError` as a runtime value.** It is a type alias; there is no class to `new` and no constants object to read codes from.
- **Do not assume `@Microservice({ version })` (or any service metadata) affects routing.** Metadata is free-form and opaque to selection; routing keys on the class name only. The typed node-level `version` is an internal monotonic counter, unrelated to this field.
- **Do not rely on multiple `SpiderMesh` instances in one process for isolation.** `LOCAL_SERVICES$` is process-global, so every mesh sees every local service. Use separate processes (and `SPIDERMESH_NAMESPACE`) to isolate.
- **Do not rename a service class casually.** Service identity — and therefore every remote link and route — is the class name. Renaming is a breaking change across the mesh.
- **Do not mutate `Registry` peer state directly** unless you are implementing a transporter; let discovery transporters populate it.

## Notes

- This package is ESM-only.
- Repository source uses emitted `.js` relative specifiers.
- `LOCAL_SERVICES$` is process-global inside one process. It is an internal module stream, not re-exported from the package root — every `SpiderMesh` in the same process sees every `@Microservice()` instance constructed in that process.
- Local services are registered when their class instances are constructed.
- Service identity is based on the class name.
- Event topic identity is based on the event class name.
- RPC target selection is round-robin unless you force `node_id`.
- `SpiderMesh` owns RPC stream lifecycle, timeout, retry, and cancel behavior. When a subscriber unsubscribes before a stream completes, `SpiderMesh` calls the `cancel()` function returned by `transporter.send()`, which causes the provider to stop the running Observable.
- Transporters focus on byte transport, pubsub topic IO, and discovery broadcasts.
- The root package entry intentionally focuses on runtime-agnostic APIs and shared contracts.
- If an AI agent is uncertain which import to use, prefer `@spider-mesh/core` first, then opt into a companion transport package such as `@spider-mesh/tcp` or `@spider-mesh/ws` only when a concrete transport is needed.