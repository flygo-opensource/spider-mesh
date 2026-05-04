# Spider Mesh Core

`@spider-mesh/core` is a lightweight TypeScript microservice core for service-to-service RPC, pubsub events, and node discovery with pluggable transporters.

It provides:

- Decorator-based local service registration
- Typed remote service linking through a Proxy
- Stream-first RPC over transporter-managed byte delivery
- Discovery and pubsub integration through simple transporter contracts
- Transporter contracts that can be implemented directly or consumed through companion transport packages such as `@spider-mesh/tcp` and `@spider-mesh/ws`
- NestJS adapters for exposing and consuming services

## Package Boundary

Spider Mesh is now split into two main package layers:

- `@spider-mesh/core`: runtime-agnostic service runtime, decorators, linking, and shared contracts
- companion transport packages: concrete transport implementations such as `@spider-mesh/tcp` and `@spider-mesh/ws`

Keep companion packages on the same published version as `@spider-mesh/core` to avoid contract drift between the runtime and concrete transports.

If you are reading this package first, the practical rule is simple:

- stay in `@spider-mesh/core` for service code and runtime code
- add a companion transport package only when you explicitly need a concrete transport implementation

## Install

```bash
bun add @spider-mesh/core rxjs reflect-metadata
```

If you also install a companion transport package, keep it on the same release version as `@spider-mesh/core`.

This package is ESM-only.

Practical implications:

- published output is loaded as ESM through the root export map
- repository TypeScript uses `module: NodeNext` and `moduleResolution: NodeNext`
- internal relative imports use emitted `.js` specifiers, for example `./SpiderMesh.js`
- avoid CommonJS patterns such as `require()` when adding new runtime code

## AI Usage Guide

Use this section as the canonical decision guide when generating code with AI tools.

### Environment support matrix

| Runtime | `@spider-mesh/core` | `@spider-mesh/tcp` | `@spider-mesh/ws` |
| --- | --- | --- | --- |
| Node.js | Supported | Supported | Supported |
| Bun | Supported | Supported | Supported |
| Browser | Partially supported, depends on your custom transporter | Not recommended | Not recommended |
| React Native | Supported for the core entry only | Not recommended | Not recommended |

### Recommended imports

Use these imports exactly.

- Core runtime, decorators, RPC linking, NestJS helpers: `import { ... } from '@spider-mesh/core'`
- Shared transporter contracts and runtime APIs: `import { ... } from '@spider-mesh/core'`
- Companion TCP transport implementation when needed: `import { Http2Rpc, Http2Pubsub, UdpDiscovery } from '@spider-mesh/tcp'`
- Companion WebSocket transport implementation when needed: `import { WebsocketTransporter } from '@spider-mesh/ws'`

### When to use what

- Use `SpiderMesh` when you need a local runtime that can expose services, discover nodes, publish events, or call remote services.
- Use `@Microservice()` on local classes that should be callable remotely.
- Use `RemoteServiceLinker.link()` when you need a typed remote proxy for another service.
- Use transporter contracts from `@spider-mesh/core` when you are implementing or typing your own transport.
- Use `@spider-mesh/tcp` or `@spider-mesh/ws` when you want a companion transport package.
- Use a custom transporter when the runtime is not Node.js or Bun, or when you need a different protocol.

### Do and don't

Do:

- Import runtime-agnostic APIs from `@spider-mesh/core`.
- Import transporter contracts from `@spider-mesh/core` when you need transport typing.
- Use a companion transport package such as `@spider-mesh/tcp` or `@spider-mesh/ws` when you need a ready-made transport implementation.
- Wait for remote service availability with `wait()` before making calls in startup flows.
- Treat companion package examples and e2e tests as canonical usage references for concrete transport behavior.

Don't:

- Do not assume `@spider-mesh/core` exports every concrete transporter.
- Do not assume `@spider-mesh/tcp` is React Native-ready.
- Do not assume `@spider-mesh/ws` is React Native-ready.
- Do not couple service code to a single transport package unless that is intentional.
- Do not call remote services before the target service has been discovered unless you intentionally rely on retry behavior.

### Canonical recipes

Provider recipe:

1. Import `Microservice` and `SpiderMesh` from `@spider-mesh/core`.
2. Choose a concrete transporter implementation, for example from `@spider-mesh/tcp` or `@spider-mesh/ws`.
3. Define a class and decorate it with `@Microservice()`.
4. Instantiate the service class.
5. Create `new SpiderMesh({ transporters: [transporter] })`.

Client recipe:

1. Import `SpiderMesh` and `RemoteServiceLinker` from `@spider-mesh/core`.
2. Choose a concrete transporter implementation, for example from `@spider-mesh/tcp` or `@spider-mesh/ws`.
3. Create `new SpiderMesh({ transporters: [transporter] })`.
4. Create a typed remote proxy with `RemoteServiceLinker.link()`.
5. Call `await proxy.wait()` before the first RPC call.

Companion transport package recipes:

TCP:

1. Import `UdpDiscovery`, `Http2Rpc`, and `Http2Pubsub` from `@spider-mesh/tcp`.
2. Add those transporters to `new SpiderMesh({ transporters: [...] })`.
3. Let the TCP package handle discovery, RPC, and pubsub transport.

WebSocket:

1. Import `WebsocketTransporter` from `@spider-mesh/ws`.
2. Add that transporter to `new SpiderMesh({ transporters: [...] })`.
3. Run `WebsocketRelayServer` from `@spider-mesh/ws/relay-server` when using the WebSocket companion package.

### AI-safe assumptions

An AI agent should assume the following unless the codebase says otherwise:

- The root package entry is the safe default for shared runtime APIs.
- Concrete transports are opt-in through companion packages or custom implementations.
- Transporter contracts come from the core package.
- The companion TCP and WebSocket packages are sibling transport options for Node.js and Bun runtimes.
- If the target runtime is React Native, prefer the core APIs and a runtime-appropriate custom transporter.

## Core Model

`SpiderMesh` is the runtime coordinator.

It keeps track of:

- The current node metadata
- Local service instances registered in the process
- Remote nodes discovered from discovery transporters
- RPC-capable nodes for each service
- RPC pending and running stream state
- Pubsub, RPC, and discovery transporters

## Quick Start

### 1. Define a local microservice

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
```

`@Microservice()` registers the instance into Spider Mesh when the class is constructed.

`@BeforeMicroserviceOnline()` marks async setup methods that must complete before the service is exposed in local metadata.

### 2. Create the runtime

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { AppDiscoveryTransporter } from './AppDiscoveryTransporter.js'
import { AppRpcTransporter } from './AppRpcTransporter.js'
import { AppPubsubTransporter } from './AppPubsubTransporter.js'

const mesh = new SpiderMesh({
  transporters: [
    AppDiscoveryTransporter,
    AppRpcTransporter,
    AppPubsubTransporter,
  ],
})
```

These transporter class names are application-local examples, not exports from `@spider-mesh/core`.

Transporter capability is inferred by method shape:

- `send()` means RPC transporter
- `publish()` means pubsub transporter
- `broadcast()` means discovery transporter

You can pass either transporter classes or already-created transporter instances into `transporters`.

### 2a. Use companion transport packages

Spider Mesh can work with companion transport packages such as `@spider-mesh/tcp` and `@spider-mesh/ws`.

The root package entry exports the runtime-agnostic APIs and shared transporter contracts. Concrete transport implementations can be imported from companion packages when needed.

Create a runtime using the TCP package:

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '@spider-mesh/tcp'

const mesh = new SpiderMesh({
  transporters: [
    new UdpDiscovery(),
    new Http2Rpc(),
    new Http2Pubsub(),
  ],
})
```

Or use the WebSocket package:

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws'

const transporter = new WebsocketTransporter({
  heartbeatIntervalMs: 5000,
  reconnectIntervalMs: 1000,
})

transporter.connect('ws://127.0.0.1:8787')

const mesh = new SpiderMesh({
  transporters: [transporter],
})
```

Both patterns keep service/runtime code in `@spider-mesh/core` while delegating concrete transport behavior to a companion package.

### 2b. Minimal working startup order

When using a discovery-based transport package, the canonical startup order is:

1. Start one or more provider processes that register local `@Microservice()` classes.
2. Start client processes.
3. Wait for discovery to converge.
4. In clients, call `await remote.wait()` before the first remote method call.

If an AI agent needs one default operational pattern, use this startup order.

### 3. Call a remote service

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
console.log(user.name)
```

Remote methods are exposed through a typed Proxy.

## RPC Behavior

### Awaitable observable calls

Remote method calls return an RxJS observable that is also awaitable.

```ts
const user = await users.getUser('42')
```

```ts
users.getUser('42').subscribe(user => {
  console.log(user)
})
```

### Stream-first RPC

Spider Mesh treats every RPC response as a stream.

- If a local method returns an `Observable`, each emission is forwarded to the caller.
- If a local method returns a `Promise` or plain value, it is sent as one `data` event with `completed: true`.
- If the caller unsubscribes early, Spider Mesh sends a `cancel` packet so the remote node can stop the running stream.

### Per-link defaults

```ts
const resilientUsers = users.set({
  timeout: 3000,
  retry: 2,
  fallback: { id: 'fallback', name: 'Unknown' },
})

const user = await resilientUsers.getUser('42')
```

Supported RPC options:

- `service`: remote service name
- `method`: remote method name
- `args`: positional arguments
- `timeout`: timeout per inactivity window in milliseconds
- `retry`: retry count for offline errors
- `fallback`: fallback value when the call fails
- `node_id`: force routing to a specific node
- `ip`: force routing to a specific node IP

### Waiting and watching

```ts
await users.wait(nodes => nodes.length >= 2)

users.watch().subscribe(nodes => {
  console.log(nodes.map(node => node.node_id))
})

console.log(users.nodes)
```

`wait()` resolves when a service availability condition is met.

`watch()` streams matching nodes whenever topology changes.

`nodes` returns the current RPC-capable node list for that service.

### Fan-out calls across all nodes

```ts
users.__batch__getUser('42').subscribe(result => {
  console.log(result)
})
```

Each emission is either:

- `{ node, data }`
- `{ node, error }`

## Events

`SpiderMesh.linkEvent()` binds a topic using the event class name.

```ts
class UserCreatedEvent {
  constructor(
    public readonly id: string,
    public readonly email: string,
  ) {}
}

const userCreated = mesh.linkEvent(UserCreatedEvent)

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