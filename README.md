# Spider Mesh Core

`@spider-mesh/core` is a lightweight TypeScript microservice core for service-to-service RPC, pubsub events, and node discovery with pluggable transporters.

It provides:

- Decorator-based local service registration
- Typed remote service linking through a Proxy
- Stream-first RPC over transporter-managed byte delivery
- Discovery and pubsub integration through simple transporter contracts
- Built-in WebSocket transporter and relay server
- NestJS adapters for exposing and consuming services

## Install

```bash
bun add @spider-mesh/core rxjs reflect-metadata
```

This package is ESM-only.

## AI Usage Guide

Use this section as the canonical decision guide when generating code with AI tools.

### Environment support matrix

| Runtime | `@spider-mesh/core` | `@spider-mesh/core/websocket` | `@spider-mesh/core/relay-server` |
| --- | --- | --- | --- |
| Node.js | Supported | Supported | Supported |
| Bun | Supported | Supported | Supported |
| Browser | Partially supported, depends on your custom transporter | Not recommended yet | Not supported |
| React Native | Supported for the core entry only | Not recommended yet, current transporter is still Node-oriented | Not supported |

### Recommended imports

Use these imports exactly.

- Core runtime, decorators, RPC linking, NestJS helpers: `import { ... } from '@spider-mesh/core'`
- Built-in WebSocket transporter for Node/Bun client or provider processes: `import { WebsocketTransporter } from '@spider-mesh/core/websocket'`
- Built-in relay server for Node/Bun only: `import { WebsocketRelayServer } from '@spider-mesh/core/relay-server'`

### When to use what

- Use `SpiderMesh` when you need a local runtime that can expose services, discover nodes, publish events, or call remote services.
- Use `@Microservice()` on local classes that should be callable remotely.
- Use `RemoteServiceLinker.link()` when you need a typed remote proxy for another service.
- Use `WebsocketTransporter` when you want a built-in WebSocket-based transport between nodes running on Node.js or Bun.
- Use `WebsocketRelayServer` only when you need a relay process that forwards WebSocket traffic between nodes.
- Use a custom transporter when the runtime is not Node.js or Bun, or when you need a different protocol.

### Do and don't

Do:

- Import runtime-agnostic APIs from `@spider-mesh/core`.
- Import WebSocket-specific APIs only from `@spider-mesh/core/websocket` or `@spider-mesh/core/relay-server`.
- Start the relay server before providers and clients when using the built-in WebSocket transport.
- Wait for remote service availability with `wait()` before making calls in startup flows.
- Treat the files under `examples/` as canonical usage references.

Don't:

- Do not import `WebsocketRelayServer` from `@spider-mesh/core`.
- Do not use `@spider-mesh/core/relay-server` in React Native or browser code.
- Do not assume the built-in WebSocket transporter is React Native-ready.
- Do not instantiate a relay server inside a mobile app process.
- Do not call remote services before the target service has been discovered unless you intentionally rely on retry behavior.

### Canonical recipes

Provider recipe:

1. Import `Microservice` and `SpiderMesh` from `@spider-mesh/core`.
2. Import `WebsocketTransporter` from `@spider-mesh/core/websocket`.
3. Define a class and decorate it with `@Microservice()`.
4. Instantiate the service class.
5. Create `new SpiderMesh({ transporters: [transporter] })`.

Client recipe:

1. Import `SpiderMesh` and `RemoteServiceLinker` from `@spider-mesh/core`.
2. Import `WebsocketTransporter` from `@spider-mesh/core/websocket`.
3. Create `new SpiderMesh({ transporters: [transporter] })`.
4. Create a typed remote proxy with `RemoteServiceLinker.link()`.
5. Call `await proxy.wait()` before the first RPC call.

Relay server recipe:

1. Import `WebsocketRelayServer` from `@spider-mesh/core/relay-server`.
2. Run it in a dedicated Node.js or Bun process.
3. Point all providers and clients to the relay WebSocket URL.

### AI-safe assumptions

An AI agent should assume the following unless the codebase says otherwise:

- The root package entry is the safe default for shared runtime APIs.
- WebSocket support is opt-in through subpath imports.
- The relay server is a server-side process, not an app-side helper.
- The built-in transporter is currently best suited to Node.js and Bun runtimes.
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
import { MdnsDiscoveryTransporter } from './transporters/MdnsDiscoveryTransporter.js'
import { RedisRpcTransporter } from './transporters/RedisRpcTransporter.js'
import { RedisPubsubTransporter } from './transporters/RedisPubsubTransporter.js'

const mesh = new SpiderMesh({
  transporters: [
    MdnsDiscoveryTransporter,
    RedisRpcTransporter,
    RedisPubsubTransporter,
  ],
})
```

Transporter capability is inferred by method shape:

- `send()` means RPC transporter
- `publish()` means pubsub transporter
- `broadcast()` means discovery transporter

You can pass either transporter classes or already-created transporter instances into `transporters`.

### 2a. Use the built-in WebSocket transporter

`@spider-mesh/core` ships with a built-in WebSocket transporter pair:

- `WebsocketRelayServer`: relay server for connected nodes
- `WebsocketTransporter`: RPC + discovery + pubsub transporter for each node

The root package entry exports only the runtime-agnostic core APIs. WebSocket-specific modules are exposed through subpath imports so React Native apps do not pull Node-only `ws` code from the root entry.

Start a relay server:

```ts
import { WebsocketRelayServer } from '@spider-mesh/core/relay-server'

const server = new WebsocketRelayServer({
  host: '127.0.0.1',
  port: 8787,
})

console.log(`WebSocket relay listening on ws://127.0.0.1:${server.port}`)
```

Create a provider node:

```ts
import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/core/websocket'

const transporter = new WebsocketTransporter('ws://127.0.0.1:8787', {
  heartbeatIntervalMs: 5000,
  reconnectIntervalMs: 1000,
})

@Microservice()
class GreetingService {
  async hello(name: string) {
    return `hello ${name}`
  }
}

new GreetingService()
new SpiderMesh({ transporters: [transporter] })
```

Create a client node:

```ts
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/core/websocket'

const transporter = new WebsocketTransporter('ws://127.0.0.1:8787', {
  heartbeatIntervalMs: 5000,
  reconnectIntervalMs: 1000,
})

type GreetingService = {
  hello(name: string): Promise<string>
}

const mesh = new SpiderMesh({ transporters: [transporter] })
const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
  service: 'GreetingService',
  timeout: 5000,
})

await greeter.wait()
console.log(await greeter.hello('websocket'))
```

The built-in WebSocket transporter is the default bundled transporter in this package. If you do not need Redis, NATS, or another custom transport, you can use it directly.

### 2b. Minimal working startup order

When using the built-in WebSocket transport, the canonical startup order is:

1. Start `WebsocketRelayServer` in a dedicated Node.js or Bun process.
2. Start one or more provider processes that register local `@Microservice()` classes.
3. Start client processes.
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
import { MdnsDiscoveryTransporter } from './transporters/MdnsDiscoveryTransporter.js'
import { RedisRpcTransporter } from './transporters/RedisRpcTransporter.js'
import { RedisPubsubTransporter } from './transporters/RedisPubsubTransporter.js'

@Module({
  providers: [
    SpiderMesh.asProvider({
      transporters: [
        MdnsDiscoveryTransporter,
        RedisRpcTransporter,
        RedisPubsubTransporter,
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

This package includes a built-in WebSocket transporter via `@spider-mesh/core/websocket` and a relay server via `@spider-mesh/core/relay-server`.

You can also provide your own classes that implement one or more transporter contracts exported by `@spider-mesh/core`.

### Public API map

- `SpiderMesh`: runtime coordinator for services, events, discovery, and transporters
- `RemoteServiceLinker.link()`: creates a typed remote proxy
- `@Microservice()`: exposes a local class instance as a remote service
- `SpiderMesh.linkEvent()`: creates a topic binding for publish and subscribe
- `WebsocketTransporter`: built-in Node/Bun WebSocket transporter
- `WebsocketRelayServer`: built-in Node/Bun relay server process

### RPC transporter

```ts
type RpcTransporter = Observable<RpcEvent> & {
  send(data: RpcPacket, node: SpiderMeshNode): Promise<void>
}
```

The RPC observable can emit:

- `message`: inbound binary payload with the source node attached
- `offline`: node offline event
- `metadata`: transporter metadata to attach to the current node

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
type DiscoveryTransporter = Observable<SpiderMeshNode> & {
  broadcast<T extends NodeMetadata>(
    data: MdnsMessage<T>,
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
- `randomUUID()` for Node.js, browser, and React Native compatible UUID generation
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
- The root package entry intentionally excludes Node-only WebSocket exports so AI tools and mobile runtimes do not pull server-side code by default.
- If an AI agent is uncertain which import to use, prefer `@spider-mesh/core` first, then opt into `@spider-mesh/core/websocket` or `@spider-mesh/core/relay-server` only when the runtime is Node.js or Bun.