# Spider Mesh Core Agent Guide

Use this file as the canonical implementation guide for this repository.

## Package Intent

`@spider-mesh/core` owns:

- local microservice registration
- remote service linking
- RPC, pubsub, and discovery contracts
- remote peer registry and RPC routing
- NestJS helper adapters

Concrete transport implementations belong in companion packages or custom transporters.

## Canonical Runtime API

### Runtime creation

```ts
import { Registry, SpiderMesh } from '@spider-mesh/core'

const registry = new Registry()
const mesh = new SpiderMesh(registry)
```

`new SpiderMesh()` is also valid when no registry-backed remote routing is needed.

### Transporter registration

```ts
mesh.registerTransporter(new MyDiscoveryTransporter())
mesh.registerTransporter(new MyRpcTransporter())
mesh.registerTransporter(new MyPubsubTransporter())
```

Register transporter instances.

Transport capability is inferred by instance shape:

- `send()` => RPC transporter
- `publish()` => pubsub transporter
- `broadcast()` => discovery transporter

### Local services

```ts
import { BeforeMicroserviceOnline, Microservice } from '@spider-mesh/core'

@Microservice({ version: '1.0.0' })
class UserService {
  @BeforeMicroserviceOnline()
  async warmup() {}
}

new UserService()
```

`@Microservice()` emits the instance into `LOCAL_SERVICES$`.

### Remote clients

```ts
import { RemoteServiceLinker } from '@spider-mesh/core'

const users = RemoteServiceLinker.link<UserServiceContract>(mesh, {
  service: 'UserService',
})

await users.wait()
const user = await users.getUser('42')
```

## RpcOptions Contract

Use this shape:

```ts
type RpcOptions<T = any> = {
  service: string
  method: string
  args: any[]
  fallback?: T
  timeout?: number
  retry?: number
  node_id?: string
  transporter?: TransporterSelector
}

type TransporterSelector = string | { name?: string } | (abstract new (...args: any[]) => any)
```

`transporter` may be a registered transporter name string, a class constructor (e.g. `Http2Rpc`), or an object with a `name` property.

## Registry Scope

`Registry` manages remote peer and RPC routing state.

Public methods:

- `getPeer(nodeId)`
- `upsertPeer(node)`
- `removePeer(nodeId)`
- `listPeers(service?)`
- `watch(service?)`
- `pickRpcNode(service, { node_id? })`
- `getRpcTransporterName(service)`
- `listTopicNodes(topic)`

## Discovery Semantics

Discovery transporters work with full node announcements.

Current `SpiderMesh` behavior:

- local node metadata is stored in an internal `BehaviorSubject`
- discovery transporters receive rebroadcasts whenever local metadata changes
- discovery input is normalized into `Registry.upsertPeer(...)`
- matching RPC transporter names are normalized into `transporters.rpc`

`SpiderMeshNode` currently includes:

- `host`
- `namespace`
- `version`
- `node_id`
- `topics`
- `services`
- `nodes`
- `transporters`

## Event Semantics

Use `SpiderMesh.linkEvent(EventClass)`.

Current rules:

- topic name is `EventClass.name`
- `publish()` sends through all registered pubsub transporters
- `listen()` returns a shared RxJS stream
- first local subscriber adds the topic to local node metadata
- last unsubscribe removes the topic from local node metadata

## NestJS Helpers

Current helpers:

- `NestJSExposeMicroservice(factory, metadata?)`
- `NestJSLinkMicroservice(factory, transporter?)`
- `NestJSLinkEvent(factory)`

`NestJSLinkMicroservice(factory, transporter?)` forwards the optional transporter selector into the linked remote client.

## Package Usage Playbook For Agents

When an AI agent needs to show or generate usage of this package, prefer these patterns.

### 1. Create a runtime

Use `new SpiderMesh(new Registry())` when the example needs remote discovery, routing, or remote service watching.

Use `new SpiderMesh()` only for local-only or single-process examples that do not depend on registry-backed peer state.

### 2. Register transporters

Always register transporter instances with `mesh.registerTransporter(...)`.

Do not register classes or constructors.

If the example needs concrete networking, import transporters from a companion package such as `@spider-mesh/tcp`.

### 3. Expose a local service

Use `@Microservice()` on the class and instantiate the class.

If startup work is needed before the service should be considered online, use `@BeforeMicroserviceOnline()` on an async method.

### 4. Link a remote service

Use `RemoteServiceLinker.link(mesh, { service: 'ServiceName' })`.

Call `await remote.wait()` before the first remote call when the example depends on discovery.

Remote proxy methods are observable-backed and can be either subscribed to or awaited.

### 5. Force a transporter only when needed

Prefer letting the runtime resolve the RPC transporter automatically.

Only pass `transporter` when the example or feature explicitly needs a specific registered transporter.

Valid forms are a transporter name string or an object/class with a `name`.

### 6. Use events

Use `mesh.linkEvent(EventClass)`.

Topic identity is `EventClass.name`.

If an example subscribes locally, remember that the local topic is added on first subscribe and removed on last unsubscribe.

### 7. Use NestJS helpers

Provide `SpiderMesh` from a normal NestJS provider factory.

Use `NestJSExposeMicroservice(ServiceClass, metadata?)` to expose a local provider.

Use `NestJSLinkMicroservice(ServiceClass)` by default, and pass the optional transporter only when a specific RPC transporter must be selected.

Use `NestJSLinkEvent(EventClass)` to inject an event binding.

### 8. Prefer these example shapes in generated docs

- runtime setup with `Registry`, `SpiderMesh`, and `mesh.registerTransporter(...)`
- service exposure with `@Microservice()` and `new ServiceClass()`
- remote linking with `RemoteServiceLinker.link(...)`
- direct RPC examples with `firstValueFrom(mesh.callRemoteService(...))`
- ESM imports and emitted `.js` relative specifiers inside repository source

## Source-Of-Truth Files

Prefer these files when answering questions or generating code:

- `src/SpiderMesh.ts`
- `src/Registry.ts`
- `src/RemoteService.ts`
- `src/types.ts`
- `src/decorators/Microservice.ts`

For behavior examples, prefer:

- `tests/mock-e2e.test.ts`
- `tests/process-e2e.test.ts`

## Implementation Notes For Agents

- keep repository source as ESM with `.js` relative specifiers
- prefer `@spider-mesh/core` for runtime-agnostic imports
- add `@spider-mesh/tcp` or `@spider-mesh/ws` only when concrete transport is explicitly needed
- treat `src/types.ts` as the transporter contract source of truth
- document runtime setup with `Registry`, `SpiderMesh`, and `mesh.registerTransporter(...)`

## Transporter Contract

The RPC transporter contract (source of truth: `src/types.ts`):

```ts
type RpcTransporter = Observable<RpcEvent> & {
  linkRegistry?(registry: Registry): void
  send(data: RpcRequestPacket | RpcResponsePacket): Promise<{ cancel: () => void }>
}
```

Key points for implementing a custom transporter:

- `send()` must return `Promise<{ cancel: () => void }>`.
- `RpcCancelPacket` is **not** passed to `send()` directly. For `request` packets the returned `cancel()` function is responsible for delivering the cancel signal through the underlying transport. The provider unsubscribes from its running Observable when it receives the cancel.
- For `response` packets: return `{ cancel: () => {} }` (no-op).
- `destination_node_id` in the packet identifies the target node. Fall back to service-based routing when it is absent on `request` packets.
- Emit `{ offline: node_id }` into the Subject when a connection to a node closes unexpectedly. `SpiderMesh` calls `registry.removePeer(node_id)` in response.

## Known Architectural Constraint

`LOCAL_SERVICES$` is process-global inside one process.
