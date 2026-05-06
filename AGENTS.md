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
  transporter?: string | { name?: string }
}
```

`transporter` may be a registered transporter name or a class/object with a `name`.

## Registry Scope

`Registry` manages remote peer and RPC routing state.

Public methods:

- `getPeer(nodeId)`
- `upsertPeer(node)`
- `removePeer(nodeId)`
- `listPeers({ service? })`
- `watch(service?)`
- `pickRpcNode(service, { node_id? })`
- `getRpcTransporterName(service, { node_id? })`
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
- `NestJSLinkMicroservice(factory, transporter)`
- `NestJSLinkEvent(factory)`

`NestJSLinkMicroservice(factory, transporter)` forwards the transporter selector into the linked remote client.

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

## Known Architectural Constraint

`LOCAL_SERVICES$` is process-global inside one process.
