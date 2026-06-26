# @spider-mesh/core — Architecture

Internal structure of `@spider-mesh/core`, for maintainers and contributors.
For *how to use* the package, see [README.md](README.md). For build/test/conventions, see [AGENTS.md](AGENTS.md).

The whole package is runtime-agnostic: it has **no socket, UDP, or HTTP code**. It defines the mesh model (services, peers, routing, events) and the *contracts* a transport must satisfy. Concrete transports live in `@spider-mesh/tcp`, `@spider-mesh/ws`, or user code.

## Module Map

```
src/
├── index.ts                 # public API barrel (values + type re-exports)
├── types.ts                 # contract source of truth: packets, transporter shapes, node shape, error codes
├── SpiderMesh.ts            # runtime: local node state, transporter wiring, RPC lifecycle, events
├── Registry.ts              # remote peer + topic + RPC-routing state
├── RemoteService.ts         # RemoteServiceLinker + the typed remote proxy
├── decorators/
│   ├── Microservice.ts                 # @Microservice() + LOCAL_SERVICES$ stream (+ MicroserviceList)
│   ├── BeforeMicroserviceOnline.ts     # @BeforeMicroserviceOnline() warmup hook
│   ├── LimitConcurrentRunning.ts       # alias of LimitConcurrency (method throttle)
│   ├── NestJSExposeMicroservice.ts     # DI factory: expose a Nest provider as a microservice
│   ├── NestJSLinkMicroservice.ts       # DI factory: inject a remote proxy
│   └── NestJSLinkEvent.ts              # DI factory: inject an event binding
└── helpers/
    ├── LimitConcurrency.ts             # concurrency limiter used by the decorators
    ├── MicroserviceException.ts        # MicroserviceError TYPE (no runtime value)
    └── sleep.ts
```

### What `index.ts` exports

- **Values:** `SpiderMesh`, `Registry`, `RemoteServiceLinker`, `Microservice`, `BeforeMicroserviceOnline`, `LimitConcurrency`, `LimitConcurrentRunning`, `NestJSExposeMicroservice`, `NestJSLinkMicroservice`, `NestJSLinkEvent`.
- **Types only:** `RpcOptions`, `RpcTransporter`, `PubsubTransporter`, `DiscoveryTransporter`, `RpcRequestPacket`, `RpcResponsePacket`, `RpcCancelPacket`, `RpcEvent`, `PubsubEvent`, `DiscoveryEvent`, `MeshTransporter`, `TransporterSelector`, `SpiderMeshNode`, `NodeMetadata`, `MdnsMessage`, `SpiderMeshError`, `SpiderMeshErrorCode`, `MicroserviceError`, plus the `RemoteService.ts` helper types.
- **NOT re-exported (internal):** `LOCAL_SERVICES$` and `MicroserviceList` live in `decorators/Microservice.ts` and are intentionally module-private.

## Core Data Structures

### `SpiderMeshNode` (the unit of discovery)

A node advertises its full state on every change. Fields: `node_id`, `namespace`, `host`, `version` (monotonic counter, bumped on every local refresh), `services`, `topics`, `transporters` (which RPC transporter name serves which service), and `nodes`/metadata. Discovery transports serialize and rebroadcast this whole object — there are no deltas.

### `LOCAL_SERVICES$` (process-global service stream)

`@Microservice()` pushes `{ name, instance, metadata }` into this RxJS subject **at construction time**. Every `SpiderMesh` in the process subscribes and registers every emitted service. This is the single most important non-obvious constraint — see [Invariants](#invariants--gotchas).

## Control & Data Flow

### Local service registration

```
new UserService()
  └─ @Microservice() decorator → LOCAL_SERVICES$.next({ name: 'UserService', instance, metadata })
        └─ SpiderMesh subscription
              ├─ await every @BeforeMicroserviceOnline() method   (warmup gate)
              ├─ add service to local node metadata
              └─ #refresh(): bump version, rebroadcast node via discovery transporters
```

Service **identity is the class name**. Metadata passed to `@Microservice({...})` is free-form and opaque to routing.

### Outbound RPC (`callRemoteService` / remote proxy)

```
proxy.getUser('42')  ──►  callRemoteService({ service, method, args, ... })
   1. availability gate: firstValueFrom(watchService(service))   (merged ServiceDirectory)
   2. resolve transporter (#selectRpcTransport):
        - explicit RpcOptions.transporter (name | class | {name}), else
        - the first registered RPC transporter whose canRoute(service, node_id) is true, else
        - the first registered RPC transporter (fallback)
   3. transporter.send(RpcRequestPacket) with destination_node_id = RpcOptions.node_id
        → the TRANSPORT picks the actual node (relay round-robin, or its own peer table)
   4. correlate RpcResponsePacket(s) by request_id on the transporter's Observable
   5. apply timeout / retry; surface data or error as an RxJS stream
```

Core selects only the **transporter** — preferring one whose `canRoute` reports a route, so a
default call is not dispatched through a transporter that can't reach the provider — and never the
target node. It delegates node selection to the transport: `@spider-mesh/tcp` round-robins via its
own injected `Registry`; `@spider-mesh/ws` lets the relay pick. Selection re-runs on retry.

The returned value is an **Observable augmented with `then`**, so callers can either `subscribe()` or `await`. `SpiderMesh` owns the entire lifecycle: timeout, retry, completion, and cancellation.

**Cancellation:** when the last subscriber unsubscribes before the stream completes, `SpiderMesh` calls the `cancel()` returned by `send()`. The core contract's `send()` only accepts `request`/`response` packets — turning `cancel()` into an on-the-wire `RpcCancelPacket` is a transport-internal concern.

**Fallback short-circuit:** if `RpcOptions.fallback !== undefined`, any caught error resolves to `of(fallback)` instead of propagating.

### Discovery & availability

Core does **not** ingest or store peers. A discovery transporter that owns a peer table
exposes it to core as a `ServiceDirectory`. `watchService(service)` merges
`directory.watchService(service)` across all such transporters (via `combineLatest`,
de-duped by `node_id`); `listRpcNodes(service)`/`nodes` do the same for snapshots. Each
transport is responsible for filling and evicting its own table (tcp ingests into its
injected `Registry`; ws maintains a node map from relay `hello`/`offline` frames). Core
keeps the outbound self-announce (`broadcast(#me$)`) only.

### Events (pub/sub)

`mesh.linkEvent(EventClass)` returns `{ publish, listen }` keyed on `EventClass.name` as the topic. `publish()` fans out across **all** registered pubsub transporters; `listen()` merges all transporter listeners into one stream. First local subscribe adds the topic to node metadata (and rebroadcasts); last unsubscribe removes it.

## The Three Transporter Contracts (`types.ts`)

Capability is **inferred by instance shape** at `registerTransporter()` time, not declared:

| Method present | Treated as | Contract |
| --- | --- | --- |
| `send()` | RPC | `send(RpcRequestPacket \| RpcResponsePacket) → Promise<{ cancel }>` |
| `publish()` | pubsub | `publish(topic, data)` + `listen(topic): Observable` |
| `broadcast()` | discovery | `broadcast(MdnsMessage<NodeMetadata>)` |
| `watchService()` + `listNodes()` | `ServiceDirectory` | availability for `wait()`/`watch()`/`nodes` |

All are `Observable<…Event>` (the runtime subscribes to them). A single instance can implement more than one contract (that is how `@spider-mesh/ws` serves all from one socket). There is **no `linkRegistry`** — transports that need a registry receive it via their own constructor.

The RPC contract also requires **`canRoute(service, node_id?): boolean`** (part of `send()`'s capability, not a separate one). Core calls it in `#selectRpcTransport` to prefer a transporter that can actually reach the target — see the checklist below.

### Implementing a custom transporter — checklist

- `send()` returns `Promise<{ cancel: () => void }>`; for `response` packets `cancel` is a no-op.
- Implement **`canRoute(service, node_id?)`** (required): return `true` only when you can currently reach a provider of `service` (honor `node_id` when given). Keep it **side-effect free** (e.g. don't advance a round-robin index). If you can't enumerate reachability, `return true` and let `send()` surface the error.
- Route `request` packets by `destination_node_id`; fall back to your own selection when absent (core does not pick the node).
- Implement `ServiceDirectory` (`watchService`/`listNodes`) if you want `wait()`/`watch()`/`nodes` to work. `watchService` must emit current state promptly (BehaviorSubject semantics) — do **not** emit a synthetic empty first.
- Own your peer table: ingest on discovery, evict on disconnect. Receive any `Registry`/state you need via your constructor.
- Deliver the cancel signal through your own wire format inside the `cancel()` closure — do **not** expect `RpcCancelPacket` through `send()`.

## Registry (transport helper)

`Registry` is pure in-memory peer + routing state — no I/O. `SpiderMesh` no longer owns or depends on it; it is a **helper for client-side-routing transports** (e.g. tcp constructs one and injects it into its three transporters). Public surface: `getPeer`, `upsertPeer`, `removePeer`, `listPeers(service?)`, `watch(service?)`, `pickRpcNode(service, { node_id?, filter? })` (round-robin; `filter(node)` excludes peers a transport can't route to yet — e.g. tcp passes one to skip providers whose Http2Rpc port isn't advertised), `getRpcTransporterName(service)`, `listTopicNodes(topic)`, plus the `nodes$` BehaviorSubject. If you change routing strategy (e.g. weighted/affinity), `pickRpcNode` is the seam.

### Extending: platform-native availability (e.g. Kubernetes)

Because availability is just a `ServiceDirectory`, a transport can back it with whatever
the platform already knows. A Kubernetes transport could implement `watchService` from
**EndpointSlice** watches (push) or a **2s DNS/endpoints poll** (simple first cut), with
routing delegated to a K8s `Service` / service mesh — no `Registry` and no app-level
round-robin. Core code is identical across tcp / ws / k8s; only the directory source differs.

## Invariants & Gotchas

- **`LOCAL_SERVICES$` is process-global.** Two `SpiderMesh` instances in one process are NOT isolated — both see every local service. Process-per-node (plus `SPIDERMESH_NAMESPACE`) is the isolation boundary. Any refactor that adds multi-mesh-per-process must replace this global stream.
- **Service & event identity = class name.** Renaming a class is a wire-breaking change. Minification that mangles class names will break routing.
- **`MicroserviceError` is a type, not a value.** Error codes are the `SpiderMeshErrorCode` string union (`MICROSERVICE_OFFLINE`, `MICROSERVICE_NOT_FOUND`, `MICROSERVICE_RPC_TIMEOUT`). There is no class to instantiate.
- **`@Microservice({ version })` is not the node `version`.** The node-level `version` is an internal monotonic counter; the decorator metadata is opaque and ignored by selection.
- **ESM-only, `.js` relative specifiers in source.** Source imports use emitted `.js` paths (`moduleResolution: NodeNext`).

## Source-of-truth files

When in doubt, read in this order: `src/types.ts` (contracts) → `src/SpiderMesh.ts` (lifecycle/routing) → `src/Registry.ts` (peer state) → `src/RemoteService.ts` (proxy) → `src/decorators/Microservice.ts` (registration). Behavior examples: `tests/mock-e2e.test.ts`, `tests/process-e2e.test.ts`.

## Environment Variables

- `SPIDERMESH_NAMESPACE` — node namespace, default `default`.
- `SPIDERMESH_NODE_HOSTNAME` — optional hostname attached to node metadata.
