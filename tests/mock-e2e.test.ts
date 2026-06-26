import { describe, expect, test } from 'bun:test'
import { firstValueFrom, from, Observable, Subject, toArray } from 'rxjs'
import { Registry } from '../src/Registry.js'
import { RemoteServiceLinker } from '../src/RemoteService.js'
import { SpiderMesh } from '../src/SpiderMesh.js'
import { NestJSLinkMicroservice } from '../src/decorators/NestJSLinkMicroservice.js'
import { LOCAL_SERVICES$ } from '../src/decorators/Microservice.js'
import type {
    DiscoveryEvent,
    DiscoveryTransporter,
    MdnsMessage,
    PubsubTransporter,
    RpcEvent,
    RpcCancelPacket,
    RpcRequestPacket,
    RpcResponsePacket,
    RpcTransporter,
    ServiceDirectory,
    SpiderMeshNode,
} from '../src/types.js'

let sequence = 0

const nextName = (prefix: string) => `${prefix}${++sequence}`

const cloneNode = (node: SpiderMeshNode): SpiderMeshNode => ({
    ...node,
    topics: [...node.topics],
    services: { ...node.services },
    nodes: { ...node.nodes },
    transporters: { ...node.transporters },
})

class NamedLoopbackRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
    constructor(private readonly localNodeId: string) {
        super()
    }

    async send(data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket, node_id?: string) {
        const targetNodeId = node_id || this.localNodeId
        this.next({
            rpc: 'sender_node_id' in data ? { ...data, sender_node_id: targetNodeId } : data,
        })
        return { cancel: () => {} }
    }

    canRoute() {
        return true
    }
}

class SilentRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
    async send(_data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket, _node_id?: string) {
        return { cancel: () => {} }
    }

    canRoute() {
        return true
    }
}

// A transporter that swallows requests (never replies) and declares it cannot route.
// Stands in for an Http2Rpc with no LAN peer for a relay-only provider.
class UnreachableRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
    sent = 0
    async send(_data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket, _node_id?: string) {
        this.sent++
        return { cancel: () => {} }
    }
    canRoute() {
        return false
    }
}

// Loopback transporter that declares it can route (sees the provider).
class RoutableLoopbackRpcTransporter extends NamedLoopbackRpcTransporter {
    canRoute() {
        return true
    }
}

class MockPubsubTransporter extends Subject<{ endpoints: Record<string, string | boolean | number> }> implements PubsubTransporter {
    #topics = new Map<string, Subject<unknown>>()

    constructor() {
        super()
    }

    async publish<T>(topic: string, data: T) {
        this.#getTopic(topic).next(data)
    }

    listen<T>(topic: string): Observable<T> {
        return this.#getTopic(topic) as Subject<T>
    }

    #getTopic(topic: string) {
        if (!this.#topics.has(topic)) {
            this.#topics.set(topic, new Subject())
        }

        return this.#topics.get(topic)!
    }
}

class MockDiscoveryTransporter extends Subject<DiscoveryEvent> implements DiscoveryTransporter {
    broadcasts: Array<MdnsMessage<SpiderMeshNode>> = []

    async broadcast(data: MdnsMessage<any>) {
        this.broadcasts.push({
            ...data,
            node: cloneNode(data.node as SpiderMeshNode),
        })
    }
}

// Discovery transporter that owns its own peer table and exposes it as a
// ServiceDirectory — mirrors how real transporters (tcp/ws) supply availability
// to core now that core no longer holds a registry.
class MockDirectoryTransporter extends Subject<DiscoveryEvent> implements DiscoveryTransporter, ServiceDirectory {
    #registry = new Registry()

    async broadcast(_data: MdnsMessage<any>) {}

    watchService(service: string) {
        return this.#registry.watch(service)
    }

    listNodes(service: string) {
        return this.#registry.listPeers(service)
    }

    upsertPeer(node: SpiderMeshNode) {
        return this.#registry.upsertPeer(node)
    }
}

describe('mock e2e', () => {
    test('routes rpc through explicit transporter name and class', async () => {
        const mesh = new SpiderMesh()
        const silent = new SilentRpcTransporter()
        const loopback = new NamedLoopbackRpcTransporter(mesh.node_id)
        const serviceName = nextName('EchoService')

        mesh.registerTransporter(silent, 'SilentRpcTransporter')
        mesh.registerTransporter(loopback)

        LOCAL_SERVICES$.next({
            name: serviceName,
            metadata: {},
            instance: {
                echo(value: string) {
                    return value
                },
                stream() {
                    return from([1, 2, 3])
                },
            },
        })

        const byName = await firstValueFrom(mesh.callRemoteService<string, never>({
            service: serviceName,
            method: 'echo',
            args: ['by-name'],
            transporter: 'NamedLoopbackRpcTransporter',
        }))

        const byClass = await firstValueFrom(mesh.callRemoteService<string, never>({
            service: serviceName,
            method: 'echo',
            args: ['by-class'],
            transporter: NamedLoopbackRpcTransporter,
        }))

        const streamValues = await firstValueFrom(mesh.callRemoteService<number, never>({
            service: serviceName,
            method: 'stream',
            args: [],
            transporter: NamedLoopbackRpcTransporter,
        }).pipe(toArray()))

        expect(byName).toBe('by-name')
        expect(byClass).toBe('by-class')
        expect(streamValues).toEqual([1, 2, 3])
    })

    test('wait supports async checker functions', async () => {
        const mesh = new SpiderMesh()
        const directory = new MockDirectoryTransporter()
        mesh.registerTransporter(directory, 'MockDirectoryTransporter')
        const serviceName = nextName('AsyncWaitService')
        const remote = RemoteServiceLinker.link<{ ping(): Promise<string> }>(mesh, { service: serviceName })

        const waitPromise = remote.wait(async nodes => {
            await Promise.resolve()
            return nodes.length > 0
        })

        await Promise.resolve()

        directory.upsertPeer({
            host: '127.0.0.1',
            namespace: mesh.namespace,
            node_id: nextName('peer'),
            topics: [],
            services: {
                [serviceName]: {},
            },
            nodes: {},
            transporters: {
                rpc: 'NamedLoopbackRpcTransporter',
            },
            version: 1,
        })

        const nodes = await waitPromise

        expect(nodes).not.toBeNull()
        expect(nodes).toHaveLength(1)
        expect(nodes?.[0]?.services?.[serviceName]).toEqual({})
    })

    test('keeps topic metadata when discovery registers after listen and clears it on unsubscribe', async () => {
        class TopicEvent {}

        const mesh = new SpiderMesh()
        const pubsub = new MockPubsubTransporter()
        const discovery = new MockDiscoveryTransporter()
        const event = mesh.linkEvent(TopicEvent)

        mesh.registerTransporter(pubsub, 'MockPubsubTransporter')

        const subscription = event.listen().subscribe(() => undefined)

        mesh.registerTransporter(discovery, 'MockDiscoveryTransporter')

        expect(discovery.broadcasts.at(-1)?.node.topics).toContain('TopicEvent')

        subscription.unsubscribe()

        expect(discovery.broadcasts.at(-1)?.node.topics).not.toContain('TopicEvent')
    })

    test('NestJSLinkMicroservice links a remote service without requiring transporter', async () => {
        class BillingService {
            charge(orderId: string): Promise<{ orderId: string }> {
                throw new Error('typing only')
            }
        }

        const mesh = new SpiderMesh()
        const loopback = new NamedLoopbackRpcTransporter(mesh.node_id)

        mesh.registerTransporter(loopback)

        LOCAL_SERVICES$.next({
            name: BillingService.name,
            metadata: {},
            instance: {
                charge(orderId: string) {
                    return { orderId }
                },
            },
        })

        const provider = NestJSLinkMicroservice(BillingService)
        const remote = provider.useFactory(mesh) as unknown as Pick<BillingService, 'charge'>

        const result = await remote.charge('order-1')

        expect(provider.provide).toBe(BillingService)
        expect(provider.inject).toEqual([SpiderMesh])
        expect(result).toEqual({ orderId: 'order-1' })
    })

    test('listRpcNodes and __batch__ are honestly empty without a ServiceDirectory', async () => {
        const mesh = new SpiderMesh()
        // An RPC transporter that is NOT a ServiceDirectory — core has no availability source.
        const loopback = new NamedLoopbackRpcTransporter(mesh.node_id)
        mesh.registerTransporter(loopback)

        expect(mesh.listRpcNodes('AnyService')).toEqual([])

        const remote = RemoteServiceLinker.link<{ ping(): Promise<string> }>(mesh, { service: 'AnyService' })
        const batched = await firstValueFrom(
            (remote as any).__batch__ping().pipe(toArray())
        )
        expect(batched).toEqual([])
    })

    test('pickRpcNode skips peers rejected by the filter (RPC-not-ready)', () => {
        const registry = new Registry()
        const service = nextName('FilterService')
        const base = {
            namespace: 'test',
            host: '127.0.0.1',
            topics: [] as string[],
            nodes: {},
            version: 1,
            services: { [service]: {} },
        }

        // A ready provider (has an Http2Rpc endpoint port) and a half-formed one (no port yet).
        registry.upsertPeer({ ...base, node_id: 'ready', transporters: { Http2Rpc: { port: 5000 } } })
        registry.upsertPeer({ ...base, node_id: 'half', transporters: { Http2Rpc: true } })

        const isReady = (node: SpiderMeshNode) => Number((node.transporters?.Http2Rpc as { port?: number } | undefined)?.port) > 0

        const picks = new Set<string | null | undefined>()
        for (let i = 0; i < 25; i++) {
            picks.add(registry.pickRpcNode(service, { filter: isReady }))
        }

        // Round-robin must never land on the half-formed peer.
        expect([...picks]).toEqual(['ready'])
    })

    // LOCAL_SERVICES$ is an unbounded ReplaySubject: a freshly constructed mesh replays every
    // previously-emitted service into its concurrency-1 registration queue, so a service emitted
    // afterwards is registered a few microtasks later. A single macrotask hop drains that backlog
    // deterministically, making these loopback tests order-independent. (Kept last so they don't
    // lengthen the replay backlog seen by earlier tests.)
    const flushLocalServiceRegistration = () => new Promise<void>(resolve => setTimeout(resolve, 0))

    test('routes rpc through a reachable transporter instead of the first-registered one', async () => {
        const mesh = new SpiderMesh()
        const unreachable = new UnreachableRpcTransporter()
        const loopback = new RoutableLoopbackRpcTransporter(mesh.node_id)
        const serviceName = nextName('ReachService')

        // The bug scenario: first-registered transporter cannot route; the second one can.
        mesh.registerTransporter(unreachable, 'UnreachableRpcTransporter')
        mesh.registerTransporter(loopback, 'RoutableLoopbackRpcTransporter')

        LOCAL_SERVICES$.next({
            name: serviceName,
            metadata: {},
            instance: {
                echo(value: string) {
                    return value
                },
            },
        })
        await flushLocalServiceRegistration()

        const result = await firstValueFrom(mesh.callRemoteService<string, never>({
            service: serviceName,
            method: 'echo',
            args: ['reach'],
        }))

        expect(result).toBe('reach')
        // The unroutable (first-registered) transporter was never dispatched to.
        expect(unreachable.sent).toBe(0)
    })

    test('falls back to the sole RPC transporter even when canRoute reports no route yet', async () => {
        const mesh = new SpiderMesh()
        const serviceName = nextName('SoleService')
        // canRoute=false (e.g. provider mid-startup) but it is the only transporter → still used.
        class NotReadyLoopback extends NamedLoopbackRpcTransporter {
            canRoute() {
                return false
            }
        }
        const loopback = new NotReadyLoopback(mesh.node_id)
        mesh.registerTransporter(loopback, 'NotReadyLoopback')

        LOCAL_SERVICES$.next({
            name: serviceName,
            metadata: {},
            instance: {
                echo(value: string) {
                    return value
                },
            },
        })
        await flushLocalServiceRegistration()

        const result = await firstValueFrom(mesh.callRemoteService<string, never>({
            service: serviceName,
            method: 'echo',
            args: ['ok'],
        }))

        expect(result).toBe('ok')
    })
})