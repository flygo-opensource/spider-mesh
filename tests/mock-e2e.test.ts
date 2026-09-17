import { describe, expect, test } from 'bun:test'
import { BehaviorSubject, firstValueFrom, from, map, Subject, toArray } from 'rxjs'
import { Registry } from '../src/Registry.js'
import { Topology } from '../src/Topology.js'
import { RemoteServiceLinker } from '../src/RemoteService.js'
import { SpiderMesh } from '../src/SpiderMesh.js'
import { NestJSLinkMicroservice } from '../src/decorators/NestJSLinkMicroservice.js'
import { LOCAL_SERVICES$ } from '../src/decorators/Microservice.js'
import type {
    RpcEvent,
    RpcCancelPacket,
    RpcRequestPacket,
    RpcResponsePacket,
    RpcTransporter,
    SpiderMeshNode,
    TopologyDiscovery,
    TopologyDiscoveryContext,
    TopologyEvent,
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
    public readonly name: string = 'loopback'
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
    public readonly name = 'silent'
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
    public readonly name: string = 'unreachable'
    sent = 0
    async send(_data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket, _node_id?: string) {
        this.sent++
        return { cancel: () => {} }
    }
    canRoute() {
        return false
    }
}

class ProbeOnlyRpcTransporter extends UnreachableRpcTransporter {
    override readonly name = 'probe-only'
    probes = 0

    async probe() {
        this.probes++
        return { reachable: true }
    }
}

class TerminalResponseRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
    public readonly name = 'terminal-response'
    cancelCount = 0

    async send(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
        if (packet.kind === 'request') {
            setTimeout(() => {
                this.next({
                    rpc: {
                        kind: 'response',
                        request_id: packet.request_id,
                        data: 'done',
                        completed: true,
                    },
                })
            }, 0)
        }

        return {
            cancel: () => {
                this.cancelCount++
            },
        }
    }

    canRoute() {
        return true
    }
}

/** Provider trả một stream dài hạn: có data nhưng không bao giờ complete. */
class StreamingRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
    public readonly name = 'streaming'
    cancelCount = 0

    async send(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
        if (packet.kind === 'request') {
            setTimeout(() => {
                this.next({
                    rpc: {
                        kind: 'response',
                        request_id: packet.request_id,
                        sender_node_id: 'provider-node',
                        data: 'tick-1',
                    },
                })
            }, 0)
        }

        return {
            cancel: () => {
                this.cancelCount++
            },
        }
    }

    canRoute() {
        return true
    }
}

/** `send()` resolve chậm, mở đúng cửa sổ mà unsubscribe chạy trước khi có hàm cancel. */
class DeferredCancelRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
    public readonly name = 'deferred-cancel'
    cancelCount = 0

    async send(_packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
        await new Promise(resolve => setTimeout(resolve, 5))
        return {
            cancel: () => {
                this.cancelCount++
            },
        }
    }

    canRoute() {
        return true
    }
}

class VerifyingTopologyDiscovery implements TopologyDiscovery {
    verifyCalls: string[] = []

    constructor(private readonly result: 'alive' | 'dead' | 'unknown') {}

    bind(_context: TopologyDiscoveryContext) {}

    async verify(node_id: string) {
        this.verifyCalls.push(node_id)
        return this.result
    }
}

// Loopback transporter that declares it can route (sees the provider).
class RoutableLoopbackRpcTransporter extends NamedLoopbackRpcTransporter {
    canRoute() {
        return true
    }
}

describe('mock e2e', () => {
    test('topology excludes suspect endpoints and emits recovery events', () => {
        const topology = new Topology()
        const events: TopologyEvent[] = []
        topology.events$.subscribe(event => events.push(event))
        const node: SpiderMeshNode = {
            host: '127.0.0.1', namespace: 'test', version: 1, node_id: 'reachability-node',
            topics: [], services: { ReachabilityService: {} }, nodes: {},
            transporters: { http2: { port: 3000 } },
        }
        topology.upsertRemote(node)

        expect(topology.route({
            service: 'ReachabilityService', transporter: 'http2',
            routing: { strategy: 'round-robin' },
        })?.node.node_id).toBe(node.node_id)

        topology.reportReachability({
            node_id: node.node_id, transporter: 'http2', status: 'suspect', reason: 'connection-lost',
        })
        expect(topology.isReachable(node.node_id, 'http2')).toBe(false)
        expect(topology.route({
            service: 'ReachabilityService', transporter: 'http2',
            routing: { strategy: 'round-robin' },
        })).toBeUndefined()

        topology.reportReachability({
            node_id: node.node_id, transporter: 'http2', status: 'reachable',
        })
        expect(topology.isReachable(node.node_id, 'http2')).toBe(true)
        expect(events.map(event => event.type)).toEqual([
            'node-online', 'endpoint-suspect', 'endpoint-recovered',
        ])
    })

    test('topology removes membership only after discovery verifies the node is dead', async () => {
        const discovery = new VerifyingTopologyDiscovery('dead')
        const topology = new Topology({ discovery })
        const node: SpiderMeshNode = {
            host: '127.0.0.1', namespace: 'test', version: 1, node_id: 'verified-dead-node',
            topics: [], services: { VerifiedService: {} }, nodes: {},
            transporters: { http2: { port: 3001 } },
        }
        topology.upsertRemote(node)

        topology.reportReachability({
            node_id: node.node_id, transporter: 'http2', status: 'unreachable',
        })
        expect(topology.getPeer(node.node_id)).toBeDefined()

        expect(await topology.verifyNode(node.node_id)).toBe('dead')
        expect(discovery.verifyCalls).toEqual([node.node_id])
        expect(topology.getPeer(node.node_id)).toBeUndefined()
    })

    test('does not cancel an awaitable RPC after receiving terminal data', async () => {
        const transporter = new TerminalResponseRpcTransporter()
        const mesh = new SpiderMesh({ transporters: [transporter] })

        const value = await firstValueFrom(mesh.callRemoteService<string, never>({
            service: 'TerminalService',
            method: 'run',
            args: [],
            transporter: transporter.name,
        }))

        expect(value).toBe('done')
        expect(transporter.cancelCount).toBe(0)
    })

    test('errors an in-flight stream when its provider node goes offline', async () => {
        const transporter = new StreamingRpcTransporter()
        const mesh = new SpiderMesh({ transporters: [transporter] })

        const received: string[] = []
        let failure: any

        mesh.callRemoteService<string, never>({
            service: 'StreamService',
            method: 'ticks',
            args: [],
            transporter: transporter.name,
        }).subscribe({
            next: value => received.push(value),
            error: error => { failure = error },
        })

        await new Promise(resolve => setTimeout(resolve, 10))
        expect(received).toEqual(['tick-1'])
        expect(failure).toBeUndefined()

        // Round-robin: node phục vụ chỉ lộ ra qua `sender_node_id` của response đầu tiên.
        transporter.next({ offline: 'provider-node' })

        expect(failure?.code).toBe('MICROSERVICE_OFFLINE')
    })

    test('ignores an offline node that is not serving any in-flight rpc', async () => {
        const transporter = new StreamingRpcTransporter()
        const mesh = new SpiderMesh({ transporters: [transporter] })

        let failure: any
        mesh.callRemoteService<string, never>({
            service: 'StreamService',
            method: 'ticks',
            args: [],
            transporter: transporter.name,
        }).subscribe({ error: error => { failure = error } })

        await new Promise(resolve => setTimeout(resolve, 10))
        transporter.next({ offline: 'some-other-node' })

        expect(failure).toBeUndefined()
    })

    test('sends cancel even when unsubscribe wins the race against send()', async () => {
        const transporter = new DeferredCancelRpcTransporter()
        const mesh = new SpiderMesh({ transporters: [transporter] })

        const subscription = mesh.callRemoteService<string, never>({
            service: 'StreamService',
            method: 'ticks',
            args: [],
            transporter: transporter.name,
        }).subscribe({ error: () => undefined })

        // Cùng một tick: `send()` chưa resolve nên chưa có hàm cancel để gọi.
        subscription.unsubscribe()

        await new Promise(resolve => setTimeout(resolve, 20))
        expect(transporter.cancelCount).toBe(1)
    })

    test('registers constructor transporters by their hardcoded names', () => {
        const transporter = new NamedLoopbackRpcTransporter('constructor-node')
        const mesh = new SpiderMesh({ transporters: [transporter] })

        expect(mesh.localNode.transporters.loopback).toBe(true)
        expect(() => mesh.registerTransporter(new NamedLoopbackRpcTransporter('duplicate')))
            .toThrow('RPC transporter "loopback" is already registered')
    })

    test('topology reuses per-request round-robin state', () => {
        const topology = new Topology()
        const service = nextName('RoutedService')
        const node = (node_id: string): SpiderMeshNode => ({
            host: '127.0.0.1', namespace: 'test', version: 1, node_id,
            topics: [], services: { [service]: {} }, nodes: {},
            transporters: { loopback: true },
        })
        topology.upsertRemote(node('node-a'))
        topology.upsertRemote(node('node-b'))

        const selected = Array.from({ length: 4 }, () => topology.route({
            service,
            transporter: 'loopback',
            routing: { strategy: 'round-robin' },
        })?.node.node_id)

        expect(selected).toEqual(['node-a', 'node-b', 'node-a', 'node-b'])
    })

    test('wait uses transporter probe when Topology is absent', async () => {
        const transporter = new ProbeOnlyRpcTransporter()
        const mesh = new SpiderMesh({ transporters: [transporter] })
        const remote = RemoteServiceLinker.link<{ ping(): Promise<void> }>(mesh, {
            service: 'InfrastructureRoutedService',
        })

        const nodes = await remote.wait()

        expect(nodes).toEqual([])
        expect(transporter.probes).toBe(1)
    })

    test('always generates a fresh random node ID without env or hostname identity', () => {
        const first = new SpiderMesh()
        const second = new SpiderMesh()

        expect(first.node_id).not.toBe(second.node_id)
        expect(first.node_id).not.toBe(process.env.SPIDERMESH_NODE_ID)
        expect(first.node_id).not.toBe(process.env.HOSTNAME)
        expect(first.node_id.length).toBeGreaterThan(20)
    })

    test('registry replaces discovery snapshots and only merges through patchPeer', () => {
        const registry = new Registry()
        const initial: SpiderMeshNode = {
            host: '127.0.0.1',
            namespace: 'test',
            version: 1,
            node_id: 'snapshot-peer',
            topics: ['old-topic'],
            services: { ServiceB: {}, ServiceC: {} },
            nodes: { old: 1 },
            transporters: { Http2Rpc: { port: 31001 }, RemovedTransporter: true },
        }

        registry.upsertPeer(initial)
        registry.upsertPeer({
            ...initial,
            version: 2,
            topics: [],
            services: { ServiceB: {} },
            nodes: {},
            transporters: { Http2Rpc: { port: 31002 } },
        })

        const replaced = registry.getPeer(initial.node_id)!
        expect(replaced.services).toEqual({ ServiceB: {} })
        expect(replaced.topics).toEqual([])
        expect(replaced.nodes).toEqual({})
        expect(replaced.transporters).toEqual({ Http2Rpc: { port: 31002 } })

        registry.upsertPeer({ ...initial, version: 1 })
        expect(registry.getPeer(initial.node_id)?.version).toBe(2)

        registry.patchPeer(initial.node_id, {
            services: { ServiceC: { patched: true } },
            transporters: { ExtraTransporter: true },
        })
        expect(registry.getPeer(initial.node_id)?.services).toEqual({
            ServiceB: {},
            ServiceC: { patched: true },
        })
        expect(registry.getPeer(initial.node_id)?.transporters).toEqual({
            Http2Rpc: { port: 31002 },
            ExtraTransporter: true,
        })
    })

    test('exposes the current local node and subsequent metadata updates', () => {
        const mesh = new SpiderMesh()
        const snapshots: SpiderMeshNode[] = []
        const subscription = mesh.localNode$.subscribe(node => snapshots.push(cloneNode(node)))

        mesh.setLocalTransporterMetadata('ExternalCapability', { port: 4321 })

        expect(mesh.localNode.node_id).toBe(mesh.node_id)
        expect(mesh.localNode.namespace).toBe(mesh.namespace)
        expect(snapshots.at(-1)?.transporters.ExternalCapability).toEqual({ port: 4321 })
        expect(snapshots.at(-1)?.version).toBeGreaterThan(snapshots[0]!.version)
        subscription.unsubscribe()
    })

    test('uses optional topology as the RPC availability source', () => {
        const topology = new Topology()
        const mesh = new SpiderMesh({
            topology,
            transporters: [new NamedLoopbackRpcTransporter('relay-owned-peer')],
        })

        const peer: SpiderMeshNode = {
            host: 'relay-peer',
            namespace: mesh.namespace,
            version: 1,
            node_id: 'relay-owned-peer',
            topics: [],
            services: { RelayOwnedService: {} },
            nodes: {},
            transporters: {},
        }
        topology.upsertRemote(peer)

        expect(mesh.listRpcNodes('RelayOwnedService').map(node => node.node_id)).toEqual([peer.node_id])
    })

    test('routes rpc through the explicit hardcoded transporter name', async () => {
        const mesh = new SpiderMesh()
        const silent = new SilentRpcTransporter()
        const loopback = new NamedLoopbackRpcTransporter(mesh.node_id)
        const serviceName = nextName('EchoService')

        mesh.registerTransporter(silent)
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
            transporter: 'loopback',
        }))

        const byNameAgain = await firstValueFrom(mesh.callRemoteService<string, never>({
            service: serviceName,
            method: 'echo',
            args: ['by-name-again'],
            transporter: 'loopback',
        }))

        const streamValues = await firstValueFrom(mesh.callRemoteService<number, never>({
            service: serviceName,
            method: 'stream',
            args: [],
            transporter: 'loopback',
        }).pipe(toArray()))

        expect(byName).toBe('by-name')
        expect(byNameAgain).toBe('by-name-again')
        expect(streamValues).toEqual([1, 2, 3])
    })

    test('wait supports async checker functions', async () => {
        const topology = new Topology()
        const mesh = new SpiderMesh({
            topology,
            transporters: [new NamedLoopbackRpcTransporter('async-wait-peer')],
        })
        const serviceName = nextName('AsyncWaitService')
        const remote = RemoteServiceLinker.link<{ ping(): Promise<string> }>(mesh, { service: serviceName })

        const waitPromise = remote.wait(async nodes => {
            await Promise.resolve()
            return nodes.length > 0
        })

        await Promise.resolve()

        topology.upsertRemote({
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

    test('announces external capability metadata and topic snapshots', async () => {
        const mesh = new SpiderMesh()
        const snapshots: SpiderMeshNode[] = []
        const subscription = mesh.localNode$.subscribe(node => snapshots.push(cloneNode(node)))
        mesh.setLocalTransporterMetadata('http2-pubsub', { port: 4321 })
        mesh.setLocalTopics(['TopicEvent'])

        expect(snapshots.at(-1)?.topics).toContain('TopicEvent')
        expect(snapshots.at(-1)?.transporters['http2-pubsub']).toEqual({ port: 4321 })

        mesh.setLocalTopics([])

        expect(snapshots.at(-1)?.topics).not.toContain('TopicEvent')
        subscription.unsubscribe()
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

    test('listRpcNodes and __batch__ are honestly empty without transporter availability', async () => {
        const mesh = new SpiderMesh()
        // This RPC transporter does not expose watchService/listNodes.
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

        registry.upsertPeer({ ...base, node_id: 'ready', transporters: { Http2Rpc: { port: 5000 } } })
        registry.upsertPeer({ ...base, node_id: 'half', transporters: { Http2Rpc: true } })

        const isReady = (node: SpiderMeshNode) => Number((node.transporters?.Http2Rpc as { port?: number } | undefined)?.port) > 0
        const picks = new Set<string | null | undefined>()
        for (let i = 0; i < 25; i++) picks.add(registry.pickRpcNode(service, { filter: isReady }))

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
        mesh.registerTransporter(unreachable)
        mesh.registerTransporter(loopback)

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

    test('does not dispatch when the sole RPC transporter reports no route', async () => {
        const mesh = new SpiderMesh()
        const serviceName = nextName('SoleService')
        // canRoute=false (e.g. provider mid-startup) but it is the only transporter → still used.
        class NotReadyLoopback extends NamedLoopbackRpcTransporter {
            override readonly name = 'not-ready-loopback'
            canRoute() {
                return false
            }
        }
        const loopback = new NotReadyLoopback(mesh.node_id)
        mesh.registerTransporter(loopback)

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

        await expect(firstValueFrom(mesh.callRemoteService<string, never>({
            service: serviceName,
            method: 'echo',
            args: ['should-not-send'],
        }))).rejects.toMatchObject({ code: 'MICROSERVICE_OFFLINE' })
    })
})
