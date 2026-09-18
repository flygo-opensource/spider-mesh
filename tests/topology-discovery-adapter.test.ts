import { expect, test } from 'bun:test'
import { BehaviorSubject, Subject } from 'rxjs'
import type { SpiderMeshNode } from '@spider-mesh/core'
import { SPIDER_MESH_DISCOVERY_TAGS, TopologyDiscoveryAdapter, type DiscoveryMessage, type DiscoveryTransporter } from '../src/index.js'

const node = (node_id: string, version = 1): SpiderMeshNode => ({
    node_id,
    namespace: 'test',
    host: '127.0.0.1',
    version,
    topics: [],
    services: {},
    nodes: {},
    transporters: {},
})

const message = (data: SpiderMeshNode): DiscoveryMessage<SpiderMeshNode> => ({
    node_id: data.node_id,
    namespace: data.namespace,
    tags: [...SPIDER_MESH_DISCOVERY_TAGS],
    version: String(data.version),
    created_at: Date.now(),
    seq: data.version,
    data,
})

class MockDiscovery extends Subject<DiscoveryMessage<SpiderMeshNode>> implements DiscoveryTransporter<SpiderMeshNode> {
    broadcasts: DiscoveryMessage<SpiderMeshNode>[] = []

    async broadcast(outbound: DiscoveryMessage<SpiderMeshNode>) {
        this.broadcasts.push(outbound)
    }
}

test('topology adapter broadcasts local nodes and writes remote nodes back', async () => {
    const localNode$ = new BehaviorSubject(node('local'))
    const discovery = new MockDiscovery()
    const remoteNodes: SpiderMeshNode[] = []
    const removed: string[] = []
    const adapter = new TopologyDiscoveryAdapter(discovery)

    const binding = adapter.bind({
        localNode$,
        upsertRemote: remote => remoteNodes.push(remote),
        removeRemote: nodeId => removed.push(nodeId),
    })
    await Promise.resolve()

    discovery.next(message(node('remote', 2)))

    const announced = discovery.broadcasts.at(-1)
    expect(announced?.data.node_id).toBe('local')
    expect(announced?.tags).toEqual(['spider-mesh', 'node'])
    expect(remoteNodes.map(remote => remote.node_id)).toEqual(['remote'])
    expect(removed).toEqual([])

    binding.unsubscribe()
    await adapter.close()
})

test('topology adapter heartbeats the latest local snapshot', async () => {
    const localNode$ = new BehaviorSubject(node('heartbeat-local'))
    const discovery = new MockDiscovery()
    const adapter = new TopologyDiscoveryAdapter(discovery, {
        heartbeatIntervalMs: 10,
        closeTransporter: false,
    })
    const binding = adapter.bind({
        localNode$,
        upsertRemote: () => undefined,
        removeRemote: () => undefined,
    })

    await Bun.sleep(35)
    expect(discovery.broadcasts.length).toBeGreaterThanOrEqual(3)
    expect(discovery.broadcasts.every(outbound => outbound.data.node_id === 'heartbeat-local')).toBe(true)

    binding.unsubscribe()
    const countAfterUnsubscribe = discovery.broadcasts.length
    await Bun.sleep(20)
    expect(discovery.broadcasts.length).toBe(countAfterUnsubscribe)
})

test('topology adapter delegates membership verification', async () => {
    class VerifyingDiscovery extends MockDiscovery {
        async verify(node_id: string) {
            return node_id === 'dead-node' ? 'dead' as const : 'alive' as const
        }
    }

    const adapter = new TopologyDiscoveryAdapter(new VerifyingDiscovery())
    expect(await adapter.verify('dead-node')).toBe('dead')
    expect(await adapter.verify('alive-node')).toBe('alive')
    expect(await new TopologyDiscoveryAdapter(new MockDiscovery()).verify('any')).toBe('unknown')
})

test('topology adapter reports broadcast failures through onError', async () => {
    // Ví dụ thật: UdpDiscovery từ chối message khác namespace. Không có onError thì lỗi này mất hẳn.
    class RejectingDiscovery extends MockDiscovery {
        override async broadcast(): Promise<void> {
            throw new Error('Discovery message namespace must be my-app')
        }
    }

    const errors: unknown[] = []
    const adapter = new TopologyDiscoveryAdapter(new RejectingDiscovery(), {
        onError: error => errors.push(error),
    })
    const binding = adapter.bind({
        localNode$: new BehaviorSubject(node('local')),
        upsertRemote: () => undefined,
        removeRemote: () => undefined,
    })
    await Bun.sleep(5)

    expect(errors.map(error => (error as Error).message)).toEqual(['Discovery message namespace must be my-app'])
    binding.unsubscribe()
})
