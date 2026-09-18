import { UdpDiscovery } from '@ohayo/udp'
import type { SpiderMeshNode, Topology } from '@spider-mesh/core'

const tags = ['spider-mesh', 'node']

export function createDiscovery(node_id?: string, topology?: Topology) {
    const discovery = new UdpDiscovery<SpiderMeshNode>({
        namespace: process.env.SPIDERMESH_NAMESPACE || 'default',
        tags,
        node_id,
        key: process.env.SPIDERMESH_DISCOVERY_KEY || process.env.OHAYO_DISCOVERY_KEY || 'spider-mesh',
        port: Number(process.env.OHAYO_DISCOVERY_PORT || 11001),
        multicastAddress: process.env.OHAYO_UDP_MULTICAST_ADDRESS
            || '239.0.1.1',
        peers: (process.env.OHAYO_UDP_WHITELIST_ADDRESS || '')
            .split(',')
            .map(value => value.trim())
            .filter(Boolean),
    })

    // Compatibility cho các resilience fixture cấp thấp chưa dùng TopologyDiscoveryAdapter.
    if (topology) discovery.subscribe(message => topology.upsertRemote(message.data))
    return discovery
}
