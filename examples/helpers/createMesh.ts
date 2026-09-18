import { SpiderMesh, Topology } from '@spider-mesh/core'
import { EventBus } from '@spider-mesh/events'
import { TopologyDiscoveryAdapter } from '@spider-mesh/discovery'
import { Http2Pubsub, Http2Rpc } from '../../src/index.js'
import { createDiscovery } from './createDiscovery.js'

export function createMesh() {
    const discovery = createDiscovery()
    const topology = new Topology({
        discovery: new TopologyDiscoveryAdapter(discovery, { heartbeatIntervalMs: 5_000 }),
        staleAfterMs: 15_000,
    })
    const mesh = new SpiderMesh({
        topology,
        transporters: [new Http2Rpc()],
    })
    const events = new EventBus({ mesh })

    events.registerTransporter(new Http2Pubsub(topology))

    return { mesh, events, registry: topology, topology, discovery }
}
