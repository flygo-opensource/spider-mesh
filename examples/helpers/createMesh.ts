import { SpiderMesh, Topology } from '@spider-mesh/core'
import { EventBus } from '@spider-mesh/events'
import { TopologyDiscoveryAdapter } from '../../src/index.js'
import { Http2Pubsub, Http2Rpc } from '../../src/index.js'
import { createDiscovery } from './createDiscovery.js'

export function createMesh() {
    const discovery = createDiscovery()
    const topology = new Topology({
        discovery: new TopologyDiscoveryAdapter(discovery),
        // UDP chỉ để tìm thấy nhau; node bị xoá khi kết nối HTTP/2 tới nó đứt liên tục đủ lâu.
        removeUnreachableAfterMs: 60_000,
    })
    const mesh = new SpiderMesh({
        topology,
        transporters: [new Http2Rpc()],
    })
    const events = new EventBus({ mesh })

    events.registerTransporter(new Http2Pubsub(topology))

    return { mesh, events, registry: topology, topology, discovery }
}
