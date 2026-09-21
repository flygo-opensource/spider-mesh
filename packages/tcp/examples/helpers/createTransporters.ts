import type { Topology } from '@spider-mesh/core'
import { Http2Rpc } from '../../src/index.js'
import { createDiscovery } from './createDiscovery.js'

export function createTransporters(topology: Topology, node_id?: string) {
    return {
        discovery: createDiscovery(node_id),
        rpc: new Http2Rpc(topology),
    }
}
