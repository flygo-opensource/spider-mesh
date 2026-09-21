import type { SpiderMeshNode } from '../src/types.js'
import type { DiscoveryMessage } from '../src/index.js'
import { createDiscovery } from './helpers/createDiscovery.js'

const localNode: SpiderMeshNode = {
    host: '127.0.0.1',
    namespace: process.env.SPIDERMESH_NAMESPACE || 'tcp-discovery-contract',
    version: 1,
    node_id: 'discovery-contract-sender',
    services: {},
    nodes: {},
    transporters: {},
}

const discovery = createDiscovery(localNode.node_id)

const hello: DiscoveryMessage<SpiderMeshNode> = {
    node_id: localNode.node_id,
    namespace: localNode.namespace,
    tags: ['spider-mesh', 'node'],
    version: String(localNode.version),
    created_at: Date.now(),
    seq: localNode.version,
    data: localNode,
}

await discovery.broadcast(hello)
console.log('DISCOVERY_SENDER_SENT')

setInterval(() => undefined, 1000)
