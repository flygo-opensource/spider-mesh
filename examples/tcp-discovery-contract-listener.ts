import process from 'node:process'
import type { SpiderMeshNode } from '../src/types.js'
import type { DiscoveryMessage } from '@spider-mesh/discovery'
import { createDiscovery } from './helpers/createDiscovery.js'

const localNode: SpiderMeshNode = {
    host: '127.0.0.1',
    namespace: process.env.SPIDERMESH_NAMESPACE || 'tcp-discovery-contract',
    version: 1,
    node_id: 'discovery-contract-listener',
    services: {},
    nodes: {},
    transporters: {},
}

const discovery = createDiscovery(localNode.node_id)

const guard = setTimeout(() => {
    console.error('Discovery contract listener timed out')
    process.exit(1)
}, 15000)

discovery.subscribe(event => {
    console.log(JSON.stringify({
        type: 'discovery',
        hasData: !!event.data,
        hasEnvelopeNodeId: 'node_id' in (event as Record<string, unknown>),
        discoveredNodeId: event.data.node_id,
        discoveredNamespace: event.data.namespace,
    }))
    clearTimeout(guard)
    process.exit(0)
})

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
console.log('DISCOVERY_LISTENER_READY')
