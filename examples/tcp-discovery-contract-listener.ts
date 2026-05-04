import process from 'node:process'
import { UdpDiscovery } from '../src/index.js'
import type { MdnsMessage, SpiderMeshNode } from '../src/types.js'

const discovery = new UdpDiscovery()

const localNode: SpiderMeshNode = {
    ips: ['127.0.0.1'],
    host: '127.0.0.1',
    namespace: process.env.SPIDERMESH_NAMESPACE || 'tcp-discovery-contract',
    version: 1,
    node_id: 'discovery-contract-listener',
    services: {},
    nodes: {},
    transporters: {},
}

const guard = setTimeout(() => {
    console.error('Discovery contract listener timed out')
    process.exit(1)
}, 15000)

discovery.subscribe(event => {
    console.log(JSON.stringify({
        type: 'discovery',
        hasDiscovered: 'discovered' in event,
        hasRawNodeId: 'node_id' in (event as Record<string, unknown>),
        discoveredNodeId: event.discovered.node_id,
        discoveredNamespace: event.discovered.namespace,
    }))
    clearTimeout(guard)
    process.exit(0)
})

const hello: MdnsMessage<SpiderMeshNode> = {
    hi: true,
    node: localNode,
    sender_id: localNode.node_id,
}

await discovery.broadcast(hello, ['127.0.0.1'])
console.log('DISCOVERY_LISTENER_READY')