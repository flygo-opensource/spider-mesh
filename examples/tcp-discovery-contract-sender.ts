import { UdpDiscovery } from '../src/index.js'
import type { MdnsMessage, SpiderMeshNode } from '../src/types.js'

const discovery = new UdpDiscovery()

const localNode: SpiderMeshNode = {
    host: '127.0.0.1',
    namespace: process.env.SPIDERMESH_NAMESPACE || 'tcp-discovery-contract',
    version: 1,
    node_id: 'discovery-contract-sender',
    services: {},
    nodes: {},
    transporters: {},
}

const hello: MdnsMessage<SpiderMeshNode> = {
    hi: true,
    node: localNode,
    sender_id: localNode.node_id,
}

await discovery.broadcast(hello)
console.log('DISCOVERY_SENDER_SENT')

setInterval(() => undefined, 1000)