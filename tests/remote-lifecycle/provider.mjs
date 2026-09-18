import { Registry } from '@spider-mesh/core'
import { UdpDiscovery } from '@ohayo/udp'
import { Http2Rpc } from '@spider-mesh/tcp'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'remote-lifecycle-test'
const providerId = process.env.SPIDERMESH_NODE_ID || 'remote-provider'
const generation = Number(process.env.PROVIDER_GENERATION || 1)
const registry = new Registry()
const discovery = new UdpDiscovery({
    namespace,
    tags: ['spider-mesh', 'node'],
    node_id: providerId,
    key: process.env.OHAYO_DISCOVERY_KEY || 'spider-mesh',
})
discovery.subscribe(message => {
    registry.upsertPeer(message.data)
})
const rpc = new Http2Rpc(registry)

const log = payload => console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    provider_pid: process.pid,
    ...payload,
}))

const startedAt = Date.now()
while (!Number(rpc.metadata?.port)) {
    if (Date.now() - startedAt > 5000) throw new Error('HTTP/2 endpoint did not become ready')
    await new Promise(resolve => setTimeout(resolve, 10))
}

const providerNode = {
    host: process.env.SPIDERMESH_NODE_HOSTNAME || '127.0.0.1',
    namespace,
    version: generation,
    node_id: providerId,
    topics: [],
    services: { LifecycleProbe: { generation } },
    nodes: {},
    transporters: { http2: { port: rpc.metadata.port } },
}

const localAnnouncement = {
    node_id: providerNode.node_id,
    namespace,
    tags: ['spider-mesh', 'node'],
    version: String(providerNode.version),
    created_at: Date.now(),
    seq: providerNode.version,
    data: providerNode,
}
await discovery.broadcast(localAnnouncement)

log({
    event: 'provider-ready',
    node_id: providerId,
    generation,
    rpc_port: rpc.metadata.port,
})

const shutdown = signal => {
    log({ event: 'provider-shutdown', signal, generation })
    rpc.unsubscribe()
    discovery.close()
    process.exit(0)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))
setInterval(() => undefined, 60_000)
