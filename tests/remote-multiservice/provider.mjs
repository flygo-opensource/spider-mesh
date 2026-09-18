import { Registry } from '@spider-mesh/core'
import { UdpDiscovery } from '@ohayo/udp'
import { Http2Rpc } from '@spider-mesh/tcp'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'remote-multiservice-test'
const nodeId = process.env.SPIDERMESH_NODE_ID
const serviceName = process.env.SERVICE_NAME
const providerLabel = process.env.PROVIDER_LABEL || nodeId

if (!nodeId || !serviceName) {
    throw new Error('SPIDERMESH_NODE_ID and SERVICE_NAME are required')
}

const registry = new Registry()
const discovery = new UdpDiscovery({
    namespace,
    tags: ['spider-mesh', 'node'],
    node_id: nodeId,
    key: process.env.OHAYO_DISCOVERY_KEY || 'spider-mesh',
})
discovery.subscribe(message => {
    registry.upsertPeer(message.data)
})
const rpc = new Http2Rpc(registry)

const log = payload => console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    pid: process.pid,
    ...payload,
}))

const startedAt = Date.now()
while (!Number(rpc.metadata?.port)) {
    if (Date.now() - startedAt > 5000) throw new Error('HTTP/2 endpoint did not become ready')
    await Bun.sleep(10)
}

const rpcSubscription = rpc.subscribe(event => {
    const packet = event.rpc
    if (packet?.kind !== 'request') return

    const response = packet.service === serviceName
        ? {
            data: {
                node_id: nodeId,
                service: serviceName,
                provider: providerLabel,
                method: packet.method,
            },
            completed: true,
        }
        : {
            error: {
                code: 'MICROSERVICE_NOT_FOUND',
                message: `${serviceName} provider cannot serve ${packet.service}`,
            },
            completed: true,
        }

    void rpc.send({
        kind: 'response',
        request_id: packet.request_id,
        destination_node_id: packet.sender_node_id,
        ...response,
    }).catch(error => log({ event: 'response-error', error: String(error) }))
})

const node = {
    host: process.env.SPIDERMESH_NODE_HOSTNAME || '127.0.0.1',
    namespace,
    version: 1,
    node_id: nodeId,
    topics: [],
    services: { [serviceName]: { provider: providerLabel } },
    nodes: {},
    transporters: { http2: { port: rpc.metadata.port } },
}

const localAnnouncement = {
    node_id: node.node_id,
    namespace,
    tags: ['spider-mesh', 'node'],
    version: String(node.version),
    created_at: Date.now(),
    seq: node.version,
    data: node,
}
await discovery.broadcast(localAnnouncement)

log({
    event: 'provider-ready',
    node_id: nodeId,
    service: serviceName,
    provider: providerLabel,
    rpc_port: rpc.metadata.port,
})

const shutdown = signal => {
    log({ event: 'provider-shutdown', signal, node_id: nodeId, service: serviceName })
    rpcSubscription.unsubscribe()
    rpc.unsubscribe()
    discovery.close()
    process.exit(0)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))
setInterval(() => undefined, 60_000)
