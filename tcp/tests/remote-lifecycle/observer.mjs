import { Registry } from '@spider-mesh/core'
import { UdpDiscovery } from '@simple-discovery/udp'
import { Http2Rpc } from '@spider-mesh/tcp'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'remote-lifecycle-test'
const observerId = process.env.SPIDERMESH_NODE_ID || 'remote-observer'
const providerIds = (process.env.PROVIDER_NODE_IDS || process.env.PROVIDER_NODE_ID || 'remote-provider')
    .split(',')
    .filter(Boolean)
const registry = new Registry()
const discovery = new UdpDiscovery({
    namespace,
    tags: ['spider-mesh', 'node'],
    node_id: observerId,
    key: process.env.SIMPLE_DISCOVERY_KEY || 'spider-mesh',
})
discovery.subscribe(message => {
    registry.upsertPeer(message.data)
})
const rpc = new Http2Rpc(registry)

let activeProviderId
let onlineOccurrence = 0
let lastPort

const log = payload => console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    observer_pid: process.pid,
    ...payload,
}))

const refreshAvailability = () => {
    const nodes = registry.nodes$.value
    const provider = activeProviderId
        ? nodes.get(activeProviderId)
        : providerIds
            .map(nodeId => nodes.get(nodeId))
            .find(node => node && rpc.canRoute('LifecycleProbe', node.node_id))

    if (provider && !activeProviderId && rpc.canRoute('LifecycleProbe', provider.node_id)) {
        activeProviderId = provider.node_id
        onlineOccurrence += 1
        lastPort = provider.transporters?.http2?.port
        log({
            event: 'provider-online',
            node_id: provider.node_id,
            occurrence: onlineOccurrence,
            rediscovered: onlineOccurrence > 1,
            generation: provider.services?.LifecycleProbe?.generation,
            rpc_port: lastPort,
        })
        return
    }

    if (activeProviderId && !rpc.canRoute('LifecycleProbe', activeProviderId)) {
        log({ event: 'provider-offline', node_id: activeProviderId, previous_rpc_port: lastPort })
        activeProviderId = undefined
        lastPort = undefined
        return
    }
}

const subscription = registry.nodes$.subscribe(() => refreshAvailability())
const availabilityTimer = setInterval(refreshAvailability, 100)

const observerNode = {
    host: process.env.SPIDERMESH_NODE_HOSTNAME || '127.0.0.1',
    namespace,
    version: 1,
    node_id: observerId,
    topics: [],
    services: {},
    nodes: {},
    transporters: { http2: { port: rpc.metadata?.port } },
}

const localAnnouncement = {
    node_id: observerNode.node_id,
    namespace,
    tags: ['spider-mesh', 'node'],
    version: String(observerNode.version),
    created_at: Date.now(),
    seq: observerNode.version,
    data: observerNode,
}
await discovery.broadcast(localAnnouncement)

log({ event: 'observer-ready', node_id: observerId, watching_node_ids: providerIds })

const shutdown = signal => {
    log({ event: 'observer-shutdown', signal })
    subscription.unsubscribe()
    clearInterval(availabilityTimer)
    rpc.unsubscribe()
    discovery.close()
    process.exit(0)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))
setInterval(() => undefined, 60_000)
