import { Registry } from '@spider-mesh/core'
import { UdpDiscovery } from '@simple-discovery/udp'
import { Http2Rpc } from '@spider-mesh/tcp'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'remote-multiservice-test'
const nodeId = process.env.SPIDERMESH_NODE_ID || 'service-a'
const expected = {
    ServiceB: new Set((process.env.EXPECTED_SERVICE_B || '').split(',').filter(Boolean)),
    ServiceC: new Set((process.env.EXPECTED_SERVICE_C || '').split(',').filter(Boolean)),
}

const registry = new Registry()
const discovery = new UdpDiscovery({
    namespace,
    tags: ['spider-mesh', 'node'],
    node_id: nodeId,
    key: process.env.SIMPLE_DISCOVERY_KEY || 'spider-mesh',
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

const idsFor = service => registry.listPeers(service).map(node => node.node_id).sort()
const hasExpected = service => {
    const actual = idsFor(service)
    const wanted = [...expected[service]].sort()
    return actual.length === wanted.length && actual.every((id, index) => id === wanted[index])
}

async function waitFor(predicate, timeoutMs, label) {
    const startedAt = Date.now()
    while (!predicate()) {
        if (Date.now() - startedAt > timeoutMs) throw new Error(`Timed out waiting for ${label}`)
        await Bun.sleep(50)
    }
}

let requestIndex = 0
async function call(service, destinationNodeId) {
    const requestId = `${nodeId}:${service}:${++requestIndex}`
    const response = new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            subscription.unsubscribe()
            reject(new Error(`RPC ${requestId} timed out`))
        }, 5000)
        const subscription = rpc.subscribe(event => {
            const packet = event.rpc
            if (packet?.kind !== 'response' || packet.request_id !== requestId) return
            clearTimeout(timer)
            subscription.unsubscribe()
            if (packet.error) reject(packet.error)
            else resolve(packet.data)
        })
    })

    await rpc.send({
        kind: 'request',
        request_id: requestId,
        sender_node_id: nodeId,
        destination_node_id: destinationNodeId,
        service,
        method: 'ping',
        args: [],
    })
    return await response
}

const startedAt = Date.now()
while (!Number(rpc.metadata?.port)) {
    if (Date.now() - startedAt > 5000) throw new Error('Service A HTTP/2 endpoint did not become ready')
    await Bun.sleep(10)
}

const node = {
    host: process.env.SPIDERMESH_NODE_HOSTNAME || '127.0.0.1',
    namespace,
    version: 1,
    node_id: nodeId,
    topics: [],
    services: { ServiceA: {} },
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

log({ event: 'service-a-ready', node_id: nodeId, rpc_port: rpc.metadata.port })

try {
    await waitFor(
        () => hasExpected('ServiceB') && hasExpected('ServiceC'),
        15000,
        'all ServiceB and ServiceC providers',
    )

    log({ event: 'all-providers-discovered', ServiceB: idsFor('ServiceB'), ServiceC: idsFor('ServiceC') })

    const stableSnapshots = []
    for (let second = 1; second <= 12; second++) {
        await Bun.sleep(1000)
        const snapshot = { second, ServiceB: idsFor('ServiceB'), ServiceC: idsFor('ServiceC') }
        stableSnapshots.push(snapshot)
        if (!hasExpected('ServiceB') || !hasExpected('ServiceC')) {
            throw new Error(`Provider set decayed: ${JSON.stringify(snapshot)}`)
        }
    }

    const directResults = []
    for (const service of ['ServiceB', 'ServiceC']) {
        for (const providerNodeId of [...expected[service]].sort()) {
            directResults.push(await call(service, providerNodeId))
        }
    }

    const roundRobinResults = { ServiceB: [], ServiceC: [] }
    for (const service of ['ServiceB', 'ServiceC']) {
        for (let index = 0; index < 4; index++) {
            roundRobinResults[service].push(await call(service))
        }
        const reached = new Set(roundRobinResults[service].map(result => result.node_id))
        if (reached.size !== expected[service].size || [...expected[service]].some(id => !reached.has(id))) {
            throw new Error(`Round robin did not reach every ${service} provider: ${JSON.stringify([...reached])}`)
        }
    }

    log({
        event: 'multiservice-test-passed',
        stable_seconds: stableSnapshots.length,
        ServiceB: idsFor('ServiceB'),
        ServiceC: idsFor('ServiceC'),
        direct_rpc_nodes: directResults.map(result => result.node_id).sort(),
        round_robin_b: roundRobinResults.ServiceB.map(result => result.node_id),
        round_robin_c: roundRobinResults.ServiceC.map(result => result.node_id),
    })
} finally {
    rpc.unsubscribe()
    discovery.close()
}
