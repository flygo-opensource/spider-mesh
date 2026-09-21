import { Registry } from '@spider-mesh/core'
import { createDiscovery } from '../helpers/createDiscovery.js'
import { discoveryMessage, makeNode, waitFor } from './helpers.js'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'duplicate-node-id-test'
const consumerRegistry = new Registry()
const consumer = createDiscovery('duplicate-consumer', consumerRegistry)
const providerB = createDiscovery('duplicate-provider', new Registry())
const providerC = createDiscovery('duplicate-provider', new Registry())

try {
    await Promise.all([
        consumer.broadcast(discoveryMessage(makeNode({ node_id: 'duplicate-consumer', namespace }))),
        providerB.broadcast(discoveryMessage(makeNode({
            node_id: 'duplicate-provider',
            namespace,
            services: { ServiceB: { process: 'b' } },
            transporters: { http2: { port: 32001 } },
        }))),
        providerC.broadcast(discoveryMessage(makeNode({
            node_id: 'duplicate-provider',
            namespace,
            services: { ServiceC: { process: 'c' } },
            transporters: { http2: { port: 32002 } },
        }))),
    ])

    await waitFor(() => !!consumerRegistry.getPeer('duplicate-provider'), 3000, 'duplicate provider')
    await Bun.sleep(500)
    const peer = consumerRegistry.getPeer('duplicate-provider')!
    const services = Object.keys(peer.services).sort()

    // Two real processes must never be merged into a synthetic peer that appears to serve both.
    if (services.includes('ServiceB') && services.includes('ServiceC')) {
        throw new Error(`Duplicate node_id silently merged two processes: ${JSON.stringify(peer)}`)
    }

    console.log(JSON.stringify({ duplicateIdSafe: true, node_id: peer.node_id, services }))
} finally {
    providerB.close()
    providerC.close()
    consumer.close()
}
