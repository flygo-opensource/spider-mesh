import { Registry } from '@spider-mesh/core'
import { createDiscovery } from '../helpers/createDiscovery.js'
import { discoveryMessage, makeNode, waitFor } from './helpers.js'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'metadata-shrink-test'
const providerRegistry = new Registry()
const consumerRegistry = new Registry()
const provider = createDiscovery('metadata-provider', providerRegistry)
const consumer = createDiscovery('metadata-consumer', consumerRegistry)

try {
    await Promise.all([
        consumer.broadcast(discoveryMessage(makeNode({ node_id: 'metadata-consumer', namespace }))),
        provider.broadcast(discoveryMessage(makeNode({
            node_id: 'metadata-provider',
            namespace,
            version: 1,
            services: { ServiceB: {}, ServiceC: {} },
            transporters: { http2: { port: 31001 } },
        }))),
    ])

    await waitFor(
        () => !!consumerRegistry.getPeer('metadata-provider')?.services.ServiceC,
        3000,
        'initial ServiceB + ServiceC metadata',
    )

    await provider.broadcast(discoveryMessage(makeNode({
        node_id: 'metadata-provider',
        namespace,
        version: 2,
        services: { ServiceB: {} },
        transporters: { http2: { port: 31002 } },
    })))

    await waitFor(
        () => consumerRegistry.getPeer('metadata-provider')?.version === 2,
        3000,
        'replacement metadata version',
    )

    const peer = consumerRegistry.getPeer('metadata-provider')
    if (!peer) throw new Error('Provider disappeared unexpectedly')
    if ('ServiceC' in peer.services) {
        throw new Error(`Stale ServiceC survived full metadata replacement: ${JSON.stringify(peer.services)}`)
    }
    if (peer.transporters.http2?.port !== 31002) {
        throw new Error(`RPC endpoint was not replaced: ${JSON.stringify(peer.transporters)}`)
    }

    console.log(JSON.stringify({ metadataShrink: true, services: Object.keys(peer.services), rpcPort: 31002 }))
} finally {
    provider.close()
    consumer.close()
}
