import { Registry, type RpcResponsePacket } from '@spider-mesh/core'
import { Http2Rpc } from '../../src/index.js'
import { createDiscovery } from '../helpers/createDiscovery.js'
import { discoveryMessage, makeNode, waitFor, waitForRpcPort } from './helpers.js'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'stream-interruption-test'
const providerRegistry = new Registry()
const consumerRegistry = new Registry()
const providerDiscovery = createDiscovery('stream-provider', providerRegistry)
const consumerDiscovery = createDiscovery('stream-consumer', consumerRegistry)
const providerRpc = new Http2Rpc(providerRegistry)
const consumerRpc = new Http2Rpc(consumerRegistry)
const request_id = 'interrupted-stream-request'

const providerSubscription = providerRpc.subscribe(event => {
    const packet = event.rpc
    if (packet?.kind !== 'request' || packet.request_id !== request_id) return
    void providerRpc.send({
        kind: 'response',
        request_id,
        destination_node_id: packet.sender_node_id,
        data: 'chunk-1',
    }).then(() => {
        setTimeout(() => providerRpc.unsubscribe(), 25)
    })
})

try {
    const [providerPort, consumerPort] = await Promise.all([
        waitForRpcPort(providerRpc),
        waitForRpcPort(consumerRpc),
    ])
    await Promise.all([
        providerDiscovery.broadcast(discoveryMessage(makeNode({
            node_id: 'stream-provider',
            namespace,
            services: { StreamService: {} },
            transporters: { http2: { port: providerPort } },
        }))),
        consumerDiscovery.broadcast(discoveryMessage(makeNode({
            node_id: 'stream-consumer',
            namespace,
            transporters: { http2: { port: consumerPort } },
        }))),
    ])
    await waitFor(() => !!consumerRegistry.getPeer('stream-provider'), 3000, 'stream provider')

    const packets: RpcResponsePacket[] = []
    const terminal = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error(`Terminal stream event timeout: ${JSON.stringify(packets)}`)), 5000)
        consumerRpc.subscribe(event => {
            const packet = event.rpc
            if (packet?.kind !== 'response' || packet.request_id !== request_id) return
            packets.push(packet)
            if (packet.completed || packet.error != undefined) {
                clearTimeout(timeout)
                resolve()
            }
        })
    })

    await consumerRpc.send({
        kind: 'request',
        request_id,
        sender_node_id: 'stream-consumer',
        destination_node_id: 'stream-provider',
        service: 'StreamService',
        method: 'stream',
        args: [],
    })
    await terminal
    await Bun.sleep(250)

    const chunks = packets.filter(packet => packet.data === 'chunk-1')
    const terminalErrors = packets.filter(packet => packet.error?.code === 'MICROSERVICE_OFFLINE' && packet.completed)
    if (chunks.length !== 1) throw new Error(`Expected one stream chunk, got ${JSON.stringify(packets)}`)
    if (terminalErrors.length !== 1) throw new Error(`Expected one terminal offline error, got ${JSON.stringify(packets)}`)
    if (packets.length !== 2) throw new Error(`Unexpected duplicate stream events: ${JSON.stringify(packets)}`)

    console.log(JSON.stringify({
        streamInterruptionPassed: true,
        chunkCount: chunks.length,
        terminalErrorCount: terminalErrors.length,
        totalEvents: packets.length,
    }))
} finally {
    providerSubscription.unsubscribe()
    if (!providerRpc.closed) providerRpc.unsubscribe()
    consumerRpc.unsubscribe()
    providerDiscovery.close()
    consumerDiscovery.close()
}
