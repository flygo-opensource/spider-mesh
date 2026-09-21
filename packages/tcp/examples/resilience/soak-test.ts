import { Registry } from '@spider-mesh/core'
import { Http2Rpc } from '../../src/index.js'
import { createDiscovery } from '../helpers/createDiscovery.js'
import { callRpc, discoveryMessage, makeNode, serveRpc, waitFor, waitForRpcPort } from './helpers.js'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'soak-test'
const providerCount = Number(process.env.SOAK_PROVIDER_COUNT || 10)
const durationMs = Number(process.env.SOAK_DURATION_MS || 60000)
const restartEveryMs = Number(process.env.SOAK_RESTART_EVERY_MS || 2000)

type Provider = {
    node_id: string
    generation: number
    registry: Registry
    discovery: ReturnType<typeof createDiscovery>
    rpc: Http2Rpc
    subscription: ReturnType<typeof serveRpc>
    port: number
}

async function startProvider(node_id: string, generation: number): Promise<Provider> {
    const registry = new Registry()
    const discovery = createDiscovery(node_id, registry)
    const rpc = new Http2Rpc(registry)
    const subscription = serveRpc(rpc, node_id)
    const port = await waitForRpcPort(rpc)
    await discovery.broadcast(discoveryMessage(makeNode({
        node_id,
        namespace,
        version: generation,
        services: { SoakService: { generation } },
        transporters: { http2: { port } },
    })))
    return { node_id, generation, registry, discovery, rpc, subscription, port }
}

function stopProvider(provider: Provider) {
    provider.subscription.unsubscribe()
    provider.rpc.unsubscribe()
    provider.discovery.close()
}

const consumerRegistry = new Registry()
const consumerDiscovery = createDiscovery('soak-consumer', consumerRegistry)
const consumerRpc = new Http2Rpc(consumerRegistry)
const consumerPort = await waitForRpcPort(consumerRpc)
await consumerDiscovery.broadcast(discoveryMessage(makeNode({
    node_id: 'soak-consumer',
    namespace,
    services: {},
    transporters: { http2: { port: consumerPort } },
})))

const providers = new Map<string, Provider>()
let rpcCalls = 0
let restarts = 0
let minNodes = Number.POSITIVE_INFINITY
let maxNodes = 0

try {
    for (let index = 0; index < providerCount; index++) {
        const node_id = `soak-provider-${index}`
        providers.set(node_id, await startProvider(node_id, 1))
    }

    await waitFor(
        () => consumerRegistry.listPeers('SoakService').length === providerCount,
        10000,
        `${providerCount} soak providers`,
    )

    const startedAt = Date.now()
    let nextRestartAt = startedAt + restartEveryMs
    let restartIndex = 0

    while (Date.now() - startedAt < durationMs) {
        const nodes = consumerRegistry.listPeers('SoakService')
        minNodes = Math.min(minNodes, nodes.length)
        maxNodes = Math.max(maxNodes, nodes.length)
        if (nodes.length !== providerCount) {
            throw new Error(`Soak registry decayed to ${nodes.length}/${providerCount}`)
        }

        const target = nodes[rpcCalls % nodes.length]
        const result = await callRpc({
            rpc: consumerRpc,
            sender_node_id: 'soak-consumer',
            service: 'SoakService',
            destination_node_id: target.node_id,
        })
        if (result?.node_id !== target.node_id) {
            throw new Error(`RPC routed to ${result?.node_id}, expected ${target.node_id}`)
        }
        rpcCalls += 1

        if (Date.now() >= nextRestartAt) {
            const node_id = `soak-provider-${restartIndex++ % providerCount}`
            const previous = providers.get(node_id)!
            const generation = previous.generation + 1
            stopProvider(previous)
            await Bun.sleep(100)
            const replacement = await startProvider(node_id, generation)
            providers.set(node_id, replacement)
            // Đợi cả kết nối tới cổng mới: ngay sau restart endpoint còn `suspect` và route() cố ý
            // không chọn nó, nên gọi sớm hơn sẽ nhận MICROSERVICE_OFFLINE (flaky trước đây).
            await waitFor(
                () => consumerRegistry.getPeer(node_id)?.transporters.http2?.port === replacement.port
                    && consumerRegistry.getReachability(node_id, 'http2') === 'reachable',
                3000,
                `${node_id} replacement endpoint`,
            )
            restarts += 1
            nextRestartAt = Date.now() + restartEveryMs
        }

        await Bun.sleep(100)
    }

    console.log(JSON.stringify({
        soakPassed: true,
        durationMs,
        providerCount,
        rpcCalls,
        restarts,
        minNodes,
        maxNodes,
    }))
} finally {
    for (const provider of providers.values()) stopProvider(provider)
    consumerRpc.unsubscribe()
    consumerDiscovery.close()
}
