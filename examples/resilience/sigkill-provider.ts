import { Registry } from '@spider-mesh/core'
import { Http2Rpc } from '../../src/index.js'
import { createDiscovery } from '../helpers/createDiscovery.js'
import { discoveryMessage, makeNode, serveRpc, waitForRpcPort } from './helpers.js'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'sigkill-test'
const node_id = process.env.SPIDERMESH_NODE_ID || 'sigkill-provider'
const generation = Number(process.env.PROVIDER_GENERATION || 1)
const registry = new Registry()
const discovery = createDiscovery(node_id, registry)
const rpc = new Http2Rpc(registry)
const subscription = serveRpc(rpc, node_id)
const port = await waitForRpcPort(rpc)

await discovery.broadcast(discoveryMessage(makeNode({
    node_id,
    namespace,
    version: generation,
    services: { SigkillService: { generation } },
    transporters: { http2: { port } },
})))

console.log(JSON.stringify({ event: 'provider-ready', node_id, generation, port }))

const shutdown = () => {
    subscription.unsubscribe()
    rpc.unsubscribe()
    discovery.close()
    process.exit(0)
}
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)
setInterval(() => undefined, 60000)
