import process from 'node:process'
import { Registry } from '@spider-mesh/core'
import { firstValueFrom, filter, map, timeout } from 'rxjs'
import { Http2Rpc } from '../src/index.js'
import type { SpiderMeshNode } from '../src/types.js'

const targetPort = Number(process.env.RPC_TARGET_PORT || 0)

if (!targetPort) {
    throw new Error('Missing RPC_TARGET_PORT')
}

const rpc = new Http2Rpc()
const registry = new Registry()
rpc.linkRegistry(registry)

await firstValueFrom(
    rpc.pipe(
        filter(event => !!event.endpoints?.port),
        map(event => event.endpoints),
        timeout(5000),
    ),
)

const target: SpiderMeshNode = {
    host: '127.0.0.1',
    namespace: 'tcp-contract',
    version: 1,
    node_id: 'rpc-contract-server',
    topics: [],
    services: {},
    nodes: {},
    transporters: {
        Http2Rpc: { port: targetPort }
    }
}

registry.upsertPeer(target)

await rpc.send({
    kind: 'request',
    request_id: `req-${Date.now().toString(36)}`,
    source_node_id: 'rpc-contract-client',
    target_node_id: target.node_id,
    service: 'ContractService',
    method: 'ping',
    args: ['ok']
}, target.node_id)

console.log('RPC_CLIENT_SENT')
process.exit(0)