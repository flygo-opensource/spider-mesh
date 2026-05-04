import process from 'node:process'
import { firstValueFrom, filter, map, timeout } from 'rxjs'
import { Http2Rpc } from '../src/index.js'
import type { SpiderMeshNode } from '../src/types.js'

const targetPort = Number(process.env.RPC_TARGET_PORT || 0)

if (!targetPort) {
    throw new Error('Missing RPC_TARGET_PORT')
}

const rpc = new Http2Rpc()

await firstValueFrom(
    rpc.pipe(
        filter(event => !!event.endpoints?.port),
        map(event => event.endpoints),
        timeout(5000),
    ),
)

const target: SpiderMeshNode = {
    ips: ['127.0.0.1'],
    host: '127.0.0.1',
    namespace: 'tcp-contract',
    version: 1,
    node_id: 'rpc-contract-server',
    services: {},
    nodes: {},
    transporters: {
        Http2Rpc: { port: targetPort }
    }
}

await rpc.send({
    kind: 'request',
    request_id: `req-${Date.now().toString(36)}`,
    source_node_id: 'rpc-contract-client',
    target_node_id: target.node_id,
    service: 'ContractService',
    method: 'ping',
    args: ['ok']
}, target)

console.log('RPC_CLIENT_SENT')
process.exit(0)