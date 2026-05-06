import { firstValueFrom, filter, timeout } from 'rxjs'
import { Registry } from '@spider-mesh/core'
import { Http2Rpc } from '../src/index.js'
import type { RpcEvent, SpiderMeshNode } from '../src/types.js'

async function main() {
    const server = new Http2Rpc()
    const client = new Http2Rpc()
    const registry = new Registry()
    client.linkRegistry(registry)

    const serverEndpoints = await firstValueFrom(
        server.pipe(
            filter((event): event is RpcEvent & { endpoints: { port: number } } => Number(event.endpoints?.port) > 0),
            timeout(5000),
        ),
    )

    const rpcEventPromise = firstValueFrom(
        server.pipe(
            filter((event): event is RpcEvent & { rpc: NonNullable<RpcEvent['rpc']> } => !!event.rpc),
            timeout(5000),
        ),
    )

    const targetNode: SpiderMeshNode = {
        host: '127.0.0.1',
        namespace: 'tcp-contract',
        version: 1,
        node_id: 'rpc-contract-server',
        topics: [],
        services: {},
        nodes: {},
        transporters: {
            Http2Rpc: { port: serverEndpoints.endpoints.port }
        }
    }

    registry.upsertPeer(targetNode)

    await client.send({
        kind: 'request',
        request_id: `req-${Date.now().toString(36)}`,
        source_node_id: 'rpc-contract-client',
        target_node_id: targetNode.node_id,
        service: 'ContractService',
        method: 'ping',
        args: ['ok']
    }, targetNode.node_id)

    const event = await rpcEventPromise
    console.log(JSON.stringify({
        type: 'rpc',
        hasRpc: 'rpc' in event,
        hasMessage: 'message' in (event as Record<string, unknown>),
        packetKind: event.rpc.packet.kind,
        senderNodeId: event.rpc.node_id,
        sourceNodeId: event.rpc.packet.source_node_id,
        targetNodeId: event.rpc.packet.target_node_id,
    }))
    console.log('TCP RPC contract test passed')

    server.unsubscribe()
    client.unsubscribe()
}

await main()