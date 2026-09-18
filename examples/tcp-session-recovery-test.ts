import { firstValueFrom, filter, timeout } from 'rxjs'
import { Registry, type RpcRequestPacket, type SpiderMeshNode } from '@spider-mesh/core'
import type { DiscoveryMessage } from '@spider-mesh/discovery'
import { Http2Rpc } from '../src/index.js'
import { createDiscovery } from './helpers/createDiscovery.js'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'tcp-session-recovery'

function node(node_id: string, port?: number, version = 1): SpiderMeshNode {
    return {
        host: '127.0.0.1',
        namespace,
        version,
        node_id,
        topics: [],
        services: port ? { RecoveryService: {} } : {},
        nodes: {},
        transporters: port ? { http2: { port } } : {},
    }
}

function message(data: SpiderMeshNode): DiscoveryMessage<SpiderMeshNode> {
    return {
        node_id: data.node_id,
        namespace: data.namespace,
        tags: ['spider-mesh', 'node'],
        version: String(data.version),
        created_at: Date.now(),
        seq: data.version,
        data,
    }
}

async function waitFor(predicate: () => boolean, timeoutMs = 5000) {
    const startedAt = Date.now()
    while (!predicate()) {
        if (Date.now() - startedAt > timeoutMs) throw new Error(`Timed out after ${timeoutMs}ms`)
        await Bun.sleep(10)
    }
}

async function waitForPort(rpc: Http2Rpc) {
    await waitFor(() => Number(rpc.metadata?.port) > 0)
    return Number(rpc.metadata?.port)
}

function serve(rpc: Http2Rpc, label: string) {
    return rpc.subscribe(event => {
        const packet = event.rpc
        if (packet?.kind !== 'request') return
        void rpc.send({
            kind: 'response',
            request_id: packet.request_id,
            destination_node_id: packet.sender_node_id,
            data: label,
            completed: true,
        }).catch(error => console.error('Failed to send recovery response', error))
    })
}

async function call(rpc: Http2Rpc, request_id: string, destinationNodeId: string) {
    const response = firstValueFrom(rpc.pipe(
        filter(event => event.rpc?.kind === 'response' && event.rpc.request_id === request_id),
        timeout(5000),
    ))
    const packet: RpcRequestPacket = {
        kind: 'request',
        request_id,
        sender_node_id: 'recovery-client',
        destination_node_id: destinationNodeId,
        service: 'RecoveryService',
        method: 'ping',
        args: [],
    }
    await rpc.send(packet)
    return (await response).rpc
}

let providerRegistry = new Registry()
const clientRegistry = new Registry({ staleAfterMs: 500 })
let providerDiscovery = createDiscovery('recovery-provider-1', providerRegistry)
const clientDiscovery = createDiscovery('recovery-client', clientRegistry)
const clientRpc = new Http2Rpc(clientRegistry)
let providerRpc = new Http2Rpc(providerRegistry)
let providerSubscription = serve(providerRpc, 'before-restart')

try {
    const firstPort = await waitForPort(providerRpc)
    await Promise.all([
        providerDiscovery.broadcast(message(node('recovery-provider-1', firstPort))),
        clientDiscovery.broadcast(message(node('recovery-client'))),
    ])
    await waitFor(() => clientRegistry.getPeer('recovery-provider-1')?.transporters.http2?.port === firstPort)

    const first = await call(clientRpc, 'session-recovery-1', 'recovery-provider-1')
    if (first?.kind !== 'response' || first.data !== 'before-restart') {
        throw new Error(`Unexpected first response: ${JSON.stringify(first)}`)
    }

    providerSubscription.unsubscribe()
    providerRpc.unsubscribe()
    providerDiscovery.close()

    await waitFor(() => !clientRegistry.getPeer('recovery-provider-1'), 3000)

    providerRegistry = new Registry()
    providerDiscovery = createDiscovery('recovery-provider-2', providerRegistry)
    providerRpc = new Http2Rpc(providerRegistry)
    providerSubscription = serve(providerRpc, 'after-restart')
    const secondPort = await waitForPort(providerRpc)
    await providerDiscovery.broadcast(message(node('recovery-provider-2', secondPort, 1)))
    await waitFor(() => clientRegistry.getPeer('recovery-provider-2')?.transporters.http2?.port === secondPort)

    const second = await call(clientRpc, 'session-recovery-2', 'recovery-provider-2')
    if (second?.kind !== 'response' || second.data !== 'after-restart') {
        throw new Error(`Unexpected recovered response: ${JSON.stringify(second)}`)
    }

    console.log(JSON.stringify({
        topologyEvictedStalePeer: true,
        rpcRecovered: true,
        nodeIdChanged: true,
        recoveredFromOneShotDiscovery: true,
    }))
    console.log('TCP session recovery test passed')
} finally {
    providerSubscription.unsubscribe()
    providerRpc.unsubscribe()
    clientRpc.unsubscribe()
    providerDiscovery.close()
    clientDiscovery.close()
}
