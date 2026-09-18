import type { RpcRequestPacket, SpiderMeshNode } from '@spider-mesh/core'
import type { DiscoveryMessage } from '@spider-mesh/discovery'
import type { Http2Rpc } from '../../src/index.js'

export function makeNode(options: {
    node_id: string
    namespace: string
    services?: Record<string, unknown>
    transporters?: Record<string, unknown>
    version?: number
    host?: string
}): SpiderMeshNode {
    return {
        host: options.host || '127.0.0.1',
        namespace: options.namespace,
        version: options.version || 1,
        node_id: options.node_id,
        topics: [],
        services: options.services || {},
        nodes: {},
        transporters: options.transporters || {},
    }
}

export function discoveryMessage(node: SpiderMeshNode): DiscoveryMessage<SpiderMeshNode> {
    return {
        node_id: node.node_id,
        namespace: node.namespace,
        tags: ['spider-mesh', 'node'],
        version: String(node.version),
        created_at: Date.now(),
        seq: node.version,
        data: node,
    }
}

export async function waitFor(predicate: () => boolean, timeoutMs = 5000, label = 'condition') {
    const startedAt = Date.now()
    while (!predicate()) {
        if (Date.now() - startedAt > timeoutMs) throw new Error(`Timed out waiting for ${label}`)
        await Bun.sleep(10)
    }
}

export async function waitForRpcPort(rpc: Http2Rpc) {
    await waitFor(() => Number(rpc.metadata?.port) > 0, 5000, 'HTTP/2 port')
    return Number(rpc.metadata?.port)
}

let requestIndex = 0
export async function callRpc(options: {
    rpc: Http2Rpc
    sender_node_id: string
    service: string
    destination_node_id?: string
    timeoutMs?: number
}) {
    const request_id = `${options.sender_node_id}:${options.service}:${++requestIndex}`
    let timer: ReturnType<typeof setTimeout>
    let subscription: ReturnType<Http2Rpc['subscribe']>
    const result = new Promise<any>((resolve, reject) => {
        timer = setTimeout(() => {
            subscription.unsubscribe()
            reject(new Error(`RPC ${request_id} timed out`))
        }, options.timeoutMs || 5000)
        subscription = options.rpc.subscribe(event => {
            const packet = event.rpc
            if (packet?.kind !== 'response' || packet.request_id !== request_id) return
            if (!packet.completed && packet.error == undefined) return
            clearTimeout(timer)
            subscription.unsubscribe()
            if (packet.error) reject(packet.error)
            else resolve(packet.data)
        })
    })

    const packet: RpcRequestPacket = {
        kind: 'request',
        request_id,
        sender_node_id: options.sender_node_id,
        destination_node_id: options.destination_node_id,
        service: options.service,
        method: 'ping',
        args: [],
    }
    try {
        await options.rpc.send(packet)
    } catch (error) {
        clearTimeout(timer!)
        subscription!.unsubscribe()
        throw error
    }
    return await result
}

export function serveRpc(rpc: Http2Rpc, node_id: string) {
    return rpc.subscribe(event => {
        const packet = event.rpc
        if (packet?.kind !== 'request') return
        void rpc.send({
            kind: 'response',
            request_id: packet.request_id,
            destination_node_id: packet.sender_node_id,
            data: { node_id, service: packet.service },
            completed: true,
        })
    })
}
