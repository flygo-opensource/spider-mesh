import { Buffer } from 'node:buffer'
import { encode } from '@msgpack/msgpack'
import { firstValueFrom, filter, timeout, type Observable } from 'rxjs'
import WebSocket from 'ws'
import { WebsocketRelayServer } from '../src/transporters/WebsocketRelayServer.js'
import { WebsocketTransporter } from '../src/transporters/WebsocketTransporter.js'
import { encodeRelayFrame } from '../src/transporters/websocketProtocol.js'
import type { MdnsMessage, RpcPacket, SpiderMeshNode } from '../src/types.js'

const port = 8800 + Math.floor(Math.random() * 200)
const baseWsUrl = `ws://127.0.0.1:${port}`
const serverWsUrl = `${baseWsUrl}?role=server`
const clientWsUrl = `${baseWsUrl}?role=client`
const server = new WebsocketRelayServer({
    port,
    host: '127.0.0.1',
    heartbeatIntervalMs: 200,
    heartbeatTimeoutMs: 600,
    isServerConnection: (_socket, request) => new URL(request.url || '/', baseWsUrl).searchParams.get('role') === 'server',
})

const nodeA: SpiderMeshNode = {
    ips: ['127.0.0.1'],
    host: '127.0.0.1',
    namespace: 'test',
    version: 1,
    node_id: 'node-a',
    topics: ['updates'],
    services: { GreetingService: {} },
    nodes: {},
    transporters: { websocket: {} },
}

const nodeB: SpiderMeshNode = {
    ips: ['127.0.0.1'],
    host: '127.0.0.1',
    namespace: 'test',
    version: 1,
    node_id: 'node-b',
    topics: ['updates'],
    services: {},
    nodes: {},
    transporters: { websocket: {} },
}

const nodeC: SpiderMeshNode = {
    ips: ['127.0.0.1'],
    host: '127.0.0.1',
    namespace: 'test',
    version: 1,
    node_id: 'node-c',
    topics: ['updates'],
    services: {},
    nodes: {},
    transporters: { websocket: {} },
}

function createDiscoveryMessage(node: SpiderMeshNode): MdnsMessage<SpiderMeshNode> {
    return {
        hi: true,
        node,
        sender_id: node.node_id,
    }
}

async function waitForConnection(transporter: WebsocketTransporter) {
    await firstValueFrom(transporter.pipe(
        filter(event => !!event?.metadata?.connected),
        timeout(5000),
    ))
}

async function expectNoEvent<T>(stream: Observable<T>, waitMs = 400) {
    let hasEvent = false
    const subscription = stream.subscribe(() => {
        hasEvent = true
    })

    await new Promise(resolve => setTimeout(resolve, waitMs))
    subscription.unsubscribe()

    if (hasEvent) {
        throw new Error('Unexpected event received')
    }
}

async function main() {
    const transporterA = new WebsocketTransporter(serverWsUrl, {
        heartbeatIntervalMs: 100,
        reconnectIntervalMs: 500,
    })

    const transporterB = new WebsocketTransporter(clientWsUrl, {
        heartbeatIntervalMs: 100,
        reconnectIntervalMs: 500,
    })

    const transporterC = new WebsocketTransporter(serverWsUrl, {
        heartbeatIntervalMs: 100,
        reconnectIntervalMs: 500,
    })

    try {
        await Promise.all([
            waitForConnection(transporterA),
            waitForConnection(transporterB),
            waitForConnection(transporterC),
        ])

        const discoveryA = firstValueFrom(transporterA.pipe(
            filter((event): event is SpiderMeshNode => !!event?.node_id && event.node_id === nodeB.node_id),
            timeout(5000),
        ))

        const discoveryC = firstValueFrom(transporterC.pipe(
            filter((event): event is SpiderMeshNode => !!event?.node_id && event.node_id === nodeB.node_id),
            timeout(5000),
        ))

        const noDiscoveryOnClient = expectNoEvent(transporterB.pipe(
            filter((event): event is SpiderMeshNode => !!event?.node_id && (event.node_id === nodeA.node_id || event.node_id === nodeC.node_id)),
        ))

        await Promise.all([
            transporterA.broadcast(createDiscoveryMessage(nodeA), []),
            transporterB.broadcast(createDiscoveryMessage(nodeB), []),
            transporterC.broadcast(createDiscoveryMessage(nodeC), []),
        ])

        await Promise.all([discoveryA, discoveryC, noDiscoveryOnClient])

        const rpcMessage = firstValueFrom(transporterB.pipe(
            filter(event => event?.message?.packet?.request_id === 'smoke-request'),
            timeout(5000),
        ))

        const packet: RpcPacket = {
            kind: 'request',
            request_id: 'smoke-request',
            source_node_id: nodeA.node_id,
            target_node_id: nodeB.node_id,
            service: 'GreetingService',
            method: 'hello',
            args: ['world'],
        }

        await transporterA.send(packet, nodeB)

        const rpcEvent = await rpcMessage
        if (JSON.stringify(rpcEvent.message?.packet) !== JSON.stringify(packet)) {
            throw new Error(`Unexpected RPC payload: ${JSON.stringify(rpcEvent.message?.packet)}`)
        }

        await transporterA.send({
            kind: 'cancel',
            request_id: 'allowed-cancel',
            source_node_id: nodeA.node_id,
            target_node_id: nodeB.node_id,
        }, nodeB)

        await firstValueFrom(transporterB.pipe(
            filter(event => event?.message?.packet?.kind === 'cancel' && event.message.packet.request_id === 'allowed-cancel'),
            timeout(5000),
        ))

        await transporterB.send({
            kind: 'request',
            request_id: 'blocked-request',
            source_node_id: nodeB.node_id,
            target_node_id: nodeA.node_id,
            service: 'GreetingService',
            method: 'hello',
            args: ['blocked'],
        }, nodeA)

        await expectNoEvent(transporterA.pipe(
            filter(event => event?.message?.packet?.request_id === 'blocked-request'),
        ))

        await transporterB.send({
            kind: 'cancel',
            request_id: 'blocked-cancel',
            source_node_id: nodeB.node_id,
            target_node_id: nodeA.node_id,
        }, nodeA)

        await expectNoEvent(transporterA.pipe(
            filter(event => event?.message?.packet?.kind === 'cancel' && event.message.packet.request_id === 'blocked-cancel'),
        ))

        const pubsubMessageA = firstValueFrom(transporterA.listen<{ ok: boolean, via: string }>('updates').pipe(
            timeout(5000),
        ))

        const pubsubMessageC = firstValueFrom(transporterC.listen<{ ok: boolean, via: string }>('updates').pipe(
            timeout(5000),
        ))

        await transporterB.publish('updates', { ok: true, via: 'binary-pubsub' })

        const [pubsubEventA, pubsubEventC] = await Promise.all([pubsubMessageA, pubsubMessageC])
        if (!pubsubEventA.ok || pubsubEventA.via !== 'binary-pubsub') {
            throw new Error(`Unexpected pubsub payload on server A: ${JSON.stringify(pubsubEventA)}`)
        }

        if (!pubsubEventC.ok || pubsubEventC.via !== 'binary-pubsub') {
            throw new Error(`Unexpected pubsub payload on server C: ${JSON.stringify(pubsubEventC)}`)
        }

        const offlineEventC = firstValueFrom(transporterC.pipe(
            filter(event => event?.offline === nodeA.node_id),
            timeout(5000),
        ))

        transporterA.close()

        await offlineEventC

        const rawNodeId = 'raw-node'
        const rawSocket = new WebSocket(clientWsUrl)
        await new Promise<void>((resolve, reject) => {
            rawSocket.once('open', () => resolve())
            rawSocket.once('error', reject)
        })

        rawSocket.send(encodeRelayFrame({ type: 'register', node_id: rawNodeId }))
        rawSocket.send(encodeRelayFrame({
            type: 'discovery',
            sender_id: rawNodeId,
        }, Buffer.from(encode({
            ...nodeA,
            node_id: rawNodeId,
        }))))

        const staleOfflineEvent = firstValueFrom(transporterC.pipe(
            filter(event => event?.offline === rawNodeId),
            timeout(5000),
        ))

        await staleOfflineEvent

        console.log('WebSocket binary smoke test passed')
    } finally {
        transporterB.close()
        transporterC.close()
        server.close()
    }
}

await main()
process.exit(0)