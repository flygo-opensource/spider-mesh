import { firstValueFrom, filter, timeout, type Observable } from 'rxjs'
import WebSocket from 'ws'
import { WebsocketRelayServer } from '../src/relay-server.js'
import { WebsocketTransporter } from '../src/node.js'
import { encodeRelayFrame } from '../src/websocketProtocol.js'
import type { DiscoveryEvent, MdnsMessage, RpcPacket, SpiderMeshNode } from '@spider-mesh/core'

const port = 8800 + Math.floor(Math.random() * 200)
const baseWsUrl = `ws://127.0.0.1:${port}`
const serverWsUrl = `${baseWsUrl}?role=server`
const clientWsUrl = `${baseWsUrl}?role=client`
const server = new WebsocketRelayServer({
    port,
    host: '127.0.0.1',
    isServerConnection: (_socket, request) => new URL(request.url || '/', baseWsUrl).searchParams.get('role') === 'server',
})

const nodeA: SpiderMeshNode = {
    host: '127.0.0.1',
    namespace: 'test',
    version: 1,
    node_id: 'node-a',
    topics: [],
    services: { GreetingService: {} },
    nodes: {},
    transporters: { websocket: {} },
}

const nodeB: SpiderMeshNode = {
    host: '127.0.0.1',
    namespace: 'test',
    version: 1,
    node_id: 'node-b',
    topics: [],
    services: {},
    nodes: {},
    transporters: { websocket: {} },
}

const nodeC: SpiderMeshNode = {
    host: '127.0.0.1',
    namespace: 'test',
    version: 1,
    node_id: 'node-c',
    topics: [],
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
    const transporterA = new WebsocketTransporter({
        heartbeatIntervalMs: 100,
        reconnectIntervalMs: 500,
    })
    transporterA.connect(serverWsUrl)

    const transporterB = new WebsocketTransporter({
        heartbeatIntervalMs: 100,
        reconnectIntervalMs: 500,
    })
    transporterB.connect(clientWsUrl)

    const transporterC = new WebsocketTransporter({
        heartbeatIntervalMs: 100,
        reconnectIntervalMs: 500,
    })
    transporterC.connect(serverWsUrl)

    try {
        const discoveryA = firstValueFrom(transporterA.pipe(
            filter((event): event is DiscoveryEvent => !!event?.discovered?.node_id && event.discovered.node_id === nodeB.node_id),
            timeout(5000),
        ))

        const discoveryC = firstValueFrom(transporterC.pipe(
            filter((event): event is DiscoveryEvent => !!event?.discovered?.node_id && event.discovered.node_id === nodeB.node_id),
            timeout(5000),
        ))

        const noDiscoveryOnClient = expectNoEvent(transporterB.pipe(
            filter((event): event is DiscoveryEvent => !!event?.discovered?.node_id && (event.discovered.node_id === nodeA.node_id || event.discovered.node_id === nodeC.node_id)),
        ))

        await Promise.all([
            transporterA.broadcast(createDiscoveryMessage(nodeA)),
            transporterB.broadcast(createDiscoveryMessage(nodeB)),
            transporterC.broadcast(createDiscoveryMessage(nodeC)),
        ])

        await Promise.all([discoveryA, discoveryC, noDiscoveryOnClient])

        const rpcMessage = firstValueFrom(transporterB.pipe(
            filter(event => event?.rpc?.packet?.request_id === 'smoke-request'),
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

        await transporterA.send(packet, nodeB.node_id)

        const rpcEvent = await rpcMessage
        if (JSON.stringify(rpcEvent.rpc?.packet) !== JSON.stringify(packet)) {
            throw new Error(`Unexpected RPC payload: ${JSON.stringify(rpcEvent.rpc?.packet)}`)
        }

        await transporterA.send({
            kind: 'cancel',
            request_id: 'allowed-cancel',
            source_node_id: nodeA.node_id,
            target_node_id: nodeB.node_id,
        }, nodeB.node_id)

        await firstValueFrom(transporterB.pipe(
            filter(event => event?.rpc?.packet?.kind === 'cancel' && event.rpc.packet.request_id === 'allowed-cancel'),
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
        }, nodeA.node_id)

        await expectNoEvent(transporterA.pipe(
            filter(event => event?.rpc?.packet?.request_id === 'blocked-request'),
        ))

        await transporterB.send({
            kind: 'cancel',
            request_id: 'blocked-cancel',
            source_node_id: nodeB.node_id,
            target_node_id: nodeA.node_id,
        }, nodeA.node_id)

        await expectNoEvent(transporterA.pipe(
            filter(event => event?.rpc?.packet?.kind === 'cancel' && event.rpc.packet.request_id === 'blocked-cancel'),
        ))

        const sharedStreamA = transporterA.listen<{ ok: boolean, via: string }>('updates')
        const pubsubMessageA = firstValueFrom(sharedStreamA.pipe(
            timeout(5000),
        ))

        const pubsubMessageC = firstValueFrom(transporterC.listen<{ ok: boolean, via: string }>('updates').pipe(
            timeout(5000),
        ))

        await new Promise(resolve => setTimeout(resolve, 50))

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

        transporterA.close(serverWsUrl)

        await offlineEventC

        const rawNodeId = 'raw-node'
        const rawSocket = new WebSocket(clientWsUrl)
        await new Promise<void>((resolve, reject) => {
            rawSocket.once('open', () => resolve())
            rawSocket.once('error', reject)
        })

        rawSocket.send(encodeRelayFrame({
            type: 'hello',
            me: {
                ...nodeA,
                node_id: rawNodeId,
            },
        }))

        const staleOfflineEvent = firstValueFrom(transporterC.pipe(
            filter(event => event?.offline === rawNodeId),
            timeout(5000),
        ))

        rawSocket.close()
        await staleOfflineEvent

        console.log('WebSocket binary smoke test passed')
    } finally {
        transporterB.close(clientWsUrl)
        transporterC.close(serverWsUrl)
        server.close()
    }
}

await main()
process.exit(0)