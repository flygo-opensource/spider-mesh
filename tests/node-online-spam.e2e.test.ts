import { expect, test } from 'bun:test'
import type { SpiderMeshNode } from '@spider-mesh/core'
import type { DiscoveryMessage } from '../src/discoveryTypes.js'
import { WebsocketRelayServer } from '../src/WebsocketRelayServer.js'
import { WebsocketTransporter } from '../src/WebsocketTransporter.js'

async function waitFor(predicate: () => boolean, timeoutMs = 3000) {
    const startedAt = Date.now()
    while (!predicate()) {
        if (Date.now() - startedAt > timeoutMs) {
            throw new Error(`Timed out after ${timeoutMs}ms`)
        }
        await Bun.sleep(10)
    }
}

function makeNode(nodeId: string, services: Record<string, object> = {}): SpiderMeshNode {
    return {
        node_id: nodeId,
        namespace: 'test',
        host: 'localhost',
        services,
        transporters: { WebsocketTransporter: true },
        topics: [],
        nodes: {},
        version: 1,
    }
}

function discoveryMessage(node: SpiderMeshNode): DiscoveryMessage<SpiderMeshNode> {
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

async function createRelay() {
    const relay = new WebsocketRelayServer({ port: 0, host: '127.0.0.1' })
    const port = relay.port!
    const url = `ws://127.0.0.1:${port}`
    return { relay, url }
}

async function createTransporter(url: string) {
    const t = new WebsocketTransporter({ heartbeatIntervalMs: 60000, reconnectIntervalMs: 60000 })
    t.connect(url)
    await waitFor(() => t.status$.value.get(url) === 'connected')
    return t
}

// Bug: khi gửi request tới node, event discovered bị fire thêm lần nữa
// cho những node đã được biết trước đó (do relay gọi #syncServerConnections)
test('node online event should not fire for already-known nodes when sending requests', async () => {
    const { relay, url } = await createRelay()
    const PROVIDER_ID = 'provider-spam-test'
    const CLIENT_ID = 'client-spam-test'

    const providerTransporter = await createTransporter(url)

    // Provider xử lý RPC request và trả về response
    providerTransporter.subscribe((event: any) => {
        if (event?.rpc?.kind === 'request') {
            void providerTransporter.send({
                kind: 'response',
                request_id: event.rpc.request_id,
                destination_node_id: event.rpc.sender_node_id,
                data: 'pong',
                completed: true,
            }).catch(() => undefined)
        }
    })

    const clientTransporter = await createTransporter(url)

    const discoveredIds: string[] = []
    clientTransporter.subscribe((event: any) => {
        if (event?.data?.node_id) {
            discoveredIds.push(event.data.node_id)
        }
    })

    // Announce cả hai node lên relay
    await providerTransporter.broadcast(discoveryMessage(makeNode(PROVIDER_ID, { PingService: {} })))
    await clientTransporter.broadcast(discoveryMessage(makeNode(CLIENT_ID)))

    // Chờ client discover được provider
    await waitFor(() => discoveredIds.includes(PROVIDER_ID), 3000)
    // Chờ các event ban đầu ổn định
    await Bun.sleep(200)

    const countAfterSetup = discoveredIds.length

    // Gửi nhiều request từ client → provider
    const REQUEST_COUNT = 5
    for (let i = 0; i < REQUEST_COUNT; i++) {
        const reqId = `ping-${i}`

        const responseReceived = new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(
                () => reject(new Error(`Request ${reqId} timed out`)),
                3000,
            )
            const sub = clientTransporter.subscribe((event: any) => {
                if (event?.rpc?.kind === 'response' && event.rpc.request_id === reqId) {
                    clearTimeout(timeout)
                    sub.unsubscribe()
                    resolve()
                }
            })
        })

        await clientTransporter.send({
            kind: 'request',
            request_id: reqId,
            sender_node_id: CLIENT_ID,
            destination_node_id: PROVIDER_ID,
            service: 'PingService',
            method: 'ping',
            args: [],
        })

        await responseReceived
    }

    // Cho thêm thời gian để event spurious có thể đến
    await Bun.sleep(100)

    const newEvents = discoveredIds.slice(countAfterSetup)

    // Không được có thêm discovered event nào khi gửi request
    expect(newEvents).toEqual([])

    relay.close()
})

// Bug: khi một node reconnect, relay gọi #syncServerConnections() →
// tất cả các node nhận lại hello cho những node đã biết → discovered spam
test('node online event should not fire for already-known nodes on reconnection', async () => {
    const { relay, url } = await createRelay()
    const NODE_A = 'node-a-reconnect-test'
    const NODE_B = 'node-b-reconnect-test'
    const NODE_C = 'node-c-reconnect-test'

    const transporterA = await createTransporter(url)
    const transporterB = await createTransporter(url)

    const discoveredByA: string[] = []
    transporterA.subscribe((event: any) => {
        if (event?.data?.node_id) {
            discoveredByA.push(event.data.node_id)
        }
    })

    // A và B announce bản thân
    await transporterA.broadcast(discoveryMessage(makeNode(NODE_A)))
    await transporterB.broadcast(discoveryMessage(makeNode(NODE_B)))

    // Chờ A discover B
    await waitFor(() => discoveredByA.includes(NODE_B), 3000)
    await Bun.sleep(200)

    const countAfterSetup = discoveredByA.length

    // C connect vào relay (gây #syncServerConnections → A nhận hello của B lần nữa)
    const transporterC = await createTransporter(url)
    await transporterC.broadcast(discoveryMessage(makeNode(NODE_C)))

    // Chờ A discover C
    await waitFor(() => discoveredByA.includes(NODE_C), 3000)
    await Bun.sleep(200)

    // Kiểm tra: A không nên nhận lại discovered event cho B (đã biết trước)
    const redundantBEvents = discoveredByA
        .slice(countAfterSetup)
        .filter(id => id === NODE_B)

    expect(redundantBEvents).toEqual([])

    relay.close()
})

// Bug: #syncServerConnections gửi hello cho tất cả các node → mỗi node nhận
// discovered cho chính mình (self-discovery) và các node đã biết
test('node should not discover itself via relay sync', async () => {
    const { relay, url } = await createRelay()
    const NODE_SELF = 'node-self-discovery-test'
    const NODE_OTHER = 'node-other-discovery-test'

    const selfTransporter = await createTransporter(url)
    const otherTransporter = await createTransporter(url)

    const discoveredBySelf: string[] = []
    selfTransporter.subscribe((event: any) => {
        if (event?.data?.node_id) {
            discoveredBySelf.push(event.data.node_id)
        }
    })

    await selfTransporter.broadcast(discoveryMessage(makeNode(NODE_SELF, { SomeService: {} })))
    await otherTransporter.broadcast(discoveryMessage(makeNode(NODE_OTHER)))

    // Chờ initial discovery
    await waitFor(() => discoveredBySelf.includes(NODE_OTHER), 3000)
    await Bun.sleep(200)

    // Self không nên bị discover chính mình
    const selfDiscovery = discoveredBySelf.filter(id => id === NODE_SELF)
    expect(selfDiscovery).toEqual([])

    relay.close()
})
