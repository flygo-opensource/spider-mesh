import { expect, test } from 'bun:test'
import type { AddressInfo } from 'node:net'
import { WebSocketServer } from 'ws'
import { SpiderMesh, Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { WebsocketTransporter } from '../src/WebsocketTransporter.js'
import { encodeRelayFrame } from '../src/websocketProtocol.js'
import { runBunScript } from './helpers/runBunScript.js'

async function waitFor(predicate: () => boolean, timeoutMs = 2000) {
    const startedAt = Date.now()

    while (!predicate()) {
        if (Date.now() - startedAt > timeoutMs) {
            throw new Error(`Timed out after ${timeoutMs}ms`)
        }

        await Bun.sleep(10)
    }
}

async function createServer() {
    const server = new WebSocketServer({ port: 0, host: '127.0.0.1' })

    await new Promise<void>(resolve => {
        server.once('listening', () => resolve())
    })

    return server
}

test('websocket transporter e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-smoke-test.ts'], 20000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('WebSocket binary smoke test passed')
})

test('websocket transporter exposes connection statuses', async () => {
    const server = await createServer()
    const connectedPort = (server.address() as AddressInfo).port
    const connectedUrl = `ws://127.0.0.1:${connectedPort}`

    const unavailableServer = await createServer()
    const unavailablePort = (unavailableServer.address() as AddressInfo).port
    await new Promise<void>((resolve, reject) => {
        unavailableServer.close(error => {
            if (error) {
                reject(error)
                return
            }

            resolve()
        })
    })

    const failedUrl = `ws://127.0.0.1:${unavailablePort}`
    const transporter = new WebsocketTransporter({ reconnectIntervalMs: 10000 })

    transporter.connect(connectedUrl)
    expect(transporter.status$.value.get(connectedUrl)).toBe('connecting')

    await waitFor(() => transporter.status$.value.get(connectedUrl) === 'connected')
    expect(transporter.status$.value.get(connectedUrl)).toBe('connected')

    transporter.connect(failedUrl)
    expect(transporter.status$.value.get(failedUrl)).toBe('connecting')

    await waitFor(() => transporter.status$.value.get(failedUrl) === 'error')
    expect(transporter.status$.value.get(failedUrl)).toBe('error')

    transporter.close(connectedUrl)
    transporter.close(failedUrl)

    expect(transporter.status$.value.has(connectedUrl)).toBe(false)
    expect(transporter.status$.value.has(failedUrl)).toBe(false)

    // `ws` removes a terminated client from WebSocketServer.clients on the next turn.
    // Calling server.close() in the same turn can miss its close callback under Bun.
    await waitFor(() => server.clients.size === 0)

    await new Promise<void>((resolve, reject) => {
        server.close(error => {
            if (error) {
                reject(error)
                return
            }

            resolve()
        })
    })
})

test('relay loss disables the endpoint without deleting topology membership', async () => {
    const server = await createServer()
    const port = (server.address() as AddressInfo).port
    const remoteNode: SpiderMeshNode = {
        node_id: 'relay-loss-provider',
        namespace: 'default',
        host: '127.0.0.1',
        version: 1,
        topics: [],
        services: { GreetingService: {} },
        nodes: {},
        transporters: { websocket: true },
    }

    server.on('connection', socket => {
        // Cho client gắn message listener sau open event trước khi relay sync directory.
        setTimeout(() => socket.send(encodeRelayFrame({ type: 'hello', me: remoteNode })), 10)
    })

    const transporter = new WebsocketTransporter({ reconnectIntervalMs: 10_000 })
    const topology = new Topology({ discovery: transporter })
    new SpiderMesh({ topology, transporters: [transporter] })
    const events: string[] = []
    topology.events$.subscribe(event => events.push(event.type))

    transporter.connect(`ws://127.0.0.1:${port}`)
    await waitFor(() => topology.getPeer(remoteNode.node_id) != undefined)
    expect(topology.isReachable(remoteNode.node_id, transporter.name)).toBe(true)

    for (const socket of server.clients) socket.terminate()
    await waitFor(() => topology.getReachability(remoteNode.node_id, transporter.name) === 'unreachable')

    expect(topology.getPeer(remoteNode.node_id)?.node_id).toBe(remoteNode.node_id)
    expect(events).toContain('endpoint-unreachable')
    expect(events).not.toContain('node-offline')

    transporter.close()
    await topology.close()
    for (const socket of server.clients) socket.terminate()
    server.close()
})
