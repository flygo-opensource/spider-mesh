import { expect, test } from 'bun:test'
import type { AddressInfo } from 'node:net'
import { WebSocketServer } from 'ws'
import { WebsocketTransporter } from '../src/WebsocketTransporter.js'
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

    transporter.close([connectedUrl, failedUrl])

    expect(transporter.status$.value.get(connectedUrl)).toBe('not_connected')
    expect(transporter.status$.value.get(failedUrl)).toBe('not_connected')

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