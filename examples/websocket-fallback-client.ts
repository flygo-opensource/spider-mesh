import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type GhostService = {
    ping(): Observable<string>
}

async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500 })
    console.log('WebSocket fallback client connected')

    // No provider running — MICROSERVICE_OFFLINE is expected.
    // The configured fallback value must be returned instead of throwing.
    const service = RemoteServiceLinker.link<GhostService>(mesh, {
        service: 'GhostService',
        fallback: 'fallback-response',
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket fallback client guard exceeded')
        process.exit(1)
    }, 10000)

    try {
        const result = await firstValueFrom(service.ping())
        console.log(JSON.stringify({ fallbackReceived: true, value: result }))
        process.exit(0)
    } catch (error) {
        console.error('Unexpected error (expected fallback, not throw):', error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
