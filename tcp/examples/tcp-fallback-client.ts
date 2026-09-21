import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

type GhostService = {
    ping(): Promise<string>
}

async function main() {
    createMesh()
    console.log('TCP fallback client connected')

    // No provider running — MICROSERVICE_OFFLINE is expected.
    // The fallback value must be returned instead of throwing.
    const { mesh } = createMesh()
    const service = RemoteServiceLinker.link<GhostService>(mesh, {
        service: 'GhostService',
        fallback: 'fallback-response',
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('TCP fallback client guard exceeded')
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
