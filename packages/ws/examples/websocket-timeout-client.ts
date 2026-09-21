import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type SlowService = {
    hang(input: string): Observable<string>
}

async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500 })
    console.log('WebSocket timeout client connected')

    // timeout: 2000 — call must be cancelled and throw MICROSERVICE_RPC_TIMEOUT
    const service = RemoteServiceLinker.link<SlowService>(mesh, {
        service: 'SlowService',
        timeout: 2000,
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket timeout client guard exceeded')
        process.exit(1)
    }, 12000)

    try {
        await service.wait()

        try {
            await firstValueFrom(service.hang('trigger-timeout'))
            console.error('Expected MICROSERVICE_RPC_TIMEOUT but received a result')
            process.exit(1)
        } catch (error: any) {
            if (error?.code === 'MICROSERVICE_RPC_TIMEOUT') {
                console.log(JSON.stringify({ timeoutDetected: true, code: error.code }))
                process.exit(0)
            }
            console.error('Unexpected error (expected MICROSERVICE_RPC_TIMEOUT):', error)
            process.exit(1)
        }
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
