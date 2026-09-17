import { firstValueFrom, timeout, type Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const { mesh } = createMesh({
        wsUrl,
        heartbeatIntervalMs: 1000,
        reconnectIntervalMs: 500,
    })
    console.log('WebSocket round-robin client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket round-robin client timed out')
        process.exit(1)
    }, 12000)

    try {
        await greeter.wait()

        const results = await Promise.all([
            firstValueFrom(greeter.hello('round-robin-1').pipe(timeout(5000))),
            firstValueFrom(greeter.hello('round-robin-2').pipe(timeout(5000))),
            firstValueFrom(greeter.hello('round-robin-3').pipe(timeout(5000))),
            firstValueFrom(greeter.hello('round-robin-4').pipe(timeout(5000))),
        ])

        console.log(JSON.stringify({ results }))
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
