import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { filter, firstValueFrom, timeout as rxTimeout, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500 })
    console.log('WebSocket reconnect client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 5000,
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket reconnect client guard exceeded')
        process.exit(1)
    }, 30000)

    try {
        // Step 1: wait for provider-v1 and make first successful call
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)
        const firstResult = await greeter.hello('reconnect-first')
        console.log(JSON.stringify({ firstCallOk: true, result: firstResult }))

        // Step 2: wait for provider-v1 to go offline (killed by harness)
        await firstValueFrom(
            greeter.watch().pipe(
                filter(nodes => nodes.length === 0),
                rxTimeout(12000),
            )
        )
        console.log(JSON.stringify({ providerOffline: true }))

        // Step 3: wait for provider-v2 to reconnect and become discoverable
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)

        // Step 4: make second successful call after reconnect
        const secondResult = await firstValueFrom(
            greeter.hello('reconnect-second').pipe(rxTimeout(5000))
        )
        console.log(JSON.stringify({ reconnectOk: true, secondResult }))
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
