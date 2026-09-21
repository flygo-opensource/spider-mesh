import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { filter, firstValueFrom, timeout as rxTimeout } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

type GreetingService = {
    hello(name: string): Promise<string>
}

async function main() {
    const { mesh } = createMesh()
    console.log('TCP offline client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 3000,
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('TCP offline client guard exceeded')
        process.exit(1)
    }, 25000)

    try {
        // Step 1: wait for provider, make first successful call
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)
        const result = await greeter.hello('offline-test')
        console.log(JSON.stringify({ firstCallOk: true, result }))

        // Step 2: wait until the provider disappears from the registry
        await firstValueFrom(
            greeter.watch().pipe(
                filter(nodes => nodes.length === 0),
                rxTimeout(12000),
            )
        )

        // Step 3: call again after offline — must get MICROSERVICE_OFFLINE
        try {
            await greeter.hello('should-fail')
            console.error('Expected MICROSERVICE_OFFLINE but received a result')
            process.exit(1)
        } catch (error: any) {
            if (error?.code === 'MICROSERVICE_OFFLINE') {
                console.log(JSON.stringify({ offlineDetected: true, code: error.code }))
                process.exit(0)
            }
            console.error('Unexpected error code after provider crash:', error)
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
