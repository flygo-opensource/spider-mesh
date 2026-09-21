import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom, timeout as rxTimeout } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

type GreetingService = {
    hello(name: string): Promise<string>
}

const CONCURRENT_COUNT = 10

async function main() {
    const { mesh } = createMesh()
    console.log('TCP concurrent client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 5000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('TCP concurrent client guard exceeded')
        process.exit(1)
    }, 20000)

    try {
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)

        // Fire CONCURRENT_COUNT calls simultaneously — all must complete correctly
        const results = await Promise.all(
            Array.from({ length: CONCURRENT_COUNT }, (_, i) =>
                firstValueFrom(greeter.hello(`concurrent-${i}`).pipe(rxTimeout(5000)))
            )
        )

        const allCorrect = results.every((r, i) => r.includes(`concurrent-${i}`))
        console.log(JSON.stringify({ concurrentOk: true, count: results.length, allCorrect }))
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
