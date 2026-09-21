import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { filter, firstValueFrom, timeout as rxTimeout, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const { mesh } = createMesh()
    console.log('TCP failover client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 5000,
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('TCP failover client guard exceeded')
        process.exit(1)
    }, 40000)

    try {
        // Phase 1: wait for all 3 nodes, then send 6 requests
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length >= 3)

        const phase1 = await Promise.all(
            Array.from({ length: 6 }, (_, i) =>
                firstValueFrom(greeter.hello(`phase1-${i}`).pipe(rxTimeout(5000)))
            )
        )
        const phase1Providers = phase1.map(r => r.split(' from ')[1])
        console.log(JSON.stringify({ phase1, phase1UniqueProviders: [...new Set(phase1Providers)].length }))

        // Phase 2: wait for harness to kill one node (nodes.length drops to 2)
        await firstValueFrom(
            greeter.watch().pipe(
                filter(nodes => nodes.length <= 2),
                rxTimeout(15000),
            )
        )
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length === 2)

        // Phase 2: 4 requests — must only hit the 2 surviving nodes, no errors
        const phase2 = await Promise.all(
            Array.from({ length: 4 }, (_, i) =>
                firstValueFrom(greeter.hello(`phase2-${i}`).pipe(rxTimeout(5000)))
            )
        )
        const phase2Providers = phase2.map(r => r.split(' from ')[1])
        console.log(JSON.stringify({ phase2, phase2UniqueProviders: [...new Set(phase2Providers)].length }))

        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
