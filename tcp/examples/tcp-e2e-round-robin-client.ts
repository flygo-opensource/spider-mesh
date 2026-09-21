import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom, timeout, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const { mesh, registry } = createMesh()
    console.log('TCP round-robin client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('TCP round-robin client timed out', [...registry.nodes$.value.values()].map(node => ({
            node_id: node.node_id,
            version: node.version,
            services: Object.keys(node.services),
            transporters: node.transporters,
        })))
        process.exit(1)
    }, 15000)

    try {
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length >= 2)

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
