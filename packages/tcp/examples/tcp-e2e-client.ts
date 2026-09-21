import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom, timeout, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const { mesh } = createMesh()
    console.log('TCP e2e client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('TCP e2e client timed out')
        process.exit(1)
    }, 12000)

    try {
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)
        const result = await firstValueFrom(greeter.hello('tcp e2e').pipe(timeout(5000)))
        console.log(result)
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()