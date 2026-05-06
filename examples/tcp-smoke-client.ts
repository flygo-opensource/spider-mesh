import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom, timeout, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

class SmokeEvent {
    constructor(
        public readonly message: string,
        public readonly sender: string,
    ) { }
}

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const { mesh } = createMesh()
    console.log('TCP smoke client connected')

    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('TCP smoke client timed out')
        process.exit(1)
    }, 15000)

    try {
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)
        const result = await firstValueFrom(greeter.hello('tcp smoke').pipe(timeout(5000)))
        console.log(result)

        await mesh.linkEvent(SmokeEvent).publish(new SmokeEvent('pubsub-ok', 'client'))
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()