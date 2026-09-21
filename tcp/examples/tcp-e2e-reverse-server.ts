import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { firstValueFrom, timeout, type Observable } from 'rxjs'
import { createMesh } from './helpers/createMesh.js'

type ClientResponderService = {
    helloFromServer(name: string): Observable<string>
}

async function main() {
    const { mesh } = createMesh()
    console.log('TCP reverse e2e server connected')

    const responder = RemoteServiceLinker.link<ClientResponderService>(mesh, {
        service: 'ClientResponderService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('TCP reverse e2e server timed out')
        process.exit(1)
    }, 12000)

    try {
        await responder.wait(() => mesh.listRpcNodes('ClientResponderService').length > 0)
        const result = await firstValueFrom(responder.helloFromServer('from server').pipe(timeout(5000)))
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