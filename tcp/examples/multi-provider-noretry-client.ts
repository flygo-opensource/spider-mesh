/**
 * Multi-provider no-retry client.
 *
 * Waits for one provider, then hammers calls with `retry: 0` for a few seconds. A SECOND
 * provider is brought up (by the orchestrator) WHILE this loop runs, so it spends time
 * half-formed (service advertised, Http2Rpc port not yet bound). Routing must skip that
 * half-formed peer — otherwise a round-robin pick would throw MICROSERVICE_OFFLINE and a
 * no-retry call would fail. Prints ALL_OK only if every call succeeded.
 */
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

type GreetingService = { hello(name: string): Promise<string> }

const { mesh } = createMesh()
const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
    service: 'GreetingService',
    timeout: 2000,
    retry: 0,
})

await greeter.wait(() => mesh.listRpcNodes('GreetingService').length >= 1)
console.log('CLIENT_LOOPING')

const startedAt = Date.now()
let calls = 0
try {
    while (Date.now() - startedAt < 3500) {
        await greeter.hello(`x${calls++}`)
    }
    console.log(`ALL_OK calls=${calls}`)
    process.exit(0)
} catch (error: any) {
    console.error('CALL_FAILED', error?.code, error?.message)
    process.exit(1)
}
