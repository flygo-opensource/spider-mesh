import { Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type GreetingService = {
    hello(name: string): Observable<string>
}

const { mesh } = createMesh({
    wsUrl,
    heartbeatIntervalMs: 5000,
    reconnectIntervalMs: 1000,
})
const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
    service: 'GreetingService',
    timeout: 5000,
})

await greeter.wait()

greeter.hello('websocket client').subscribe(message => {
    console.log(message)
})