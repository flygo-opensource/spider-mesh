import { Observable } from 'rxjs'
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../src/node.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

const transporter = new WebsocketTransporter({
    heartbeatIntervalMs: 5000,
    reconnectIntervalMs: 1000,
})
transporter.connect(wsUrl)

type GreetingService = {
    hello(name: string): Observable<string>
}

const mesh = new SpiderMesh({ transporters: [transporter] })
const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
    service: 'GreetingService',
    timeout: 5000,
})

await greeter.wait()

greeter.hello('websocket client').subscribe(message => {
    console.log(message)
})