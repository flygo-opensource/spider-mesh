import { Observable } from 'rxjs'
import { RemoteServiceLinker, SpiderMesh } from '../src/index.js'
import { WebsocketTransporter } from '../src/transporters/WebsocketTransporter.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

const transporter = new WebsocketTransporter(wsUrl, {
    heartbeatIntervalMs: 5000,
    reconnectIntervalMs: 1000,
})

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