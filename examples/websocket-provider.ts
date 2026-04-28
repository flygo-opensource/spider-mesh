import { Microservice, SpiderMesh } from '../src/index.js'
import { WebsocketTransporter } from '../src/transporters/WebsocketTransporter.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

const transporter = new WebsocketTransporter(wsUrl, {
    heartbeatIntervalMs: 5000,
    reconnectIntervalMs: 1000,
})

@Microservice({ role: 'provider' })
class GreetingService {
    async hello(name: string) {
        return `hello ${name} from provider`
    }
}

new GreetingService()
new SpiderMesh({ transporters: [transporter] })

console.log(`WebSocket provider started at ${wsUrl}`)