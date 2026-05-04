import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../src/node.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

const transporter = new WebsocketTransporter({
    heartbeatIntervalMs: 5000,
    reconnectIntervalMs: 1000,
})
transporter.connect(wsUrl)

@Microservice({ role: 'provider' })
class GreetingService {
    async hello(name: string) {
        return `hello ${name} from provider`
    }
}

new GreetingService()
new SpiderMesh({ transporters: [transporter] })

console.log(`WebSocket provider started at ${wsUrl}`)