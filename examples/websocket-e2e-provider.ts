import { Microservice, SpiderMesh } from '../src/index.js'
import { WebsocketTransporter } from '../src/transporters/WebsocketTransporter.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'
const providerId = process.env.PROVIDER_ID || 'provider'

const transporter = new WebsocketTransporter(wsUrl, {
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
})

@Microservice({ role: 'provider', mode: 'e2e' })
class GreetingService {
    async hello(name: string) {
        return `hello ${name} from ${providerId}`
    }
}

new GreetingService()
new SpiderMesh({ transporters: [transporter] })

console.log(`WebSocket e2e provider ready at ${wsUrl} (${providerId})`)

setInterval(() => undefined, 1000)