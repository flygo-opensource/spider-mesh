import { Microservice, SpiderMesh } from '../src/index.js'
import { WebsocketTransporter } from '../src/transporters/WebsocketTransporter.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'
const transporterOptions = {
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
}

const transporter = new WebsocketTransporter(wsUrl, transporterOptions)

const subscription = transporter.subscribe(event => {
    if (event?.metadata?.connected) {
        console.log('WebSocket reverse e2e client connected')
        subscription.unsubscribe()
    }
})

@Microservice({ role: 'client', mode: 'reverse-e2e' })
class ClientResponderService {
    async helloFromServer(name: string) {
        return `hello ${name} from client`
    }
}

new ClientResponderService()
new SpiderMesh({ transporters: [transporter] })

setInterval(() => undefined, 1000)