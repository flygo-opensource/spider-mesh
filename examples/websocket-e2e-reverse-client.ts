import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../src/index.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'
const transporterOptions = {
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
}

const transporter = new WebsocketTransporter(transporterOptions)
transporter.connect(wsUrl)
console.log('WebSocket reverse e2e client connected')

@Microservice({ role: 'client', mode: 'reverse-e2e' })
class ClientResponderService {
    async helloFromServer(name: string) {
        return `hello ${name} from client`
    }
}

new ClientResponderService()
new SpiderMesh({ transporters: [transporter] })

setInterval(() => undefined, 1000)