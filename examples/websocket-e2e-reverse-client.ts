import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'
console.log('WebSocket reverse e2e client connected')

@Microservice({ role: 'client', mode: 'reverse-e2e' })
class ClientResponderService {
    async helloFromServer(name: string) {
        return `hello ${name} from client`
    }
}

new ClientResponderService()
createMesh({
    wsUrl,
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
})

setInterval(() => undefined, 1000)