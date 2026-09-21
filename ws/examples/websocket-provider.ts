import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

@Microservice({ role: 'provider' })
class GreetingService {
    async hello(name: string) {
        return `hello ${name} from provider`
    }
}

new GreetingService()
createMesh({
    wsUrl,
    heartbeatIntervalMs: 5000,
    reconnectIntervalMs: 1000,
})

console.log(`WebSocket provider started at ${wsUrl}`)