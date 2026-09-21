import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

// Provider with a method that never resolves — used to trigger MICROSERVICE_RPC_TIMEOUT
@Microservice({ role: 'provider', mode: 'ws-timeout' })
class SlowService {
    async hang(_input: string): Promise<string> {
        await new Promise<never>(() => {})
        return ''
    }
}

new SlowService()
createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500 })

console.log('WebSocket timeout provider ready')

setInterval(() => undefined, 1000)
