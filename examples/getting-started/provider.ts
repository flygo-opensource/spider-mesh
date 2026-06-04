/**
 * Getting started (ws) — Step 2: a provider node.
 *
 * Exposes GreetingService over the mesh. Note: NO Registry anywhere — the relay does
 * the routing and the WebSocket transporter supplies availability to core as a
 * ServiceDirectory.
 *
 *   bun run examples/getting-started/provider.ts
 */
import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../../src/node.js'

// Any class decorated with @Microservice() self-registers when constructed.
// Service identity is the class name ('GreetingService').
@Microservice()
class GreetingService {
    async hello(name: string) {
        return `hello ${name}`
    }
}

new GreetingService()

const transporter = new WebsocketTransporter()
transporter.connect(process.env.WS_URL || 'ws://127.0.0.1:8787')

const mesh = new SpiderMesh()          // ← registry-free: no constructor argument
mesh.registerTransporter(transporter)  // ← one transporter serves rpc + discovery + pubsub + availability

console.log('provider online — exposing GreetingService')

setInterval(() => undefined, 1000)
