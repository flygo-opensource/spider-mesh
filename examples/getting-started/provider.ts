/**
 * Getting started (tcp) — a provider node.
 *
 * tcp discovers peers over UDP multicast, so there is NO relay process. Because core is
 * registry-free, tcp owns its own routing: construct ONE shared Registry and inject it
 * into all three transporters. UdpDiscovery fills the registry and exposes it to core as
 * a ServiceDirectory; Http2Rpc / Http2Pubsub read it for routing.
 *
 * Run provider and client in two terminals (same host / LAN):
 *   bun run examples/getting-started/provider.ts
 */
import { Microservice, Registry, SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '../../src/index.js'

@Microservice()
class GreetingService {
    async hello(name: string) {
        return `hello ${name}`
    }
}

new GreetingService()

const mesh = new SpiderMesh()    // ← core holds no registry
const registry = new Registry()  // ← tcp's own routing table, shared by the three transporters

mesh.registerTransporter(new UdpDiscovery(registry))
mesh.registerTransporter(new Http2Rpc(registry))
mesh.registerTransporter(new Http2Pubsub(registry))

console.log('provider online — exposing GreetingService')

setInterval(() => undefined, 1000)
