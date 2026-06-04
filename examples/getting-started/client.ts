/**
 * Getting started (tcp) — a client node.
 *
 * Same construction as the provider: one shared Registry injected into the three
 * transporters. The client discovers the provider over multicast, then calls it.
 *
 *   bun run examples/getting-started/client.ts
 */
import { RemoteServiceLinker, Registry, SpiderMesh } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '../../src/index.js'

type GreetingService = {
    hello(name: string): Promise<string>
}

const mesh = new SpiderMesh()
const registry = new Registry()

mesh.registerTransporter(new UdpDiscovery(registry))
mesh.registerTransporter(new Http2Rpc(registry))
mesh.registerTransporter(new Http2Pubsub(registry))

const greeter = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })

// Wait until discovery has found at least one provider for the service.
await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)

console.log(await greeter.hello('world'))

process.exit(0)
