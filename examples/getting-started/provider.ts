/**
 * Ví dụ provider TCP.
 *
 * Topology nhận UDP Discovery trong constructor; SpiderMesh nhận Topology và Http2Rpc.
 * Http2Pubsub là event transporter riêng nhưng đọc cùng một Topology.
 *
 * Chạy provider và client ở hai terminal trong cùng LAN:
 *   bun run examples/getting-started/provider.ts
 */
import { Microservice, SpiderMesh, Topology } from '@spider-mesh/core'
import { EventBus } from '@spider-mesh/events'
import { TopologyDiscoveryAdapter } from '../../src/index.js'
import { Http2Pubsub, Http2Rpc } from '../../src/index.js'
import { createDiscovery } from '../helpers/createDiscovery.js'

@Microservice()
class GreetingService {
    async hello(name: string) {
        return `hello ${name}`
    }
}

new GreetingService()

const topology = new Topology({
    discovery: new TopologyDiscoveryAdapter(createDiscovery(), { heartbeatIntervalMs: 5_000 }),
    staleAfterMs: 15_000,
})
const mesh = new SpiderMesh({
    topology,
    transporters: [new Http2Rpc()],
})
const events = new EventBus({ mesh })

events.registerTransporter(new Http2Pubsub(topology))

console.log('provider online — exposing GreetingService')

setInterval(() => undefined, 1000)
