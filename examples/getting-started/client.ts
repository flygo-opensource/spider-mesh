/**
 * Ví dụ client TCP.
 *
 * Cấu hình giống provider: Discovery ghi node vào Topology, còn Http2Rpc dùng Topology
 * để chọn endpoint và truyền packet.
 *
 *   bun run examples/getting-started/client.ts
 */
import { RemoteServiceLinker, SpiderMesh, Topology } from '@spider-mesh/core'
import { EventBus } from '@spider-mesh/events'
import { TopologyDiscoveryAdapter } from '@spider-mesh/discovery'
import { Http2Pubsub, Http2Rpc } from '../../src/index.js'
import { createDiscovery } from '../helpers/createDiscovery.js'

type GreetingService = {
    hello(name: string): Promise<string>
}

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

const greeter = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })

// Chờ đến khi có ít nhất một provider vừa có service vừa có HTTP/2 endpoint.
await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)

console.log(await greeter.hello('world'))

process.exit(0)
