/**
 * App thử trên cluster thật (xem manifest.yaml cùng thư mục). ROLE=provider phục vụ GreetingService;
 * ROLE=client gọi nó mỗi giây và in pod nào trả lời, để thấy round-robin và việc pod vào/ra.
 *
 * Import core từ chỗ tcp dùng, như tests/fixtures/mesh-process.ts, để chỉ có một bản core.
 */
import { Microservice, RemoteServiceLinker, SpiderMesh, Topology } from '../../../tcp/node_modules/@spider-mesh/core/build/src/index.js'
import { Http2Rpc } from '../../../tcp/build/src/index.js'
import { KubernetesDiscovery } from '../../src/index.js'

const pod = process.env.HOSTNAME ?? 'unknown'
const discovery = new KubernetesDiscovery({ service: 'spider-mesh' })
const topology = new Topology({ discovery })
const mesh = new SpiderMesh({ topology, transporters: [new Http2Rpc({ port: 7000 })] })

topology.events$.subscribe(event => {
    if (event.type !== 'node-online' && event.type !== 'node-offline') return
    const reason = event.type === 'node-offline' ? event.reason : undefined
    console.log(JSON.stringify({ at: new Date().toISOString(), pod, event: event.type, host: event.node.host, reason }))
})

if (process.env.ROLE === 'provider') {
    @Microservice()
    class GreetingService {
        hello(from: string) {
            return `${pod} greets ${from}`
        }
    }
    new GreetingService()
    console.log(JSON.stringify({ pod, role: 'provider', ready: true }))
} else {
    const greeting = RemoteServiceLinker.link<{ hello(from: string): string }>(mesh, { service: 'GreetingService' })
    await greeting.wait()
    setInterval(async () => {
        const providers = topology.list('GreetingService').map(node => node.host).sort()
        try {
            const reply = await greeting.hello(pod)
            console.log(JSON.stringify({ at: new Date().toISOString(), mode: discovery.mode, providers, reply }))
        } catch (error) {
            console.log(JSON.stringify({ at: new Date().toISOString(), mode: discovery.mode, providers, error: (error as { code?: string }).code ?? String(error) }))
        }
    }, 1000)
}
