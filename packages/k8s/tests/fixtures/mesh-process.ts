/**
 * Một process Spider Mesh thật (Topology + Http2Rpc) dùng KubernetesDiscovery với API server giả.
 *
 * `@spider-mesh/tcp` không cài được làm dependency `file:` (các `file:` của chính nó bị Bun phân giải sai
 * chỗ), nên fixture import bản build của tcp, và core từ đúng chỗ tcp dùng để chỉ có một bản core.
 * Cần build tcp trước, như thứ tự trong CI.
 */
import { Microservice, RemoteServiceLinker, SpiderMesh, Topology } from '../../../tcp/node_modules/@spider-mesh/core/build/src/index.js'
import { Http2Rpc } from '../../../tcp/build/src/index.js'
import { KubernetesDiscovery } from '../../src/index.js'

const discovery = new KubernetesDiscovery({
    service: 'mesh',
    namespace: 'apps',
    port: Number(process.env.DISCOVERY_PORT),
    api: { server: process.env.K8S_API! },
})
const topology = new Topology({ discovery })
const mesh = new SpiderMesh({ topology, transporters: [new Http2Rpc()] })

if (process.env.ROLE === 'provider') {
    @Microservice()
    class GreetingService {
        hello(name: string) {
            return `hello ${name}`
        }
    }
    new GreetingService()
    console.log('ready')
    setInterval(() => undefined, 1000)
} else {
    const greeting = RemoteServiceLinker.link<{ hello(name: string): string }>(mesh, { service: 'GreetingService' })
    await greeting.wait()
    console.log(await greeting.hello('k8s'))

    // Pod provider hết ready thì phải rời Topology ngay, không đợi removeUnreachableAfterMs.
    while (topology.list('GreetingService').length > 0) await Bun.sleep(20)
    console.log('removed')
    process.exit(0)
}
