import { createServer, type ServerHttp2Session } from 'node:http2'
import { AddressInfo } from 'node:net'
import { Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { Http2Rpc } from '../../src/index.js'
import { waitFor } from './helpers.js'

// Kết nối HTTP/2 là bằng chứng node còn sống. Discovery (UDP) chỉ để tìm thấy nhau và không phát
// lại, nên Http2Rpc phải tự nối lại khi mạng phục hồi, và chỉ dừng khi node rời Topology.

const reserve = createServer()
await new Promise<void>(resolve => reserve.listen(0, '127.0.0.1', resolve))
const port = (reserve.address() as AddressInfo).port
await new Promise<void>((resolve, reject) => reserve.close(error => error ? reject(error) : resolve()))

const topology = new Topology()
const rpc = new Http2Rpc(topology)
const reachabilityEvents: string[] = []
topology.events$.subscribe(event => {
    if (event.type.startsWith('endpoint-')) reachabilityEvents.push(event.type)
})
const node: SpiderMeshNode = {
    node_id: 'reconnect-provider',
    namespace: process.env.SPIDERMESH_NAMESPACE || 'reconnect-test',
    host: '127.0.0.1',
    version: 1,
    topics: [],
    services: { ReconnectService: {} },
    nodes: {},
    transporters: { http2: { port } },
}

const server = createServer()
const sessions = new Set<ServerHttp2Session>()
server.on('session', session => {
    sessions.add(session)
    session.once('close', () => sessions.delete(session))
})

try {
    // 1. Node được discovery thấy nhưng chưa có gì lắng nghe: sau vài lần thử thì bị đánh dấu.
    topology.upsertRemote(node)
    await waitFor(
        () => topology.getReachability(node.node_id, 'http2') === 'unreachable',
        5000,
        'provider marked unreachable',
    )

    // 2. "Mạng phục hồi". Không upsert lại, không có discovery nào gõ cửa: Http2Rpc phải tự nối lại.
    await new Promise<void>(resolve => server.listen(port, '127.0.0.1', resolve))
    await waitFor(
        () => sessions.size === 1 && topology.getReachability(node.node_id, 'http2') === 'reachable',
        5000,
        'reconnect after recovery without a new discovery snapshot',
    )
    const recovered = reachabilityEvents.includes('endpoint-recovered')

    // 3. Node rời Topology: vòng thử phải dừng, không mở thêm kết nối nào.
    for (const session of sessions) session.destroy()
    topology.removeRemote(node.node_id)
    await Bun.sleep(1500)
    const stoppedAfterRemoval = sessions.size === 0

    console.log(JSON.stringify({
        markedUnreachable: true,
        recoveredWithoutDiscovery: recovered,
        stoppedAfterRemoval,
        reachabilityEvents,
    }))
} finally {
    rpc.unsubscribe()
    for (const session of sessions) session.destroy()
    await new Promise<void>(resolve => server.close(() => resolve()))
}
