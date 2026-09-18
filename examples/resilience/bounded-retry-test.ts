import { createServer, type ServerHttp2Session } from 'node:http2'
import { AddressInfo } from 'node:net'
import { Registry, type SpiderMeshNode } from '@spider-mesh/core'
import { Http2Rpc } from '../../src/index.js'
import { waitFor } from './helpers.js'

const reserve = createServer()
await new Promise<void>(resolve => reserve.listen(0, '127.0.0.1', resolve))
const port = (reserve.address() as AddressInfo).port
await new Promise<void>((resolve, reject) => reserve.close(error => error ? reject(error) : resolve()))

const registry = new Registry()
const rpc = new Http2Rpc(registry)
const reachabilityEvents: string[] = []
registry.events$.subscribe(event => {
    if (event.type.startsWith('endpoint-')) reachabilityEvents.push(event.type)
})
const node = (node_id: string): SpiderMeshNode => ({
    node_id,
    namespace: process.env.SPIDERMESH_NAMESPACE || 'bounded-retry-test',
    host: '127.0.0.1',
    version: 1,
    topics: [],
    services: { BoundedRetryService: {} },
    nodes: {},
    transporters: { http2: { port } },
})

const recoveredServer = createServer()
const recoveredSessions = new Set<ServerHttp2Session>()
recoveredServer.on('session', session => {
    recoveredSessions.add(session)
    session.once('close', () => recoveredSessions.delete(session))
})

try {
    registry.upsertPeer(node('bounded-retry-provider-1'))
    await waitFor(
        () => !!registry.getPeer('bounded-retry-provider-1')
            && registry.getReachability('bounded-retry-provider-1', 'http2') === 'unreachable',
        5000,
        'bounded retry exhaustion',
    )

    await new Promise<void>(resolve => recoveredServer.listen(port, '127.0.0.1', resolve))
    await Bun.sleep(750)
    if (recoveredSessions.size !== 0) {
        throw new Error('Http2Rpc kept retrying after the endpoint was marked unreachable')
    }

    registry.upsertPeer(node('bounded-retry-provider-2'))
    await waitFor(
        () => recoveredSessions.size === 1
            && registry.getReachability('bounded-retry-provider-2', 'http2') === 'reachable',
        5000,
        'fresh connection after a new discovery snapshot',
    )

    console.log(JSON.stringify({
        boundedRetryStopped: true,
        topologyMembershipPreserved: true,
        discoveryRestartedConnection: true,
        reachabilityEvents,
    }))
} finally {
    rpc.unsubscribe()
    for (const session of recoveredSessions) session.destroy()
    await new Promise<void>(resolve => recoveredServer.close(() => resolve()))
}
