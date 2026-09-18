import { expect, test } from 'bun:test'
import { Topology } from '../src/Topology.js'
import type { SpiderMeshNode, TopologyEvent } from '../src/types.js'

const node = (node_id: string): SpiderMeshNode => ({
    host: '127.0.0.1',
    namespace: 'test',
    version: 1,
    node_id,
    topics: [],
    services: { Svc: {} },
    nodes: {},
    transporters: { http2: { port: 1 }, websocket: true },
})

const WINDOW = 150

const offlineEvents = (topology: Topology) => {
    const events: Extract<TopologyEvent, { type: 'node-offline' }>[] = []
    topology.events$.subscribe(event => {
        if (event.type === 'node-offline') events.push(event)
    })
    return events
}

test('removes a node once every transporter has been unreachable for the whole window', async () => {
    const topology = new Topology({ removeUnreachableAfterMs: WINDOW })
    const offline = offlineEvents(topology)
    topology.upsertRemote(node('a'))

    topology.reportReachability({ node_id: 'a', transporter: 'http2', status: 'unreachable' })
    await Bun.sleep(WINDOW / 2)
    expect(topology.getPeer('a')).toBeDefined()

    await Bun.sleep(WINDOW * 1.5)
    expect(topology.getPeer('a')).toBeUndefined()
    expect(offline.map(event => [event.node.node_id, event.reason])).toEqual([['a', 'unreachable']])
    await topology.close()
})

test('repeated unreachable reports do not restart the window', async () => {
    // Transporter báo lại mỗi lần thử kết nối thất bại; nếu mỗi lần báo làm mới bộ đếm thì node
    // chết sẽ không bao giờ bị xoá.
    const topology = new Topology({ removeUnreachableAfterMs: WINDOW })
    topology.upsertRemote(node('a'))

    const reporter = setInterval(() => {
        topology.reportReachability({ node_id: 'a', transporter: 'http2', status: 'unreachable' })
    }, 20)
    topology.reportReachability({ node_id: 'a', transporter: 'http2', status: 'unreachable' })

    await Bun.sleep(WINDOW * 2.5)
    clearInterval(reporter)
    expect(topology.getPeer('a')).toBeUndefined()
    await topology.close()
})

test('keeps a node that recovers before the window ends', async () => {
    const topology = new Topology({ removeUnreachableAfterMs: WINDOW })
    topology.upsertRemote(node('a'))

    topology.reportReachability({ node_id: 'a', transporter: 'http2', status: 'unreachable' })
    await Bun.sleep(WINDOW / 2)
    topology.reportReachability({ node_id: 'a', transporter: 'http2', status: 'reachable' })

    await Bun.sleep(WINDOW * 2)
    expect(topology.getPeer('a')).toBeDefined()
    await topology.close()
})

test('keeps a node while any transporter still reaches it', async () => {
    const topology = new Topology({ removeUnreachableAfterMs: WINDOW })
    topology.upsertRemote(node('a'))

    topology.reportReachability({ node_id: 'a', transporter: 'http2', status: 'unreachable' })
    topology.reportReachability({ node_id: 'a', transporter: 'websocket', status: 'reachable' })

    await Bun.sleep(WINDOW * 2)
    expect(topology.getPeer('a')).toBeDefined()
    await topology.close()
})

test('does nothing unless removeUnreachableAfterMs is set', async () => {
    const topology = new Topology()
    topology.upsertRemote(node('a'))
    topology.reportReachability({ node_id: 'a', transporter: 'http2', status: 'unreachable' })

    await Bun.sleep(WINDOW * 2)
    expect(topology.getPeer('a')).toBeDefined()
    await topology.close()
})
