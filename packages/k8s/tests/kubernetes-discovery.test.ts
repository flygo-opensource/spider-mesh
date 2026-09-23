import { afterEach, expect, test } from 'bun:test'
import { BehaviorSubject } from 'rxjs'
import type { SpiderMeshNode, TopologyDiscoveryContext } from '@spider-mesh/core'
import { KubernetesDiscovery, type KubernetesDiscoveryOptions } from '../src/index.js'
import { FakeKubernetesApi, FakePeer, freePort, node, until } from './helpers/fakes.js'

const cleanups: (() => void)[] = []
afterEach(() => {
    while (cleanups.length) cleanups.pop()!()
})

/** Context ghi lại mọi upsert/remove, như Topology thấy. */
function recordingContext(local = node('local')) {
    const localNode$ = new BehaviorSubject<SpiderMeshNode>(local)
    const nodes = new Map<string, SpiderMeshNode>()
    const removed: string[] = []
    const context: TopologyDiscoveryContext = {
        localNode$,
        upsertRemote: remote => { nodes.set(remote.node_id, remote) },
        removeRemote: node_id => {
            nodes.delete(node_id)
            removed.push(node_id)
        },
    }
    return { context, localNode$, nodes, removed }
}

function api() {
    const fake = new FakeKubernetesApi()
    cleanups.push(() => fake.close())
    return fake
}

function peer(remote: SpiderMeshNode, hostname?: string) {
    const fake = new FakePeer(remote, hostname)
    cleanups.push(() => fake.close())
    return fake
}

async function discovery(options: Partial<KubernetesDiscoveryOptions> & { api?: KubernetesDiscoveryOptions['api'] }) {
    const warnings: string[] = []
    const instance = new KubernetesDiscovery({
        service: 'mesh',
        namespace: 'apps',
        port: await freePort(),
        onWarning: message => warnings.push(message),
        ...options,
    })
    cleanups.push(() => instance.close())
    return { instance, warnings }
}

test('lists EndpointSlices and pulls every ready pod, using the pod address as host', async () => {
    const k8s = api()
    const a = peer(node('a'))
    const b = peer(node('b'))
    k8s.setSlice('mesh-1', [{ address: '127.0.0.1' }], a.port)
    k8s.setSlice('mesh-2', [{ address: '127.0.0.1' }], b.port)

    const { instance, warnings } = await discovery({ api: { server: k8s.url, token: () => 'secret' } })
    const { context, nodes } = recordingContext()
    instance.bind(context)

    await until(() => nodes.size === 2, 3000, 'two nodes')
    expect(instance.mode).toBe('api')
    expect(nodes.get('a')?.host).toBe('127.0.0.1')
    expect(k8s.authorizations[0]).toBe('Bearer secret')
    expect(warnings).toEqual([])
})

test('streams node updates and removes a pod as soon as it stops being ready', async () => {
    const k8s = api()
    const a = peer(node('a'))
    const b = peer(node('b'))
    k8s.setSlice('mesh-1', [{ address: '127.0.0.1' }], a.port)
    k8s.setSlice('mesh-2', [{ address: '127.0.0.1' }], b.port)

    const { instance } = await discovery({ api: { server: k8s.url } })
    const { context, nodes, removed } = recordingContext()
    instance.bind(context)
    await until(() => nodes.size === 2, 3000, 'two nodes')

    a.push({ ...node('a', 2), services: { Greeting: {} } })
    await until(() => nodes.get('a')?.version === 2, 3000, 'version 2')
    expect(nodes.get('a')?.services).toEqual({ Greeting: {} })

    expect(await instance.verify('b')).toBe('alive')
    k8s.setSlice('mesh-2', [{ address: '127.0.0.1', ready: false }], b.port)
    await until(() => removed.includes('b'), 3000, 'b removed')
    expect(nodes.has('a')).toBe(true)
    expect(await instance.verify('b')).toBe('unknown')

    k8s.deleteSlice('mesh-1')
    await until(() => removed.includes('a'), 3000, 'a removed')
})

test('relists after the watch expires (410) and keeps following changes', async () => {
    const k8s = api()
    const a = peer(node('a'))
    const b = peer(node('b'))
    k8s.setSlice('mesh-1', [{ address: '127.0.0.1' }], a.port)

    const { instance } = await discovery({ api: { server: k8s.url } })
    const { context, nodes } = recordingContext()
    instance.bind(context)
    await until(() => nodes.size === 1 && k8s.watches === 1, 3000, 'first watch')

    k8s.expire()
    await until(() => k8s.lists === 2 && k8s.watches === 2, 3000, 'relist and rewatch')

    k8s.setSlice('mesh-2', [{ address: '127.0.0.1' }], b.port)
    await until(() => nodes.has('b'), 3000, 'b after relist')
})

test('a dropped /node stream reconnects without removing the node', async () => {
    const k8s = api()
    const a = peer(node('a'))
    k8s.setSlice('mesh-1', [{ address: '127.0.0.1' }], a.port)

    const { instance } = await discovery({ api: { server: k8s.url } })
    const { context, nodes, removed } = recordingContext()
    instance.bind(context)
    await until(() => nodes.has('a'), 3000, 'a')

    a.dropStreams()
    await until(() => a.connections >= 2, 3000, 'reconnect')
    a.push(node('a', 5))
    await until(() => nodes.get('a')?.version === 5, 3000, 'update after reconnect')
    expect(removed).toEqual([])
})

test('a new pod reusing an address replaces the old node', async () => {
    const k8s = api()
    const pod = peer(node('old-pod'))
    k8s.setSlice('mesh-1', [{ address: '127.0.0.1' }], pod.port)

    const { instance } = await discovery({ api: { server: k8s.url } })
    const { context, nodes, removed } = recordingContext()
    instance.bind(context)
    await until(() => nodes.has('old-pod'), 3000, 'old pod')

    pod.push(node('new-pod'))
    await until(() => nodes.has('new-pod'), 3000, 'new pod')
    expect(removed).toEqual(['old-pod'])
})

test('ignores its own pod and pods of another mesh namespace', async () => {
    const k8s = api()
    const other = peer(node('stranger', 1, 'another-mesh'))
    const port = await freePort()
    k8s.setSlice('mesh-1', [{ address: '127.0.0.1' }], port) // chính process này
    k8s.setSlice('mesh-2', [{ address: '127.0.0.1' }], other.port)

    const { instance, warnings } = await discovery({ port, api: { server: k8s.url } })
    const { context, nodes, removed } = recordingContext()
    instance.bind(context)

    await until(() => warnings.some(message => message.includes('another-mesh')), 3000, 'namespace warning')
    await Bun.sleep(100)
    expect(nodes.size).toBe(0)
    expect(removed).toEqual([])
})

test('falls back to DNS with a warning when the service account may not watch EndpointSlices', async () => {
    const k8s = api()
    k8s.status = 403
    // Peer trên IPv6 loopback, discovery nghe IPv4: cùng cổng mà không đụng nhau.
    const a = peer(node('a'), '::1')
    const lookups: string[] = []

    const { instance, warnings } = await discovery({
        port: a.port,
        host: '127.0.0.1',
        api: { server: k8s.url },
        dnsIntervalMs: 20,
        resolve: async hostname => {
            lookups.push(hostname)
            return ['::1']
        },
    })
    const { context, nodes } = recordingContext()
    instance.bind(context)

    await until(() => nodes.has('a'), 3000, 'a through DNS')
    expect(instance.mode).toBe('dns')
    expect(nodes.get('a')?.host).toBe('::1')
    expect(lookups[0]).toBe('mesh.apps.svc.cluster.local')
    expect(warnings).toHaveLength(1)
    expect(warnings[0]).toContain('HTTP 403')
    expect(warnings[0]).toContain('list and watch on endpointslices.discovery.k8s.io')
    expect(warnings[0]).toContain('Falling back to DNS')
})

test('mode "api" warns and retries instead of falling back', async () => {
    const k8s = api()
    k8s.status = 403
    const { instance, warnings } = await discovery({ mode: 'api', api: { server: k8s.url }, resolve: async () => ['127.0.0.1'] })
    instance.bind(recordingContext().context)

    await until(() => warnings.length === 1, 3000, 'warning')
    expect(warnings[0]).toContain('does not fall back to DNS')
    expect(instance.mode).toBe('api')
})

test('without a service account, mode "auto" warns and uses DNS', async () => {
    const { instance, warnings } = await discovery({ resolve: async () => [], dnsIntervalMs: 20 })
    instance.bind(recordingContext().context)

    await until(() => instance.mode === 'dns', 3000, 'dns mode')
    expect(warnings[0]).toContain('No Kubernetes service account')
})

test('a DNS failure keeps the current members instead of dropping them', async () => {
    const a = peer(node('a'), '::1')
    let failing = false
    const { instance } = await discovery({
        mode: 'dns',
        port: a.port,
        host: '127.0.0.1',
        dnsIntervalMs: 20,
        resolve: async () => {
            if (failing) throw Object.assign(new Error('timeout'), { code: 'ETIMEOUT' })
            return ['::1']
        },
    })
    const { context, nodes, removed } = recordingContext()
    instance.bind(context)
    await until(() => nodes.has('a'), 3000, 'a')

    failing = true
    await Bun.sleep(100)
    expect(removed).toEqual([])
    expect(await instance.verify('a')).toBe('unknown')
})

test('serves the local node on /node and pushes every change', async () => {
    const { instance } = await discovery({ mode: 'dns', resolve: async () => [] })
    const { context, localNode$ } = recordingContext(node('me'))
    instance.bind(context)
    await until(() => true)
    await Bun.sleep(50)

    const health = await fetch(`http://127.0.0.1:${instance.port}/healthz`)
    expect(health.status).toBe(200)

    const response = await fetch(`http://127.0.0.1:${instance.port}/node`)
    expect(response.headers.get('content-type')).toBe('application/x-ndjson')
    const reader = response.body!.pipeThrough(new TextDecoderStream()).getReader()
    const lines: SpiderMeshNode[] = []
    const read = async () => {
        let buffer = ''
        while (true) {
            const { value, done } = await reader.read()
            if (done) return
            buffer += value
            let newline: number
            while ((newline = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, newline).trim()
                buffer = buffer.slice(newline + 1)
                if (line) lines.push(JSON.parse(line))
            }
        }
    }
    void read().catch(() => undefined)

    await until(() => lines.length === 1, 3000, 'first snapshot')
    localNode$.next({ ...node('me', 2), services: { Greeting: {} } })
    await until(() => lines.length === 2, 3000, 'second snapshot')
    expect(lines[1].services).toEqual({ Greeting: {} })
    await reader.cancel()
})
