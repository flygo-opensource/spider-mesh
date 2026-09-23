import { createServer } from 'node:net'
import type { SpiderMeshNode } from '@spider-mesh/core'

const encoder = new TextEncoder()

type Endpoint = { address: string; ready?: boolean }

/**
 * API server Kubernetes giả, đủ cho `list` và `watch` EndpointSlice: đổi slice thì phát event tới mọi
 * watch đang mở; `expire()` phát lỗi 410; `status` khác 200 thì trả lỗi đó cho mọi request.
 */
export class FakeKubernetesApi {
    status = 200
    lists = 0
    watches = 0
    authorizations: (string | null)[] = []
    #slices = new Map<string, object>()
    #resourceVersion = 1
    #watchers = new Set<ReadableStreamDefaultController<Uint8Array>>()
    #server = Bun.serve({ port: 0, hostname: '127.0.0.1', fetch: request => this.#handle(request) })

    get url() {
        return `http://127.0.0.1:${this.#server.port}`
    }

    setSlice(name: string, endpoints: Endpoint[], port?: number) {
        const type = this.#slices.has(name) ? 'MODIFIED' : 'ADDED'
        const slice = {
            metadata: { name, resourceVersion: String(++this.#resourceVersion) },
            ports: port ? [{ name: 'discovery', port }] : [],
            endpoints: endpoints.map(endpoint => ({
                addresses: [endpoint.address],
                conditions: endpoint.ready === undefined ? {} : { ready: endpoint.ready },
            })),
        }
        this.#slices.set(name, slice)
        this.#send({ type, object: slice })
    }

    deleteSlice(name: string) {
        const slice = this.#slices.get(name)
        if (!slice) return
        this.#slices.delete(name)
        this.#send({ type: 'DELETED', object: { ...slice, metadata: { name, resourceVersion: String(++this.#resourceVersion) } } })
    }

    expire() {
        this.#send({ type: 'ERROR', object: { kind: 'Status', code: 410, message: 'too old resource version' } })
    }

    close() {
        for (const controller of this.#watchers) {
            try { controller.close() } catch { }
        }
        this.#server.stop(true)
    }

    #send(event: object) {
        const line = encoder.encode(`${JSON.stringify(event)}\n`)
        for (const controller of this.#watchers) {
            try { controller.enqueue(line) } catch { this.#watchers.delete(controller) }
        }
    }

    #handle(request: Request) {
        const url = new URL(request.url)
        if (!url.pathname.endsWith('/endpointslices')) return new Response('not found', { status: 404 })
        this.authorizations.push(request.headers.get('authorization'))
        if (this.status !== 200) return new Response('{"kind":"Status","reason":"Forbidden"}', { status: this.status })

        if (url.searchParams.get('watch') === 'true') {
            this.watches++
            let own: ReadableStreamDefaultController<Uint8Array>
            const body = new ReadableStream<Uint8Array>({
                start: controller => {
                    own = controller
                    this.#watchers.add(controller)
                },
                cancel: () => {
                    this.#watchers.delete(own)
                },
            })
            return new Response(body, { headers: { 'content-type': 'application/json' } })
        }

        this.lists++
        return Response.json({
            kind: 'EndpointSliceList',
            metadata: { resourceVersion: String(this.#resourceVersion) },
            items: [...this.#slices.values()],
        })
    }
}

/** Pod giả: phục vụ `GET /node` như `KubernetesDiscovery`, đẩy snapshot mới bằng `push()`. */
export class FakePeer {
    connections = 0
    #node: SpiderMeshNode
    #streams = new Set<ReadableStreamDefaultController<Uint8Array>>()
    #server: ReturnType<typeof Bun.serve>

    constructor(node: SpiderMeshNode, hostname = '127.0.0.1') {
        this.#node = node
        this.#server = Bun.serve({
            port: 0,
            hostname,
            fetch: request => {
                if (new URL(request.url).pathname !== '/node') return new Response('not found', { status: 404 })
                this.connections++
                let own: ReadableStreamDefaultController<Uint8Array>
                const body = new ReadableStream<Uint8Array>({
                    start: controller => {
                        own = controller
                        this.#streams.add(controller)
                        controller.enqueue(encoder.encode(`${JSON.stringify(this.#node)}\n`))
                    },
                    cancel: () => {
                        this.#streams.delete(own)
                    },
                })
                return new Response(body, { headers: { 'content-type': 'application/x-ndjson' } })
            },
        })
    }

    get port() {
        return this.#server.port!
    }

    push(node: SpiderMeshNode) {
        this.#node = node
        for (const controller of this.#streams) {
            try { controller.enqueue(encoder.encode(`${JSON.stringify(node)}\n`)) } catch { }
        }
    }

    /** Cắt mọi stream đang mở nhưng vẫn nhận kết nối mới. */
    dropStreams() {
        for (const controller of this.#streams) {
            try { controller.close() } catch { }
        }
        this.#streams.clear()
    }

    close() {
        this.dropStreams()
        this.#server.stop(true)
    }
}

export const node = (node_id: string, version = 1, namespace = 'default'): SpiderMeshNode => ({
    node_id,
    namespace,
    host: '',
    version,
    topics: [],
    services: {},
    nodes: {},
    transporters: {},
})

export async function freePort() {
    return await new Promise<number>((resolve, reject) => {
        const server = createServer()
        server.once('error', reject)
        server.listen(0, '127.0.0.1', () => {
            const { port } = server.address() as { port: number }
            server.close(() => resolve(port))
        })
    })
}

/** Chờ tới khi `check()` đúng, hoặc ném lỗi sau `timeoutMs`. */
export async function until(check: () => boolean, timeoutMs = 3000, label = 'condition') {
    const deadline = Date.now() + timeoutMs
    while (!check()) {
        if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}`)
        await Bun.sleep(10)
    }
}
