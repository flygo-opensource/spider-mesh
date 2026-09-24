import { connect, createServer, type ClientHttp2Session, type Http2Server } from 'node:http2'
import type { AddressInfo } from 'node:net'
import { pack, unpack } from 'msgpackr'
import { ReplaySubject, Subject } from 'rxjs'
import type { SpiderMeshNode, Topology } from '@spider-mesh/core'

/** Metadata endpoint được Http2Pubsub quảng bá qua EventBus. */
type PubsubMetadata = Record<string, string | boolean | number>

/**
 * Event transporter tạm thời, không broker, truyền payload qua HTTP/2.
 * Instance này đăng ký với `EventBus`, không đăng ký với `SpiderMesh`.
 */
export class Http2Pubsub {
    public readonly name = 'http2-pubsub'
    readonly #nodes = new Map<string, ClientHttp2Session>()
    readonly #subscriptions = new Map<string, Subject<unknown>>()
    readonly #server: Http2Server
    readonly #metadata = new ReplaySubject<PubsubMetadata>(1)
    #port = 0

    constructor(private readonly topology: Topology) {
        this.#server = createServer()
        this.#server.on('request', (request, response) => {
            const encodedTopic = request.url?.split('/')?.[2]
            if (!encodedTopic) {
                response.statusCode = 404
                response.end()
                return
            }

            const buffers: Buffer[] = []
            request.on('data', chunk => buffers.push(chunk as Buffer))
            request.on('end', () => {
                const stream = this.#subscriptions.get(decodeURIComponent(encodedTopic))
                if (stream) {
                    try {
                        stream.next(unpack(Buffer.concat(buffers)))
                    } catch {
                        stream.next(Buffer.concat(buffers))
                    }
                }
                response.statusCode = 204
                response.end()
            })
        })

        this.#server.listen(0, () => {
            this.#port = (this.#server.address() as AddressInfo).port
            this.#metadata.next({ port: this.#port })
        })
    }

    get metadata() {
        return this.#port ? { port: this.#port } : null
    }

    get metadata$() {
        return this.#metadata.asObservable()
    }

    listen<T>(topic: string) {
        const existing = this.#subscriptions.get(topic)
        if (existing) return existing as Subject<T>
        const created = new Subject<unknown>()
        this.#subscriptions.set(topic, created)
        return created as Subject<T>
    }

    async publish<T>(topic: string, data: T) {
        const payload = pack(data)
        const targets = this.topology.listTopicNodes(topic).filter(node => this.#resolvePort(node) > 0)

        await Promise.all(targets.map(async node => {
            const connection = await this.#connect(node)
            const request = connection.request({
                ':path': `/events/${encodeURIComponent(topic)}`,
                ':method': 'POST',
                'content-type': 'application/octet-stream',
            })

            await new Promise<void>((resolve, reject) => {
                let settled = false
                request.on('response', headers => {
                    const status = Number(headers[':status'] || 0)
                    if (status >= 400 && !settled) {
                        settled = true
                        reject(new Error(`HTTP/2 event delivery failed with status ${status}`))
                    }
                })
                request.on('data', () => undefined)
                request.once('end', () => {
                    if (!settled) {
                        settled = true
                        resolve()
                    }
                })
                request.once('error', error => {
                    if (!settled) {
                        settled = true
                        reject(error)
                    }
                })
                request.end(payload)
            })
        }))
    }

    async #connect(node: SpiderMeshNode) {
        const existing = this.#nodes.get(node.node_id)
        if (existing && !existing.destroyed && !existing.closed) return existing

        const port = this.#resolvePort(node)
        if (!port) throw new Error(`Event endpoint metadata missing for node ${node.node_id}`)
        if (!node.host) throw new Error(`Node ${node.node_id} has no host: set SPIDERMESH_NODE_HOSTNAME on that node`)
        const host = node.host.includes(':') ? `[${node.host}]` : node.host
        const connection = connect(`http://${host}:${port}`)
        this.#nodes.set(node.node_id, connection)
        const remove = () => {
            if (this.#nodes.get(node.node_id) === connection) this.#nodes.delete(node.node_id)
        }
        connection.once('close', remove)
        connection.once('error', remove)
        return connection
    }

    #resolvePort(node: SpiderMeshNode) {
        const metadata = node.transporters?.[this.name]
        if (typeof metadata === 'number') return metadata
        if (metadata && typeof metadata === 'object') {
            return Number((metadata as { port?: number }).port || 0)
        }
        return 0
    }

    close() {
        for (const connection of this.#nodes.values()) connection.close()
        this.#nodes.clear()
        for (const subscription of this.#subscriptions.values()) subscription.complete()
        this.#subscriptions.clear()
        this.#metadata.complete()
        this.#server.close()
    }
}
