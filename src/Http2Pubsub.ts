import { Subject } from "rxjs"
import { connect, createServer, type ClientHttp2Session, type Http2Server } from "node:http2"
import { AddressInfo } from "node:net"
import { unpack, pack } from 'msgpackr'
import type { PubsubEvent, PubsubTransporter, Registry, SpiderMeshNode } from '@spider-mesh/core'

export type PubsubMessage<T = any> = {
    namespace: string
    node_id: string
    sender_id: string
    topic: string
    data: T
}

export class Http2Pubsub extends Subject<PubsubEvent> implements PubsubTransporter {

    #nodes = new Map<string, ClientHttp2Session>()
    #subscriptions = new Map<string, Subject<any>>()
    #port = 0
    #server: Http2Server
    #registry?: Registry


    constructor(registry?: Registry) {
        super()
        this.#registry = registry
        this.#server = createServer()

        this.#server.on('request', (req, res) => {
            const event = req.url?.split('/')?.[2]
            if (!event) {
                res.statusCode = 404
                res.end()
                return
            }
            const buffers = [] as Buffer[]
            req.on('data', chunk => buffers.push(chunk as Buffer))
            req.on('end', () => {
                const stream = this.#subscriptions.get(event)
                if (stream) {
                    try {
                        stream.next(unpack(Buffer.concat(buffers)))
                    } catch {
                        stream.next(Buffer.concat(buffers))
                    }
                }
                res.statusCode = 204
                res.end()
            })
        })

        this.#server.listen(0, () => {
            const address = this.#server.address() as AddressInfo
            this.#port = address.port
            this.next({ endpoints: { port: this.#port } })
        })
    }

    listen<T>(topic: string) {
        const $ = this.#subscriptions.get(topic) || new Subject<any>()
        if (!this.#subscriptions.has(topic)) {
            this.#subscriptions.set(topic, $)
        }
        return $ as Subject<T>
    }

    async publish<T>(topic: string, data: T) {
        const buffer = pack(data)
        const targets = (this.#registry?.listTopicNodes(topic) || []).filter(node => {
            return !!this.#resolvePort(node)
        })

        for (const node of targets) {
            const connection = await this.#connect(node)
            if (!connection) continue
            if (!connection.destroyed && !connection.closed) {
                const req = connection.request({
                    ':path': `/events/${topic}`,
                    ':method': 'POST',
                    'content-type': 'application/json'
                })
                await new Promise<void>((resolve, reject) => {
                    let settled = false

                    req.on('response', headers => {
                        const status = Number(headers[':status'] || 0)
                        if (status >= 400 && !settled) {
                            settled = true
                            reject(new Error(`HTTP2 pubsub request failed with status ${status}`))
                        }
                    })
                    req.on('data', () => undefined)
                    req.once('end', () => {
                        if (!settled) {
                            settled = true
                            resolve()
                        }
                    })
                    req.once('error', error => {
                        if (!settled) {
                            settled = true
                            reject(error)
                        }
                    })

                    req.end(buffer)
                })
            }
        }
    }

    async #connect(node: SpiderMeshNode) {
        const current = this.#nodes.get(node.node_id)
        if (current && !current.destroyed && !current.closed) {
            return current
        }

        const port = this.#resolvePort(node)
        if (!port) return null
        const connection = connect(`http://${node.host}:${port}`)
        this.#nodes.set(node.node_id, connection)
        connection.once('close', () => {
            this.#nodes.delete(node.node_id)
        })
        connection.once('error', () => {
            this.#nodes.delete(node.node_id)
        })
        return connection
    }

    #resolvePort(node: SpiderMeshNode) {
        const metadata = node.transporters?.[this.constructor.name]
            || node.transporters?.Http2Pubsub
            || node.transporters?.http2pubsub
        return Number(metadata?.port || metadata || 0)
    }

    close() {
        for (const connection of this.#nodes.values()) {
            connection.close()
        }
        this.#nodes.clear()
        this.#server.close()
        super.unsubscribe()
    }
}
