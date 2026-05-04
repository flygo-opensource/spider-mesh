import { firstValueFrom, fromEvent, reduce, Subject, takeUntil } from "rxjs"
import { connect, createServer, type ClientHttp2Session, type ClientHttp2Stream, type IncomingHttpHeaders, type ServerHttp2Stream } from 'node:http2'
import { SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE } from "./const.js"
import { AddressInfo } from "node:net"
import { unpack, pack } from 'msgpackr'
import type { RpcEvent, RpcPacket, RpcTransporter, SpiderMeshError, SpiderMeshNode } from "./types.js"
import { transportRuntime } from "./runtime.js"


export class Http2Rpc extends Subject<RpcEvent> implements RpcTransporter {

    #connections = new Map<string, ClientHttp2Session>()
    #responseStreams = new Map<string, ServerHttp2Stream>()
    #metadata: RpcEvent['endpoints'] | null = null
    #server = createServer({})
    #isDisposing = false

    constructor() {
        super()

        fromEvent(this.#server, 'stream').subscribe(args => {
            const [stream] = args as [ServerHttp2Stream]
            void this.#handleStream(stream)
        })

        this.#server.listen({
            port: 0,
            isIPv4: true,
            isIPv6: true
        }, () => {
            const { port } = this.#server.address() as AddressInfo
            this.#metadata = { port }
            transportRuntime.setTransporterMetadata(this.constructor.name, this.#metadata)
            this.next({ endpoints: this.#metadata })
        })
    }

    get metadata() {
        return this.#metadata
    }

    override unsubscribe() {
        this.#isDisposing = true
        for (const stream of this.#responseStreams.values()) {
            stream.close()
        }
        this.#responseStreams.clear()
        for (const connection of this.#connections.values()) {
            connection.destroy()
        }
        this.#connections.clear()
        this.#server.close()
        super.unsubscribe()
    }

    async send(packet: RpcPacket, node: SpiderMeshNode) {
        if (packet.kind === 'response') {
            const stream = this.#responseStreams.get(packet.request_id)
            if (stream) {
                await this.#writeResponsePacket(stream, packet)
                if (packet.completed || packet.error != undefined) {
                    this.#responseStreams.delete(packet.request_id)
                }
                return
            }
        }

        const connection = await this.#connect(node)
        const request = connection.request({
            ':method': 'POST',
            ':path': '/rpc',
            'content-type': 'application/octet-stream',
        })

        if (packet.kind === 'request') {
            this.#bindResponseStream(packet, node, request)
        }

        await new Promise<void>((resolve, reject) => {
            let settled = false

            request.on('response', (headers: IncomingHttpHeaders) => {
                const status = Number(headers[':status'] || 0)
                if (status >= 400 && !settled) {
                    settled = true
                    reject(new Error(`HTTP2 RPC request failed with status ${status}`))
                    return
                }

                if (!settled) {
                    settled = true
                    resolve()
                }
            })
            request.on('data', () => undefined)
            request.once('end', () => {
                if (!settled && packet.kind !== 'request') {
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

            request.end(pack(packet))
        })
    }

    async #connect(node: SpiderMeshNode) {
        const current_connection = this.#connections.get(node.node_id)
        if (current_connection) {
            if (!current_connection.destroyed && !current_connection.closed) {
                return current_connection
            }
        }

        const auto = SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE
        const metadata = this.#getTransporterMetadata(node)
        const port = Number(metadata?.port)

        if (!port) {
            throw {
                code: 'MICROSERVICE_OFFLINE',
                message: `Http2Rpc metadata missing for node ${node.node_id}`
            } satisfies SpiderMeshError
        }

        const urls = [
            `http://${node.host.includes(':') ? `[${node.host}]` : node.host}:${port}`,
        ]


        for (const url of urls) {
            const connection = connect(url)
            const connected = connection.connecting ? await new Promise<boolean>(resolve => {
                connection.once('connect', () => resolve(true))
                connection.once('error', () => resolve(false))
            }) : true
            if (connected) {
                connection.once('close', () => {
                    this.#connections.delete(node.node_id)
                    if (!this.#isDisposing && !this.closed) {
                        this.next({ offline: node.node_id })
                    }
                })
                this.#connections.set(node.node_id, connection)
                return connection
            }
        }
        const e: SpiderMeshError = {
            code: 'MICROSERVICE_OFFLINE',
            message: `All connection attempts to node ${node.node_id} failed` 
        }
        throw e
    }

    async #handleStream(stream: ServerHttp2Stream) {
        try {
            const end$ = fromEvent(stream, 'end')
            const buffers = await firstValueFrom(
                fromEvent(stream, 'data').pipe(
                    takeUntil(end$),
                    reduce((chunks, chunk) => {
                        chunks.push(chunk as Buffer)
                        return chunks
                    }, [] as Buffer[])
                )
            )

            const packet = unpack(Buffer.concat(buffers)) as RpcPacket

            if (packet.kind === 'request') {
                stream.respond({
                    ':status': 200,
                    'content-type': 'application/octet-stream'
                })
                this.#responseStreams.set(packet.request_id, stream)
                stream.once('close', () => {
                    this.#responseStreams.delete(packet.request_id)
                })
            } else {
                stream.respond({ ':status': 204 })
                stream.end()
            }

            this.next({
                rpc: {
                    node_id: packet.source_node_id,
                    packet
                }
            })
        } catch {
            if (!stream.headersSent) {
                stream.respond({ ':status': 400 })
            }
            stream.end()
        }
    }

    async #writeResponsePacket(stream: ServerHttp2Stream, packet: RpcPacket) {
        const frame = this.#encodeFrame(packet)
        const isTerminal = packet.kind === 'response' && (packet.completed || packet.error != undefined)

        await new Promise<void>((resolve, reject) => {
            const done = (error?: Error | null) => {
                if (error) {
                    reject(error)
                    return
                }
                resolve()
            }

            if (isTerminal) {
                stream.end(frame, done)
                return
            }

            stream.write(frame, done)
        })
    }

    #bindResponseStream(packet: Extract<RpcPacket, { kind: 'request' }>, node: SpiderMeshNode, request: ClientHttp2Stream) {
        let buffer = Buffer.alloc(0)
        let completed = false
        let accepted = false

        const emitResponse = (response: RpcPacket) => {
            if (this.#isDisposing || this.closed) {
                return
            }

            this.next({
                rpc: {
                    node_id: node.node_id,
                    packet: response
                }
            })
        }

        request.on('response', (headers: IncomingHttpHeaders) => {
            const status = Number(headers[':status'] || 0)
            accepted = status > 0 && status < 400
        })

        request.on('data', chunk => {
            buffer = Buffer.concat([buffer, chunk as Buffer])

            while (buffer.length >= 4) {
                const size = buffer.readUInt32BE(0)
                if (buffer.length < size + 4) {
                    return
                }

                const frame = buffer.subarray(4, size + 4)
                buffer = buffer.subarray(size + 4)

                const response = unpack(frame) as RpcPacket
                if (response.kind !== 'response') {
                    continue
                }

                if (response.completed || response.error != undefined) {
                    completed = true
                }

                emitResponse(response)
            }
        })

        request.once('end', () => {
            if (!accepted || completed) {
                return
            }

            emitResponse({
                kind: 'response',
                request_id: packet.request_id,
                source_node_id: node.node_id,
                target_node_id: packet.source_node_id,
                error: {
                    code: 'MICROSERVICE_OFFLINE',
                    message: 'RPC response stream ended unexpectedly'
                },
                completed: true
            })
        })

        request.once('error', error => {
            if (!accepted || completed) {
                return
            }

            completed = true
            emitResponse({
                kind: 'response',
                request_id: packet.request_id,
                source_node_id: node.node_id,
                target_node_id: packet.source_node_id,
                error: {
                    code: 'MICROSERVICE_OFFLINE',
                    message: error.message || 'RPC response stream failed'
                },
                completed: true
            })
        })
    }

    #encodeFrame(packet: RpcPacket) {
        const payload = pack(packet)
        const frame = Buffer.allocUnsafe(payload.length + 4)
        frame.writeUInt32BE(payload.length, 0)
        payload.copy(frame, 4)
        return frame
    }

    #getTransporterMetadata(node?: SpiderMeshNode | null) {
        if (!node) return undefined
        return node.transporters?.[this.constructor.name]
            || node.transporters?.Http2Rpc
            || node.transporters?.http2rpc
    }
} 