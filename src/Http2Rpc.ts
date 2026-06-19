import { firstValueFrom, fromEvent, reduce, Subject, takeUntil } from "rxjs"
import { connect, createServer, type ClientHttp2Session, type ClientHttp2Stream, type IncomingHttpHeaders, type ServerHttp2Stream } from 'node:http2'
import { SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE } from "./const.js"
import { AddressInfo } from "node:net"
import { unpack, pack } from 'msgpackr'
import type { Registry, RpcCancelPacket, RpcEvent, RpcRequestPacket, RpcResponsePacket, RpcTransporter, SpiderMeshError, SpiderMeshNode } from '@spider-mesh/core'


export class Http2Rpc extends Subject<RpcEvent> implements RpcTransporter {

    #connections = new Map<string, ClientHttp2Session>()
    #responseStreams = new Map<string, ServerHttp2Stream>()
    #metadata: RpcEvent['endpoints'] | null = null
    #server = createServer({})
    #isDisposing = false
    #registry?: Registry

    constructor(registry?: Registry) {
        super()
        this.#registry = registry

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

    async send(packet: RpcRequestPacket | RpcCancelPacket | RpcResponsePacket): Promise<{ cancel: () => void }> {
        if (packet.kind === 'response') {
            const stream = this.#responseStreams.get(packet.request_id)
            if (stream) {
                await this.#writeResponsePacket(stream, packet)
                if (packet.completed || packet.error != undefined) {
                    this.#responseStreams.delete(packet.request_id)
                }
                return { cancel: () => {} }
            }
            return { cancel: () => {} }
        }

        if (packet.kind === 'request' && !this.#registry) {
            throw {
                code: 'MICROSERVICE_OFFLINE',
                message: 'Http2Rpc requires a registry to route RPC requests'
            } satisfies SpiderMeshError
        }

        // destination_node_id is embedded in the packet; fall back to registry round-robin for
        // requests. Skip peers that have advertised the service but not yet their Http2Rpc port,
        // so a half-formed provider mid-startup is never picked (it would throw "metadata missing").
        const resolvedNodeId = packet.destination_node_id
            ?? (packet.kind === 'request'
                ? (this.#registry?.pickRpcNode(packet.service, { filter: node => this.#hasRpcEndpoint(node) }) ?? undefined)
                : undefined)

        const node = this.#resolveNode(resolvedNodeId)

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

        if (packet.kind === 'request') {
            return {
                cancel: () => {
                    void this.send({
                        kind: 'cancel',
                        request_id: packet.request_id,
                        destination_node_id: resolvedNodeId,
                    }).catch(() => undefined)
                }
            }
        }

        return { cancel: () => {} }
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
                    this.#registry?.removePeer(node.node_id)
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
            message: `All connection attempts to node ${node.node_id} failed` ,
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

            const packet = unpack(Buffer.concat(buffers)) as RpcRequestPacket | RpcResponsePacket | RpcCancelPacket

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

            this.next({ rpc: packet })
        } catch {
            if (!stream.headersSent) {
                stream.respond({ ':status': 400 })
            }
            stream.end()
        }
    }

    async #writeResponsePacket(stream: ServerHttp2Stream, packet: RpcResponsePacket) {
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

    #bindResponseStream(packet: RpcRequestPacket, node: SpiderMeshNode, request: ClientHttp2Stream) {
        let buffer = Buffer.alloc(0)
        let completed = false
        let accepted = false

        const emitResponse = (response: RpcResponsePacket) => {
            if (this.#isDisposing || this.closed) {
                return
            }

            this.next({ rpc: response })
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

                const response = unpack(frame) as RpcResponsePacket
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
                error: {
                    code: 'MICROSERVICE_OFFLINE',
                    message: error.message || 'RPC response stream failed'
                },
                completed: true
            })
        })
    }

    #encodeFrame(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
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

    // A peer is RPC-routable only once it has advertised an Http2Rpc endpoint port.
    // NOTE: this keys on the 'Http2Rpc' transporter name (matching `#getTransporterMetadata`);
    // registering Http2Rpc under a custom name would defeat this lookup — see UdpDiscovery#isRpcReady.
    #hasRpcEndpoint(node: SpiderMeshNode): boolean {
        return Number((this.#getTransporterMetadata(node) as { port?: number } | undefined)?.port) > 0
    }

    // Reachability probe used by core's #selectRpcTransport. Side-effect free: mirrors the
    // condition #resolveNode/#connect need (a peer serving `service` that has advertised its
    // Http2Rpc endpoint port) WITHOUT advancing the registry's round-robin index.
    canRoute(service: string, node_id?: string): boolean {
        const registry = this.#registry
        if (!registry) return false
        if (node_id) {
            const node = registry.getPeer(node_id)
            return !!node && node.services?.[service] != undefined && this.#hasRpcEndpoint(node)
        }
        return registry.listPeers(service).some(node => this.#hasRpcEndpoint(node))
    }

    #resolveNode(node_id?: string) {
        const node = node_id ? this.#registry?.getPeer(node_id) : undefined
        if (node) {
            return node
        }

        throw {
            code: 'MICROSERVICE_OFFLINE',
            message: node_id
                ? `Node ${node_id} is not available`
                : 'RPC target node is not available'
        } satisfies SpiderMeshError
    }
} 