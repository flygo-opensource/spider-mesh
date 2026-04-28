import { Subject } from "rxjs"
import { connect, createServer, type ClientHttp2Session, type IncomingHttpHeaders, type ServerHttp2Stream } from 'node:http2'
import { SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE } from "./const.js"
import { AddressInfo } from "node:net"
import { unpack, pack } from 'msgpackr'
import type { RpcEvent, RpcPacket, RpcTransporter, SpiderMeshError, SpiderMeshNode } from "./types.js"
import { transportRuntime } from "./runtime.js"


export type RequestHeaders = {
    ':path': string
    ':authority': string,
    smnode?: string
    smport?: string
}


export class Http2Rpc extends Subject<RpcEvent> implements RpcTransporter {

    #connections = new Map<string, ClientHttp2Session>()
    #metadata: RpcEvent['metadata'] | null = null
    #server = createServer({})

    constructor() {
        super()

        this.#server.on('stream', (stream: ServerHttp2Stream, headers: RequestHeaders) => {
            this.#handleStream(stream, headers)
        })

        this.#server.listen({
            port: 0,
            isIPv4: true,
            isIPv6: true
        }, () => {
            const { port } = this.#server.address() as AddressInfo
            this.#metadata = { port }
            transportRuntime.setTransporterMetadata(this.constructor.name, this.#metadata)
            this.next({ metadata: this.#metadata })
        })
    }

    get metadata() {
        return this.#metadata
    }

    override unsubscribe() {
        for (const connection of this.#connections.values()) {
            connection.close()
        }
        this.#connections.clear()
        this.#server.close()
        super.unsubscribe()
    }

    async send(packet: RpcPacket, node: SpiderMeshNode) {
        const connection = await this.#connect(node)
        const request = connection.request({
            ':method': 'POST',
            ':path': '/rpc',
            'content-type': 'application/octet-stream',
            ...(this.#senderHeaders(packet.source_node_id))
        })

        await new Promise<void>((resolve, reject) => {
            let settled = false

            request.on('response', (headers: IncomingHttpHeaders) => {
                const status = Number(headers[':status'] || 0)
                if (status >= 400 && !settled) {
                    settled = true
                    reject(new Error(`HTTP2 RPC request failed with status ${status}`))
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

        const urls = auto ? [
            `http://${node.host}:${port}`,
        ] : node.ips.sort((a: string, b: string) => a.length - b.length).map((ip: string) => `http://${ip.includes(':') ? `[${ip}]` : ip}:${port}`)


        for (const url of urls) {
            const connection = connect(url)
            const connected = connection.connecting ? await new Promise<boolean>(resolve => {
                connection.once('connect', () => resolve(true))
                connection.once('error', () => resolve(false))
            }) : true
            if (connected) {
                connection.once('close', () => {
                    this.#connections.delete(node.node_id)
                    this.next({ offline: node.node_id })
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

    #handleStream(stream: ServerHttp2Stream, headers: RequestHeaders) {
        const buffers = [] as Buffer[]
        stream.on('data', (chunk: Buffer) => buffers.push(chunk))
        stream.once('end', () => {
            try {
                const packet = unpack(Buffer.concat(buffers)) as RpcPacket
                const node = this.#resolveSenderNode(packet, headers, stream)
                transportRuntime.updateNode(node)
                stream.respond({ ':status': 204 })
                stream.end()
                this.next({
                    message: {
                        node,
                        packet
                    }
                })
            } catch {
                if (!stream.headersSent) {
                    stream.respond({ ':status': 400 })
                }
                stream.end()
            }
        })
    }

    #resolveSenderNode(packet: RpcPacket, headers: RequestHeaders, stream: ServerHttp2Stream) {
        const encodedNode = headers.smnode
        if (encodedNode) {
            try {
                return transportRuntime.updateNode(unpack(Buffer.from(encodedNode, 'base64url')) as SpiderMeshNode)
            } catch {
                return this.#fallbackSenderNode(packet, headers, stream)
            }
        }

        return this.#fallbackSenderNode(packet, headers, stream)
    }

    #fallbackSenderNode(packet: RpcPacket, headers: RequestHeaders, stream: ServerHttp2Stream) {
        const known = transportRuntime.getNode(packet.source_node_id)
        const remoteAddress = this.#normalizeHost(stream.session?.socket.remoteAddress)
        const port = Number(headers.smport || this.#getTransporterMetadata(known)?.port || 0)

        return {
            ips: known?.ips?.length ? known.ips : remoteAddress ? [remoteAddress] : [],
            host: known?.host || remoteAddress || '',
            namespace: known?.namespace || transportRuntime.localNode?.namespace || '',
            version: known?.version || 0,
            node_id: packet.source_node_id,
            online: known?.online,
            topics: known?.topics || [],
            services: known?.services || {},
            nodes: known?.nodes || {},
            transporters: {
                ...(known?.transporters || {}),
                [this.constructor.name]: {
                    ...(this.#getTransporterMetadata(known) || {}),
                    ...(port ? { port } : {})
                }
            }
        } satisfies SpiderMeshNode
    }

    #senderHeaders(sourceNodeId: string) {
        const localNode = transportRuntime.localNode
        const port = this.#metadata?.port
        const senderNode = localNode?.node_id === sourceNodeId
            ? transportRuntime.withTransporters(localNode)
            : localNode

        return {
            ...(senderNode ? { smnode: Buffer.from(pack(senderNode)).toString('base64url') } : {}),
            ...(port ? { smport: String(port) } : {})
        }
    }

    #getTransporterMetadata(node?: SpiderMeshNode | null) {
        if (!node) return undefined
        return node.transporters?.[this.constructor.name]
            || node.transporters?.Http2Rpc
            || node.transporters?.http2rpc
    }

    #normalizeHost(host?: string | null) {
        if (!host) return ''
        return host.replace(/^::ffff:/, '')
    }
} 