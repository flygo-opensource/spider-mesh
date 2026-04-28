import { Buffer } from 'node:buffer'
import { decode, encode } from '@msgpack/msgpack'
import { Observable, Subject } from 'rxjs'
import WebSocket from 'ws'
import type { DiscoveryTransporter, MdnsMessage, NodeMetadata, PubsubTransporter, RpcEvent, RpcPacket, RpcTransporter, SpiderMeshNode } from '../types.js'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayHeader, type RpcRelayHeader } from './websocketProtocol.js'

export type WebsocketTransporterOptions = {
    heartbeatIntervalMs?: number
    reconnectIntervalMs?: number
}

export class WebsocketTransporter extends Subject<any> implements RpcTransporter, DiscoveryTransporter, PubsubTransporter {
    #socket: WebSocket | null = null
    #heartbeat: ReturnType<typeof setInterval> | null = null
    #reconnect: ReturnType<typeof setTimeout> | null = null
    #closedManually = false
    #topics = new Map<string, Subject<any>>()
    #knownNodes = new Map<string, SpiderMeshNode>()
    #localNode: SpiderMeshNode | null = null
    #latestDiscovery: MdnsMessage<NodeMetadata> | null = null
    #connectionMetadata: RpcEvent['metadata'] | null = null

    constructor(
        protected url: string,
        protected options: WebsocketTransporterOptions = {}
    ) {
        super()
        this.#connect()
    }

    get metadata() {
        return this.#connectionMetadata
    }

    close() {
        this.#closedManually = true
        this.#stopHeartbeat()

        if (this.#reconnect) {
            clearTimeout(this.#reconnect)
            this.#reconnect = null
        }

        if (this.#socket && this.#socket.readyState === WebSocket.OPEN) {
            this.#socket.close()
        } else if (this.#socket && this.#socket.readyState === WebSocket.CONNECTING) {
            this.#socket.terminate()
        }
    }


    async send(packet: RpcPacket, node: SpiderMeshNode) {
        await this.#sendFrame({
            type: packet.kind,
            node_id: node.node_id,
            sender_id: this.#localNode?.node_id,
        }, this.#encodeRpcPacket(packet))
    }

    async broadcast<T extends NodeMetadata>(data: MdnsMessage<T>, _ips: string[]) {
        this.#localNode = data.node as unknown as SpiderMeshNode
        this.#latestDiscovery = data
        this.#knownNodes.set(this.#localNode.node_id, this.#localNode)

        if (this.#socket?.readyState === WebSocket.OPEN) {
            await this.#sendFrame({ type: 'register', node_id: this.#localNode.node_id })
        }

        await this.#sendFrame({
            type: 'discovery',
            sender_id: data.sender_id,
        }, Buffer.from(encode(this.#localNode)), { allowDisconnected: true })
    }

    async publish<T>(topic: string, data: T) {
        await this.#sendFrame({
            type: 'pubsub',
            sender_id: this.#localNode?.node_id,
            topic,
        }, Buffer.from(encode(data)))
    }

    listen<T>(topic: string): Observable<T> {
        let stream = this.#topics.get(topic)
        if (!stream) {
            stream = new Subject<T>()
            this.#topics.set(topic, stream)
        }

        return stream.asObservable()
    }

    #connect() {
        const socket = new WebSocket(this.url)
        this.#socket = socket

        socket.on('open', () => {
            this.#connectionMetadata = {
                protocol: 'websocket',
                connected: true,
                heartbeatIntervalMs: this.options.heartbeatIntervalMs || 30000
            }

            this.next({ metadata: this.#connectionMetadata } satisfies RpcEvent)

            if (this.#localNode) {
                void this.#sendFrame({ type: 'register', node_id: this.#localNode.node_id }).catch(() => undefined)
            }

            if (this.#latestDiscovery) {
                void this.#sendFrame({
                    type: 'discovery',
                    sender_id: this.#latestDiscovery.sender_id,
                }, Buffer.from(encode(this.#latestDiscovery.node))).catch(() => undefined)
            }

            this.#startHeartbeat()
        })

        socket.on('message', raw => {
            const frame = this.#parseFrame(raw)
            if (!frame) return

            if (this.#isRpcHeader(frame.header)) {
                const packet = this.#decodeRpcPacket(frame.header, frame.payload)
                if (!packet) return
                const sender = this.#resolveNode(frame.header.sender_id)
                this.next({
                    message: {
                        node: sender,
                        packet
                    }
                } satisfies RpcEvent)
                return
            }

            if (frame.header.type === 'discovery') {
                try {
                    const node = decode(frame.payload) as SpiderMeshNode
                    this.#knownNodes.set(node.node_id, node)
                    this.next(node)
                } catch {
                    return
                }
                return
            }

            if (frame.header.type === 'pubsub') {
                const topic = this.#topics.get(frame.header.topic)
                if (topic) {
                    try {
                        topic.next(decode(frame.payload) as any)
                    } catch {
                        topic.next(frame.payload)
                    }
                }
                return
            }

            if (frame.header.type === 'offline') {
                this.#knownNodes.delete(frame.header.sender_id)
                this.next({ offline: frame.header.sender_id } satisfies RpcEvent)
            }
        })

        socket.on('close', () => {
            this.#connectionMetadata = {
                protocol: 'websocket',
                connected: false,
                heartbeatIntervalMs: this.options.heartbeatIntervalMs || 30000
            }
            this.#stopHeartbeat()
            if (this.#closedManually) return
            this.#scheduleReconnect()
        })

        socket.on('error', () => undefined)
    }

    #startHeartbeat() {
        this.#stopHeartbeat()
        this.#heartbeat = setInterval(() => {
            if (this.#socket?.readyState === WebSocket.OPEN) {
                this.#socket.ping()
            }
        }, this.options.heartbeatIntervalMs || 30000)
    }

    #stopHeartbeat() {
        if (this.#heartbeat) {
            clearInterval(this.#heartbeat)
            this.#heartbeat = null
        }
    }

    #scheduleReconnect() {
        if (this.#closedManually) return
        if (this.#reconnect) return
        this.#reconnect = setTimeout(() => {
            this.#reconnect = null
            this.#connect()
        }, this.options.reconnectIntervalMs || 1000)
    }

    #resolveNode(nodeId?: string) {
        if (nodeId && this.#knownNodes.has(nodeId)) {
            return this.#knownNodes.get(nodeId)!
        }

        return {
            ips: [],
            host: '',
            namespace: '',
            node_id: nodeId || '',
            services: {},
            topics: [],
            transporters: {},
            nodes: {},
            version: 0,
        } satisfies SpiderMeshNode
    }

    #isRpcHeader(header: RelayHeader): header is RpcRelayHeader {
        return header.type === 'request' || header.type === 'response' || header.type === 'cancel'
    }

    #encodeRpcPacket(packet: RpcPacket) {
        const { kind: _, ...payload } = packet
        return Buffer.from(encode(payload))
    }

    #decodeRpcPacket(header: RpcRelayHeader, payload: Buffer) {
        try {
            return {
                kind: header.type,
                ...(decode(payload) as Record<string, unknown>),
            } as RpcPacket
        } catch {
            return null
        }
    }

    async #sendFrame(header: RelayHeader, payload: Buffer = Buffer.alloc(0), options: { allowDisconnected?: boolean } = {}) {
        if (!this.#socket || this.#socket.readyState !== WebSocket.OPEN) {
            if (options.allowDisconnected) return
            throw new Error('WebSocket is not connected')
        }

        this.#socket.send(encodeRelayFrame(header, payload), { binary: true })
    }

    #parseFrame(raw: WebSocket.RawData) {
        return decodeRelayFrame(normalizeRelayRawData(raw))
    }
}