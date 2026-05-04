import { decode, encode } from '@msgpack/msgpack'
import { defer, finalize, fromEvent, ignoreElements, map, merge, Observable, of, ReplaySubject, retry, share, Subject, Subscription, switchMap, take, tap, throwError, timer } from 'rxjs'
import WebSocket from 'ws'
import type { DiscoveryTransporter, MdnsMessage, NodeMetadata, PubsubTransporter, RpcEvent, RpcPacket, RpcTransporter, SpiderMeshNode } from '@spider-mesh/core'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame, type ReceivedRelayFrame, type ReceivedRelayRpcFrame } from './websocketProtocol.js'

export type WebsocketTransporterOptions = {
    heartbeatIntervalMs?: number
    reconnectIntervalMs?: number
    unsubscribeDelayMs?: number
}

type TopicStream<T = any> = {
    subject: Subject<T>
    stream: Observable<T>
}

type KnownNode = {
    node: SpiderMeshNode
    relayUrl: string
}

type RelayConnection = {
    subscription: Subscription
    socket?: WebSocket
}

export class WebsocketTransporter extends Subject<any> implements RpcTransporter, DiscoveryTransporter, PubsubTransporter {
    #connections = new Map<string, RelayConnection>()
    #topics = new Map<string, TopicStream>()
    #nodes = new Map<string, KnownNode>()
    #me$ = new ReplaySubject<SpiderMeshNode>(1)

    constructor(protected options: WebsocketTransporterOptions = {}) {
        super()
    }

    get metadata() {
        return {}
    }

    connect(url: string | string[]) {
        for (const relayUrl of new Set(Array.isArray(url) ? url : [url])) {
            if (this.#connections.has(relayUrl)) continue
            this.#connections.set(relayUrl, {
                subscription: this.#createConnectionLoop(relayUrl).subscribe(),
            })
        }
    }

    close(url?: string | string[]) {
        const urls = url ? [...new Set(Array.isArray(url) ? url : [url])] : [...this.#connections.keys()]

        for (const relayUrl of urls) {
            this.#connections.get(relayUrl)?.subscription.unsubscribe()
            this.#connections.delete(relayUrl)
        }
    }


    async send(packet: RpcPacket, node: SpiderMeshNode) {
        const socket = this.#selectRpcSocket(node)
        await this.#sendFrame(socket, {
            type: packet.kind,
            target_id: node.node_id,
            payload: this.#encodeRpcPacket(packet),
        })
    }

    async broadcast(data: MdnsMessage<NodeMetadata>, _ips: string[]) {
        const localNode = {
            ...(data.node as unknown as SpiderMeshNode),
            node_id: data.sender_id,
        } satisfies SpiderMeshNode
        this.#me$.next(localNode)
    }

    async publish<T>(topic: string, data: T) {
        await this.#sendFrameToAll({
            type: 'publish',
            topic,
            payload: encode(data),
        })
    }

    listen<T>(topic: string): Observable<T> {
        let entry = this.#topics.get(topic) as TopicStream<T> | undefined
        if (!entry) {
            const subject = new Subject<T>()
            const stream = defer(() => {
                void this.#announceSubscriptions().catch(() => undefined)

                return subject.asObservable().pipe(
                    finalize(() => {
                        if (this.#topics.get(topic)?.subject !== subject) return

                        this.#topics.delete(topic)
                        void this.#announceUnsubscribe([topic]).catch(() => undefined)
                    })
                )
            }).pipe(share({
                resetOnRefCountZero: () => timer(this.options.unsubscribeDelayMs || 10000),
            }))

            entry = { subject, stream }
            this.#topics.set(topic, entry)
        }

        return entry.stream
    }

    #createConnectionLoop(url: string) {
        return of(1).pipe(
            map(() => new WebSocket(url)),
            switchMap(socket => {
                const disconnect = () => {
                    const connection = this.#connections.get(url)
                    if (connection?.socket === socket) {
                        delete connection.socket
                    }
                }

                return merge(
                    fromEvent(socket, 'open').pipe(
                        take(1),
                        map(() => socket),
                    ),
                    merge(
                        fromEvent(socket, 'close'),
                        fromEvent(socket, 'error'),
                    ).pipe(
                        take(1),
                        switchMap(() => throwError(() => new Error(`WebSocket disconnected: ${url}`))),
                    )
                ).pipe(
                    take(1),
                    tap(() => {
                        const connection = this.#connections.get(url)
                        if (connection) {
                            connection.socket = socket
                        }
                    }),
                    switchMap(() => merge(
                        this.#me$.pipe(
                            tap(localNode => {
                                void this.#announceLocalNode(localNode).catch(() => undefined)
                                void this.#announceSubscriptions().catch(() => undefined)
                            }),
                            ignoreElements(),
                        ),
                        fromEvent<WebSocket.RawData | [WebSocket.RawData, boolean] | { data: WebSocket.RawData }>(socket, 'message').pipe(
                            tap(event => {
                                const raw = Array.isArray(event)
                                    ? event[0]
                                    : (event && typeof event === 'object' && 'data' in event ? event.data : event)

                                const frame = this.#parseFrame(raw)
                                if (!frame) return

                                const handler = this[`on_${frame.type}` as keyof WebsocketTransporter] as ((url: string, frame: ReceivedRelayFrame) => void) | undefined
                                if (!handler) return

                                handler.call(this, url, frame)
                            }),
                            ignoreElements(),
                        ),
                        merge(
                            fromEvent(socket, 'close'),
                            fromEvent(socket, 'error'),
                        ).pipe(
                            take(1),
                            switchMap(() => throwError(() => new Error(`WebSocket disconnected: ${url}`))),
                        ),
                        timer(0, this.options.heartbeatIntervalMs || 30000).pipe(
                            tap(() => {
                                if (socket.readyState === WebSocket.OPEN) {
                                    socket.ping()
                                }
                            }),
                            ignoreElements(),
                        ),
                    )),
                    finalize(() => {
                        disconnect()

                        if (socket.readyState === WebSocket.OPEN) {
                            socket.close()
                        } else if (socket.readyState === WebSocket.CONNECTING) {
                            socket.terminate()
                        }
                    })
                )
            }),
        ).pipe(retry({
            delay: () => timer(this.options.reconnectIntervalMs || 1000),
        }))
    }

    private on_request(_url: string, frame: ReceivedRelayRpcFrame) {
        this.on_rpc(frame)
    }

    private on_response(_url: string, frame: ReceivedRelayRpcFrame) {
        this.on_rpc(frame)
    }

    private on_cancel(_url: string, frame: ReceivedRelayRpcFrame) {
        this.on_rpc(frame)
    }

    private on_rpc(frame: ReceivedRelayRpcFrame) {
        const packet = this.#decodeRpcPacket(frame)
        if (!packet) return

        this.next({
            rpc: {
                node_id: frame.sender_id,
                packet
            }
        } satisfies RpcEvent)
    }

    private on_hello(url: string, frame: Extract<ReceivedRelayFrame, { type: 'hello' }>) {
        const node = frame.me
        this.#nodes.set(node.node_id, {
            node,
            relayUrl: url,
        })
        this.next({ discovered: node })
    }

    private on_offline(url: string, frame: Extract<ReceivedRelayFrame, { type: 'offline' }>) {
        const current = this.#nodes.get(frame.node_id)
        if (current?.relayUrl === url) {
            this.#nodes.delete(frame.node_id)
        }

        this.next({ offline: frame.node_id } satisfies RpcEvent)
    }

    private on_publish(_url: string, frame: Extract<ReceivedRelayFrame, { type: 'publish' }>) {
        const topic = this.#topics.get(frame.topic)
        if (!topic) return

        try {
            topic.subject.next(decode(frame.payload) as any)
        } catch {
            topic.subject.next(frame.payload)
        }
    }

    #selectRpcSocket(node: SpiderMeshNode) {
        const relayUrl = this.#nodes.get(node.node_id)?.relayUrl
        const routedSocket = relayUrl ? this.#connections.get(relayUrl)?.socket : null

        if (routedSocket?.readyState === WebSocket.OPEN) {
            return routedSocket
        }

        for (const { socket } of this.#connections.values()) {
            if (socket?.readyState === WebSocket.OPEN) {
                return socket
            }
        }

        throw new Error('WebSocket is not connected')
    }

    #resolveNode(nodeId?: string) {
        if (nodeId) {
            const knownNode = this.#nodes.get(nodeId)
            if (knownNode) {
                return knownNode.node
            }
        }

        return {
            ips: [],
            host: '',
            namespace: '',
            node_id: nodeId || '',
            services: {},
            transporters: {},
            nodes: {},
            version: 0,
        } satisfies SpiderMeshNode
    }

    async #announceLocalNode(localNode: SpiderMeshNode) {
        await this.#sendFrameToAll({
            type: 'hello',
            me: {
                ...localNode,
                online: true,
            },
        })
    }

    async #announceSubscriptions() {
        await this.#sendFrameToAll({
            type: 'subscribe',
            topics: [...this.#topics.keys()],
        })
    }

    async #announceUnsubscribe(topics: string[]) {
        if (topics.length === 0) return

        await this.#sendFrameToAll({
            type: 'unsubscribe',
            topics,
        })
    }

    #encodeRpcPacket(packet: RpcPacket) {
        const { kind: _, ...payload } = packet
        return encode(payload)
    }

    #decodeRpcPacket(frame: ReceivedRelayRpcFrame) {
        try {
            return {
                kind: frame.type,
                ...(decode(frame.payload) as Record<string, unknown>),
            } as RpcPacket
        } catch {
            return null
        }
    }

    async #sendFrame(socket: WebSocket | null | undefined, frame: RelayFrame) {
        if (!socket || socket.readyState !== WebSocket.OPEN) {
            throw new Error('WebSocket is not connected')
        }

        socket.send(encodeRelayFrame(frame), { binary: true })
    }

    async #sendFrameToAll(frame: RelayFrame) {
        const sockets = [...this.#connections.values()]
            .map(connection => connection.socket)
            .filter((socket): socket is WebSocket => socket?.readyState === WebSocket.OPEN)

        if (sockets.length === 0) {
            return
        }

        await Promise.all(sockets.map(socket => this.#sendFrame(socket, frame)))
    }

    #parseFrame(raw: WebSocket.RawData) {
        return decodeRelayFrame(normalizeRelayRawData(raw)) as ReceivedRelayFrame | null
    }
}