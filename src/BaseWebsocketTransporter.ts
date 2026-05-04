import { decode, encode } from '@msgpack/msgpack'
import { BehaviorSubject, defer, finalize, from, fromEvent, ignoreElements, map, merge, Observable, ReplaySubject, retry, share, Subject, Subscription, switchMap, take, takeUntil, tap, throwError, timer } from 'rxjs'
import type { DiscoveryTransporter, MdnsMessage, NodeMetadata, PubsubTransporter, RpcEvent, RpcPacket, RpcTransporter, SpiderMeshNode } from '@spider-mesh/core'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame, type RelayRawData, type ReceivedRelayFrame, type ReceivedRelayRpcFrame } from './websocketProtocol.js'

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

export type WebSocketLike = {
    readyState: number
    binaryType?: BinaryType
    close(): void
    send(data: Uint8Array, options?: { binary?: boolean }): void
    ping?(): void
    terminate?(): void
}

type SocketMessageEvent = RelayRawData | [RelayRawData, boolean] | { data: RelayRawData }

type RelayConnection = {
    subscription: Subscription
    socket?: WebSocketLike
}

export type WebsocketConnectionStatus = 'connecting' | 'connected' | 'error' | 'not_connected'

const WEBSOCKET_CONNECTING = 0
const WEBSOCKET_OPEN = 1

export abstract class BaseWebsocketTransporter extends Subject<any> implements RpcTransporter, DiscoveryTransporter, PubsubTransporter {
    #connections = new Map<string, RelayConnection>()
    #topics = new Map<string, TopicStream>()
    #nodes = new Map<string, KnownNode>()
    #me$ = new ReplaySubject<SpiderMeshNode>(1)
    public readonly status$ = new BehaviorSubject<Map<string, string>>(new Map())

    constructor(protected options: WebsocketTransporterOptions = {}) {
        super()
    }

    get metadata() {
        return {}
    }

    connect(url: string) {
        if (this.#connections.has(url)) return

        this.#setConnectionStatus(url, 'connecting')
        this.#connections.set(url, {
            subscription: this.#createConnectionLoop(url).subscribe(),
        })
    }

    close(url: string) {
        const connection = this.#connections.get(url)
        const socket = connection?.socket

        this.#connections.delete(url)
        this.#deleteConnectionStatus(url)

        connection?.subscription.unsubscribe()
        if (socket) {
            this.#disconnectSocket(socket)
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

    async broadcast(data: MdnsMessage<NodeMetadata>) {
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

    protected abstract createSocket(url: string): Promise<WebSocketLike> | WebSocketLike

    #createConnectionLoop(url: string) {
        return defer(() => {
            this.#setConnectionStatus(url, 'connecting')
            return from(Promise.resolve(this.createSocket(url)))
        }).pipe(
            switchMap(socket => {
                const closed$ = fromEvent(socket as never, 'close').pipe(
                    take(1),
                    tap(() => {
                        this.#setConnectionStatus(url, 'not_connected')
                    }),
                    switchMap(() => throwError(() => new Error(`WebSocket disconnected: ${url}`))),
                )

                const errored$ = fromEvent(socket as never, 'error').pipe(
                    take(1),
                    tap(() => {
                        this.#setConnectionStatus(url, 'error')
                    }),
                    switchMap(() => throwError(() => new Error(`WebSocket disconnected: ${url}`))),
                )

                const disconnection$ = merge(closed$, errored$).pipe(take(1))

                const clearSocket = () => {
                    const connection = this.#connections.get(url)
                    if (connection && connection.socket === socket) {
                        delete connection.socket
                    }
                }

                return fromEvent(socket as never, 'open').pipe(
                    map(() => socket),
                    takeUntil(disconnection$),
                    tap(() => {
                        const connection = this.#connections.get(url)
                        if (connection) {
                            connection.socket = socket
                        }
                        this.#setConnectionStatus(url, 'connected')
                    }),
                    switchMap(() => merge(
                        this.#me$.pipe(
                            tap(localNode => {
                                void this.#announceLocalNode(localNode).catch(() => undefined)
                                void this.#announceSubscriptions().catch(() => undefined)
                            }),
                            ignoreElements(),
                        ),
                        fromEvent<SocketMessageEvent>(socket as never, 'message').pipe(
                            tap(event => {
                                const raw = this.#getMessageData(event)

                                const frame = this.#parseFrame(raw)
                                if (!frame) return

                                const handler = this[`on_${frame.type}` as keyof BaseWebsocketTransporter] as ((url: string, frame: ReceivedRelayFrame) => void) | undefined
                                if (!handler) return

                                handler.call(this, url, frame)
                            }),
                            ignoreElements(),
                        ),
                        timer(0, this.options.heartbeatIntervalMs || 30000).pipe(
                            tap(() => {
                                if (socket.readyState === WEBSOCKET_OPEN) {
                                    socket.ping?.()
                                }
                            }),
                            ignoreElements(),
                        ),
                    )),
                    takeUntil(disconnection$),
                    finalize(() => {
                        clearSocket()
                        this.#disconnectSocket(socket)
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
        const routedSocket = relayUrl ? this.#connections.get(relayUrl)?.socket : undefined

        if (routedSocket?.readyState === WEBSOCKET_OPEN) {
            return routedSocket
        }

        for (const { socket } of this.#connections.values()) {
            if (socket?.readyState === WEBSOCKET_OPEN) {
                return socket
            }
        }

        throw new Error('WebSocket is not connected')
    }

    async #announceLocalNode(localNode: SpiderMeshNode) {
        await this.#sendFrameToAll({
            type: 'hello',
            me: localNode,
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

    async #sendFrame(socket: WebSocketLike | null | undefined, frame: RelayFrame) {
        if (!socket || socket.readyState !== WEBSOCKET_OPEN) {
            throw new Error('WebSocket is not connected')
        }

        socket.send(encodeRelayFrame(frame), { binary: true })
    }

    async #sendFrameToAll(frame: RelayFrame) {
        const sockets = [...this.#connections.values()]
            .map(connection => connection.socket)
            .filter((socket): socket is WebSocketLike => socket?.readyState === WEBSOCKET_OPEN)

        if (sockets.length === 0) {
            return
        }

        await Promise.all(sockets.map(socket => this.#sendFrame(socket, frame)))
    }

    #parseFrame(raw: RelayRawData) {
        return decodeRelayFrame(normalizeRelayRawData(raw)) as ReceivedRelayFrame | null
    }

    #getMessageData(event: SocketMessageEvent) {
        return Array.isArray(event)
            ? event[0]
            : (event && typeof event === 'object' && 'data' in event ? event.data : event)
    }

    #disconnectSocket(socket: WebSocketLike) {
        if (socket.readyState === WEBSOCKET_OPEN) {
            socket.close()
        } else if (socket.readyState === WEBSOCKET_CONNECTING) {
            socket.terminate?.()
            socket.close()
        }
    }

    #setConnectionStatus(url: string, status: WebsocketConnectionStatus) {
        const currentStatus = this.status$.value.get(url)
        if (currentStatus === status) {
            return
        }

        const nextStatuses = new Map(this.status$.value)
        nextStatuses.set(url, status)
        this.status$.next(nextStatuses)
    }

    #deleteConnectionStatus(url: string) {
        if (!this.status$.value.has(url)) {
            return
        }

        const nextStatuses = new Map(this.status$.value)
        nextStatuses.delete(url)
        this.status$.next(nextStatuses)
    }
}