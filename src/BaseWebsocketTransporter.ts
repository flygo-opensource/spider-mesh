import { decode, encode } from '@msgpack/msgpack'
import { BehaviorSubject, defer, distinctUntilChanged, finalize, from, fromEvent, ignoreElements, map, merge, Observable, ReplaySubject, retry, share, Subject, Subscription, switchMap, take, takeUntil, tap, throwError, timer } from 'rxjs'
import type { DiscoveryTransporter, MdnsMessage, NodeMetadata, NodeRef, PubsubTransporter, RpcEvent, RpcRequestPacket, RpcResponsePacket, RpcTransporter, ServiceDirectory, SpiderMeshNode } from '@spider-mesh/core'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame, type RelayRawData } from './websocketProtocol.js'

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

export abstract class BaseWebsocketTransporter extends Subject<any> implements RpcTransporter, DiscoveryTransporter, PubsubTransporter, ServiceDirectory {
    #connections = new Map<string, RelayConnection>()
    #topics = new Map<string, TopicStream>()
    #nodes = new Map<string, KnownNode>()
    #nodes$ = new BehaviorSubject<KnownNode[]>([])
    #me$ = new ReplaySubject<SpiderMeshNode>(1)
    #localNodeId?: string
    public readonly status$ = new BehaviorSubject<Map<string, string>>(new Map())

    constructor(protected options: WebsocketTransporterOptions = {}) {
        super()
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


    async send(packet: RpcRequestPacket | RpcResponsePacket): Promise<{ cancel: () => void }> {
        const socket = this.#selectRpcSocket(packet.destination_node_id)
        await this.#sendFrame(socket, {
            type: 'rpc',
            data: packet,
        })

        if (packet.kind === 'request') {
            return {
                cancel: () => {
                    void this.#sendFrame(socket, {
                        type: 'rpc',
                        data: { kind: 'cancel', request_id: packet.request_id },
                    }).catch(() => undefined)
                }
            }
        }

        return { cancel: () => {} }
    }

    async broadcast(data: MdnsMessage<SpiderMeshNode>) {
        const localNode = {
            ...data.node,
            node_id: data.sender_id,
        } satisfies SpiderMeshNode
        this.#localNodeId = localNode.node_id
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

                                const handler = this[`on_${frame.type}` as keyof BaseWebsocketTransporter] as ((url: string, frame: RelayFrame) => void) | undefined
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

    private on_rpc(_url: string, frame: RelayFrame) {
        if (frame.type !== 'rpc') return
        this.next({ rpc: frame.data } satisfies RpcEvent)
    }

    private on_hello(url: string, frame: Extract<RelayFrame, { type: 'hello' }>) {
        const node = frame.me
        if (node.node_id === this.#localNodeId) return

        const existing = this.#nodes.get(node.node_id)
        this.#nodes.set(node.node_id, { node, relayUrl: url })
        this.#publishNodes()

        if (existing && this.#hasSameServices(existing.node, node)) return
        this.next({ discovered: node })
    }

    #hasSameServices(a: SpiderMeshNode, b: SpiderMeshNode): boolean {
        const aKeys = Object.keys(a.services || {}).sort()
        const bKeys = Object.keys(b.services || {}).sort()
        if (aKeys.length !== bKeys.length) return false
        return aKeys.every((k, i) => k === bKeys[i])
    }

    private on_offline(url: string, frame: Extract<RelayFrame, { type: 'offline' }>) {
        const current = this.#nodes.get(frame.node_id)
        if (current?.relayUrl === url) {
            this.#nodes.delete(frame.node_id)
            this.#publishNodes()
        }

        this.next({ offline: frame.node_id } satisfies RpcEvent)
    }

    // --- ServiceDirectory: relay-backed availability for core's wait()/watch()/nodes ---

    #publishNodes() {
        this.#nodes$.next([...this.#nodes.values()])
    }

    #nodesForService(nodes: KnownNode[], service: string): NodeRef[] {
        return nodes
            .filter(known => !!known.node.services && service in known.node.services)
            .map(known => known.node)
    }

    watchService(service: string): Observable<NodeRef[]> {
        return this.#nodes$.pipe(
            map(nodes => this.#nodesForService(nodes, service)),
            distinctUntilChanged((prev, curr) => {
                if (prev.length !== curr.length) return false
                const prevIds = new Set(prev.map(n => n.node_id))
                return curr.every(n => prevIds.has(n.node_id))
            })
        )
    }

    listNodes(service: string): NodeRef[] {
        return this.#nodesForService([...this.#nodes.values()], service)
    }

    private on_publish(_url: string, frame: Extract<RelayFrame, { type: 'publish' }>) {
        const topic = this.#topics.get(frame.topic)
        if (!topic) return

        try {
            topic.subject.next(decode(frame.payload) as any)
        } catch {
            topic.subject.next(frame.payload)
        }
    }

    #selectRpcSocket(node_id?: string) {
        const relayUrl = node_id ? this.#nodes.get(node_id)?.relayUrl : undefined
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
        return decodeRelayFrame(normalizeRelayRawData(raw)) as RelayFrame | null
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