import { decode, encode } from '@msgpack/msgpack'
import { BehaviorSubject, defer, distinctUntilChanged, finalize, from, fromEvent, ignoreElements, map, merge, Observable, of, ReplaySubject, retry, share, Subject, Subscription, switchMap, take, takeUntil, tap, throwError, timer } from 'rxjs'
import type { NodeRef, RpcCancelPacket, RpcEvent, RpcProbeRequest, RpcProbeResult, RpcRequestPacket, RpcResponsePacket, RpcTransporter, RpcTransporterContext, SpiderMeshNode, Topology, TopologyDiscoveryContext } from '@spider-mesh/core'
import type { DiscoveryEvent, DiscoveryMessage, DiscoveryTransporter } from './discoveryTypes.js'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame, type RelayRawData } from './websocketProtocol.js'

/** Cấu hình heartbeat, reconnect và thời gian giữ subscription của WebSocket transporter. */
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

/** Interface socket tối thiểu dùng chung cho Node, browser và React Native. */
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

/** Các trạng thái kết nối relay được công bố qua `status$`. */
export type WebsocketConnectionStatus = 'connecting' | 'connected' | 'error' | 'not_connected'

const WEBSOCKET_CONNECTING = 0
const WEBSOCKET_OPEN = 1

function toDiscoveryEvent(node: SpiderMeshNode): DiscoveryEvent<SpiderMeshNode> {
    return {
        node_id: node.node_id,
        namespace: node.namespace,
        tags: ['spider-mesh', 'node'],
        version: String(node.version || 0),
        created_at: Date.now(),
        seq: Number(node.version || 0),
        data: node,
    }
}

/**
 * Transporter WebSocket dùng chung cho Node, browser và React Native.
 * Relay có thể tự routing khi không có Topology; cùng instance cũng có thể làm Discovery.
 */
export abstract class BaseWebsocketTransporter extends Subject<any> implements RpcTransporter, DiscoveryTransporter<SpiderMeshNode> {
    public readonly name = 'websocket'
    #connections = new Map<string, RelayConnection>()
    /**
     * Request đã gửi đi mà chưa nhận response kết thúc, theo socket đã mang nó. Khi socket đó
     * mất, relay không thể báo gì cho caller nữa, nên transporter phải tự kết thúc các request này.
     */
    #inflight = new Map<string, WebSocketLike>()
    #topics = new Map<string, TopicStream>()
    #nodes = new Map<string, KnownNode>()
    #nodes$ = new BehaviorSubject<KnownNode[]>([])
    #me$ = new ReplaySubject<SpiderMeshNode>(1)
    #localNodeId?: string
    #topology?: Topology
    #topologyContext?: TopologyDiscoveryContext
    #topologyBinding = Subscription.EMPTY
    #runtimeLocalBinding = Subscription.EMPTY
    public readonly status$ = new BehaviorSubject<Map<string, string>>(new Map())

    constructor(protected options: WebsocketTransporterOptions = {}) {
        super()
    }

    /** Core truyền Topology optional cho routing theo từng RPC request. */
    start(context: RpcTransporterContext) {
        this.#topology = context.topology
        this.#runtimeLocalBinding.unsubscribe()
        // Nếu chính transporter đang làm Discovery của Topology thì bind() đã announce local node.
        if (this.#topologyContext) return
        this.#runtimeLocalBinding = context.localNode$.subscribe(node => {
            void this.broadcast(toDiscoveryEvent(node))
        })
    }

    /** Dừng toàn bộ WebSocket connection của transporter. */
    stop() {
        this.#runtimeLocalBinding.unsubscribe()
        this.close()
    }

    /**
     * Kết nối WebSocket transporter như một Discovery của Topology.
     * Local node được announce lên relay, hello/offline từ relay cập nhật remote nodes.
     */
    bind(context: TopologyDiscoveryContext) {
        this.#topologyBinding.unsubscribe()
        this.#topologyContext = context
        this.#topologyBinding = context.localNode$.subscribe(node => {
            void this.broadcast(toDiscoveryEvent(node))
        })
        return new Subscription(() => {
            this.#topologyBinding.unsubscribe()
            if (this.#topologyContext === context) this.#topologyContext = undefined
        })
    }

    connect(url: string) {
        if (this.#connections.has(url)) return

        this.#setConnectionStatus(url, 'connecting')
        this.#connections.set(url, {
            subscription: this.#createConnectionLoop(url).subscribe(),
        })
    }

    close(url?: string) {
        if (!url) {
            for (const connectedUrl of [...this.#connections.keys()]) {
                this.close(connectedUrl)
            }
            return
        }

        const connection = this.#connections.get(url)
        const socket = connection?.socket

        this.#connections.delete(url)

        // An explicit close should not leave a Node `ws` peer waiting on the close
        // handshake (which would also keep a WebSocketServer.close callback pending).
        socket?.terminate?.()
        connection?.subscription.unsubscribe()
        if (socket && !socket.terminate) {
            this.#disconnectSocket(socket)
        }
        this.#deleteConnectionStatus(url)
    }


    async send(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket): Promise<{ cancel: () => void, destination_node_id?: string }> {
        const route = packet.kind === 'request' && packet.routing
            ? this.#topology?.route({
                service: packet.service,
                transporter: this.name,
                node_id: packet.destination_node_id,
                routing: packet.routing,
            })
            : undefined
        const outboundPacket = route && packet.kind === 'request'
            ? { ...packet, destination_node_id: route.node.node_id }
            : packet
        const socket = this.#selectRpcSocket(outboundPacket.destination_node_id)
        await this.#sendFrame(socket, {
            type: 'rpc',
            data: outboundPacket,
        })

        if (packet.kind === 'request') {
            this.#inflight.set(packet.request_id, socket)
            return {
                // Core cần biết node đã được Topology chọn để đóng stream khi node đó offline.
                destination_node_id: outboundPacket.destination_node_id,
                cancel: () => {
                    this.#inflight.delete(packet.request_id)
                    void this.#sendFrame(socket, {
                        type: 'rpc',
                        data: {
                            kind: 'cancel',
                            request_id: packet.request_id,
                            destination_node_id: outboundPacket.destination_node_id,
                        },
                    }).catch(() => undefined)
                }
            }
        }

        return { cancel: () => {} }
    }

    async broadcast(message: DiscoveryMessage<SpiderMeshNode>) {
        const localNode = { ...message.data } satisfies SpiderMeshNode
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
            const created = this.createSocket(url)
            return created && typeof (created as Promise<WebSocketLike>).then === 'function'
                ? from(created as Promise<WebSocketLike>)
                : of(created as WebSocketLike)
        }).pipe(
            switchMap(socket => {
                const closed$ = fromEvent(socket as never, 'close').pipe(
                    take(1),
                    tap(() => {
                        this.#setConnectionStatus(url, 'not_connected')
                        this.#reportDirectoryUnreachable('relay-connection-lost')
                    }),
                    switchMap(() => throwError(() => new Error(`WebSocket disconnected: ${url}`))),
                )

                const errored$ = fromEvent(socket as never, 'error').pipe(
                    take(1),
                    tap(() => {
                        this.#setConnectionStatus(url, 'error')
                        this.#reportDirectoryUnreachable('relay-connection-error')
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
                        this.#failInflight(socket, url)
                    })
                )
            }),
        ).pipe(retry({
            delay: () => timer(this.options.reconnectIntervalMs || 1000),
        }))
    }

    private on_rpc(_url: string, frame: RelayFrame) {
        if (frame.type !== 'rpc') return
        const packet = frame.data
        if (packet.kind === 'response' && (packet.completed || packet.error != undefined)) {
            this.#inflight.delete(packet.request_id)
        }
        this.next({ rpc: packet } satisfies RpcEvent)
    }

    /**
     * Kết thúc mọi request đang bay trên một socket vừa mất bằng `MICROSERVICE_OFFLINE`, như thể
     * relay đã trả lời. Không đụng tới membership: mất relay không có nghĩa là node đã chết.
     * Cũng không tự gửi lại qua relay khác, vì request có thể đã chạy ở provider; caller tự quyết
     * bằng `retry`.
     */
    #failInflight(socket: WebSocketLike, url: string) {
        for (const [request_id, owner] of this.#inflight) {
            if (owner !== socket) continue
            this.#inflight.delete(request_id)
            this.next({
                rpc: {
                    kind: 'response',
                    request_id,
                    error: {
                        code: 'MICROSERVICE_OFFLINE',
                        message: `WebSocket relay connection ${url} was lost while the request was in flight`,
                    },
                    completed: true,
                },
            } satisfies RpcEvent)
        }
    }

    private on_hello(url: string, frame: Extract<RelayFrame, { type: 'hello' }>) {
        const node = frame.me
        if (node.node_id === this.#localNodeId) return

        const existing = this.#nodes.get(node.node_id)
        this.#nodes.set(node.node_id, { node, relayUrl: url })
        this.#publishNodes()
        this.#topologyContext?.upsertRemote(node)
        this.#topology?.reportReachability({
            node_id: node.node_id,
            transporter: this.name,
            status: 'reachable',
        })

        if (existing && this.#hasSameServices(existing.node, node)) return
        this.next(toDiscoveryEvent(node))
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
            this.#topologyContext?.removeRemote(frame.node_id)
        }

        this.next({ offline: frame.node_id } satisfies RpcEvent)
    }

    // --- Relay-backed availability for core's wait()/watch()/nodes ---

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

    // Reachability probe used by core's #selectRpcTransport. Mirrors #selectRpcSocket:
    // we can route only while at least one relay socket is open AND the relay has reported
    // a node serving `service` (the specific `node_id`, when given).
    canRoute(service: string, node_id?: string): boolean {
        const hasOpenSocket = [...this.#connections.values()].some(
            connection => connection.socket?.readyState === WEBSOCKET_OPEN
        )
        if (!hasOpenSocket) return false
        const nodes = this.listNodes(service)
        return node_id ? nodes.some(node => node.node_id === node_id) : nodes.length > 0
    }

    /** Relay directory cung cấp reachability tối thiểu khi SpiderMesh không có Topology. */
    async probe(request: RpcProbeRequest): Promise<RpcProbeResult> {
        const startedAt = Date.now()
        const nodes = this.listNodes(request.service)
        const node = request.node_id
            ? nodes.find(candidate => candidate.node_id === request.node_id)
            : nodes[0]
        return {
            reachable: !!node && this.canRoute(request.service, request.node_id),
            node_id: node?.node_id,
            latency: Date.now() - startedAt,
        }
    }

    /** Relay directory chỉ xác minh membership khi còn ít nhất một socket đang kết nối. */
    async verify(node_id: string) {
        const connected = [...this.#connections.values()].some(
            connection => connection.socket?.readyState === WEBSOCKET_OPEN
        )
        if (!connected) return 'unknown' as const
        return this.#nodes.has(node_id) ? 'alive' as const : 'dead' as const
    }

    /**
     * Khi mất toàn bộ relay, transporter chỉ báo endpoint unreachable.
     * Membership vẫn do offline frame/Discovery quyết định; verify lúc này sẽ trả unknown.
     */
    #reportDirectoryUnreachable(reason: string) {
        const hasAnotherOpenSocket = [...this.#connections.values()].some(
            connection => connection.socket?.readyState === WEBSOCKET_OPEN
        )
        if (hasAnotherOpenSocket) return

        for (const { node } of this.#nodes.values()) {
            this.#topology?.reportReachability({
                node_id: node.node_id,
                transporter: this.name,
                status: 'unreachable',
                reason,
            })
        }
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
