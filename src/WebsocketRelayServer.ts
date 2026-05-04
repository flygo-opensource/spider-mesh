import { WebSocket, WebSocketServer as WsServer } from 'ws'
import { fromEvent, ignoreElements, merge, mergeMap, Subscription, take, tap } from 'rxjs'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame, type ReceivedRelayFrame, type ReceivedRelayHelloFrame, type ReceivedRelayPublishFrame, type ReceivedRelaySubscribeFrame, type ReceivedRelayUnsubscribeFrame, type RelayHelloFrame } from './websocketProtocol.js'

type RelayServerConnectionRequest = {
    url?: string
}

const relaySocketState = Symbol('relaySocketState')

type RelaySocket = WebSocket & {
    [relaySocketState]?: {
        isServerConnection: boolean
        metadata?: ReceivedRelayHelloFrame['me']
    }
}

export type WebsocketRelayServerOptions = {
    port?: number
    host?: string
    path?: string
    isServerConnection?: (socket: WebSocket, request: RelayServerConnectionRequest) => boolean
}

export class WebsocketRelayServer {
    #server: WsServer
    #subscription = Subscription.EMPTY
    #nodes = new Map<string, WebSocket>()
    #listeners = new Map<string, Set<string>>()

    constructor(options: WebsocketRelayServerOptions = {}) {
        this.#server = new WsServer({
            host: options.host,
            path: options.path,
            port: options.port || 8787,
        })

        this.#subscription = fromEvent<[WebSocket, RelayServerConnectionRequest]>(this.#server, 'connection').pipe(
            mergeMap(([socket, request]) => this.on_connection(socket, request, options)),
        ).subscribe()
    }

    get port() {
        const address = this.#server.address()
        return typeof address === 'object' && address ? address.port : null
    }

    close() {
        this.#subscription.unsubscribe()
        this.#server.close()
    }

    private on_connection(socket: WebSocket, request: RelayServerConnectionRequest, options: WebsocketRelayServerOptions) {
        const isServerConnection = options.isServerConnection ? options.isServerConnection(socket, request) : true
        ;(socket as RelaySocket)[relaySocketState] = {
            isServerConnection,
        }

        if (isServerConnection) {
            queueMicrotask(() => this.#syncClientState(socket))
        }

        return merge(
            fromEvent<WebSocket.RawData | [WebSocket.RawData, boolean] | { data: WebSocket.RawData }>(socket, 'message').pipe(
                tap(event => {
                    const raw = Array.isArray(event)
                        ? event[0]
                        : (event && typeof event === 'object' && 'data' in event ? event.data : event)

                    this.on_message(socket, raw, isServerConnection)
                }),
                ignoreElements(),
            ),
            fromEvent(socket, 'close').pipe(
                take(1),
                tap(() => this.on_close(socket)),
                ignoreElements(),
            ),
        )
    }

    private on_message(socket: WebSocket, raw: WebSocket.RawData, isServerConnection: boolean) {
        const frame = this.#parseFrame(raw)
        if (!frame) return

        if (frame.type === 'hello') {
            const normalized = this.#normalizeHelloFrame(socket, frame)
            if (!normalized) return

            const previous = this.#getSocketMetadata(this.#nodes.get(normalized.sender_id))
            this.#registerNode(socket, normalized)

            if (this.#shouldSyncDiscovery(previous, normalized.me)) {
                this.#syncServerConnections()
            }
            return
        }

        const senderId = this.#resolveSenderId(socket)
        if (!senderId) return
        const normalized = { ...frame, sender_id: senderId } as ReceivedRelayFrame

        if (this.#hasTargetId(normalized) && normalized.target_id) {
            if (this.#requiresServerRole(normalized) && !isServerConnection) {
                return
            }

            const target = this.#nodes.get(normalized.target_id)
            if (target?.readyState === WebSocket.OPEN) {
                target.send(encodeRelayFrame(normalized), { binary: true })
            }
            return
        }

        const handler = this[`on_${normalized.type}` as keyof WebsocketRelayServer] as ((frame: ReceivedRelayFrame, sender: WebSocket) => void) | undefined
        if (!handler) {
            this.on_broadcast(normalized, socket)
            return
        }

        handler.call(this, normalized, socket)
    }

    private on_close(socket: WebSocket) {
        const nodeId = this.#resolveSenderId(socket)
        if (!nodeId) return

        this.#nodes.delete(nodeId)
        this.#removeTopicListeners(nodeId)
        this.on_broadcast({
            type: 'offline',
            sender_id: nodeId,
            node_id: nodeId,
        }, socket)
        this.#syncServerConnections()
    }

    #registerNode(socket: WebSocket, discovery: ReceivedRelayHelloFrame) {
        const nodeId = discovery.sender_id
        const previousNodeId = this.#resolveSenderId(socket)
        if (previousNodeId && previousNodeId !== nodeId) {
            this.#nodes.delete(previousNodeId)
            this.#removeTopicListeners(previousNodeId)
            this.on_broadcast({
                type: 'offline',
                sender_id: previousNodeId,
                node_id: previousNodeId,
            }, socket)
        }

        this.#nodes.set(nodeId, socket)
        this.#setSocketMetadata(socket, discovery.me)
    }

    #isServerConnection(socket: WebSocket) {
        return (socket as RelaySocket)[relaySocketState]?.isServerConnection === true
    }

    #setSocketMetadata(socket: WebSocket, metadata: ReceivedRelayHelloFrame['me']) {
        const relaySocket = socket as RelaySocket
        const current = relaySocket[relaySocketState]
        if (!current) return

        relaySocket[relaySocketState] = {
            isServerConnection: current.isServerConnection,
            metadata,
        }
    }

    #getSocketMetadata(socket?: WebSocket) {
        return socket ? (socket as RelaySocket)[relaySocketState]?.metadata : undefined
    }

    #requiresServerRole(frame: RelayFrame | ReceivedRelayFrame) {
        return frame.type === 'request' || frame.type === 'cancel'
    }

    #hasTargetId(frame: RelayFrame | ReceivedRelayFrame): frame is (RelayFrame | ReceivedRelayFrame) & { target_id?: string } {
        return 'target_id' in frame
    }

    #normalizeHelloFrame(socket: WebSocket, frame: RelayHelloFrame) {
        const senderId = frame.me?.node_id
        if (!senderId) return null

        return {
            ...frame,
            sender_id: senderId,
            me: {
                ...frame.me,
            },
        } satisfies ReceivedRelayHelloFrame
    }

    #shouldSyncDiscovery(previous: ReceivedRelayHelloFrame['me'] | undefined, next: ReceivedRelayHelloFrame['me']) {
        if (!previous) return true
        return !this.#hasSameServiceList(previous.services || {}, next.services || {})
    }

    #hasSameServiceList(left: Record<string, unknown>, right: Record<string, unknown>) {
        const leftKeys = Object.keys(left).sort()
        const rightKeys = Object.keys(right).sort()

        if (leftKeys.length !== rightKeys.length) return false

        for (let index = 0; index < leftKeys.length; index++) {
            if (leftKeys[index] !== rightKeys[index]) return false
        }

        return true
    }

    #removeTopicListeners(nodeId: string) {
        for (const [topic, listeners] of this.#listeners) {
            listeners.delete(nodeId)
            if (listeners.size === 0) {
                this.#listeners.delete(topic)
            }
        }
    }

    #resolveSenderId(socket: WebSocket) {
        const metadata = this.#getSocketMetadata(socket)
        return metadata?.node_id || null
    }

    #syncClientState(socket: WebSocket) {
        if (!this.#isServerConnection(socket)) return

        for (const [node_id, client] of this.#nodes) {
            if (socket.readyState !== WebSocket.OPEN) return
            const metadata = this.#getSocketMetadata(client)
            if (!metadata) continue
            socket.send(encodeRelayFrame({
                type: 'hello',
                sender_id: node_id,
                me: metadata,
            }), { binary: true })
        }
    }

    #syncServerConnections() {
        for (const client of this.#server.clients) {
            if (client.readyState !== WebSocket.OPEN) continue
            if (!this.#isServerConnection(client)) continue
            this.#syncClientState(client)
        }
    }

    private on_subscribe(frame: ReceivedRelaySubscribeFrame) {
        this.#removeTopicListeners(frame.sender_id)

        for (const topic of new Set(frame.topics.filter(Boolean))) {
            const listeners = this.#listeners.get(topic) || new Set<string>()
            listeners.add(frame.sender_id)
            this.#listeners.set(topic, listeners)
        }
    }

    private on_unsubscribe(frame: ReceivedRelayUnsubscribeFrame) {
        for (const topic of new Set(frame.topics.filter(Boolean))) {
            const listeners = this.#listeners.get(topic)
            if (!listeners) continue

            listeners.delete(frame.sender_id)
            if (listeners.size === 0) {
                this.#listeners.delete(topic)
            }
        }
    }

    private on_publish(frame: ReceivedRelayPublishFrame) {
        const listeners = this.#listeners.get(frame.topic)
        if (!listeners || listeners.size === 0) return

        const message = encodeRelayFrame(frame)
        const delivered = new Set<WebSocket>()

        for (const nodeId of listeners) {
            const client = this.#nodes.get(nodeId)
            if (!client || client.readyState !== WebSocket.OPEN || delivered.has(client)) continue
            client.send(message, { binary: true })
            delivered.add(client)
        }
    }

    private on_broadcast(frame: ReceivedRelayFrame, sender: WebSocket) {
        const message = encodeRelayFrame(frame)
        for (const client of this.#server.clients) {
            if (client === sender || client.readyState !== WebSocket.OPEN) continue
            if (!this.#isServerConnection(client)) continue
            client.send(message, { binary: true })
        }
    }

    #parseFrame(raw: WebSocket.RawData) {
        return decodeRelayFrame(normalizeRelayRawData(raw))
    }
}