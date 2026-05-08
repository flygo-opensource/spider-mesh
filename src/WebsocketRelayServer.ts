import { WebSocket, WebSocketServer as WsServer } from 'ws'
import { fromEvent, ignoreElements, merge, mergeMap, Subscription, take, tap } from 'rxjs'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame, type RelayHelloFrame, type RelayPublishFrame, type RelaySubscribeFrame, type RelayUnsubscribeFrame } from './websocketProtocol.js'

type RelayServerConnectionRequest = {
    url?: string
}

const relaySocketState = Symbol('relaySocketState')

type RelaySocket = WebSocket & {
    [relaySocketState]?: {
        isServerConnection: boolean
        metadata?: RelayHelloFrame['me']
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
    #services = new Map<string, Set<string>>()
    #serviceRrIndex = new Map<string, number>()
    #pendingRequests = new Map<string, WebSocket>()

    constructor(options: WebsocketRelayServerOptions = {}) {
        this.#server = new WsServer({
            host: options.host,
            path: options.path,
            port: options.port ?? 8787,
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
            const nodeId = frame.me?.node_id
            if (!nodeId) return

            const previous = this.#getSocketMetadata(this.#nodes.get(nodeId))
            this.#registerNode(socket, frame, nodeId)

            if (this.#shouldSyncDiscovery(previous, frame.me)) {
                this.#syncServerConnections()
            }
            return
        }

        if (frame.type === 'rpc') {
            const packet = frame.data

            if (packet.kind === 'response') {
                if (packet.destination_node_id) {
                    const target = this.#nodes.get(packet.destination_node_id)
                    if (target?.readyState === WebSocket.OPEN) {
                        target.send(encodeRelayFrame(frame), { binary: true })
                    }
                }
                if (packet.completed || packet.error) {
                    this.#pendingRequests.delete(packet.request_id)
                }
                return
            }

            if (!isServerConnection) return

            if (packet.kind === 'request') {
                const targetNodeId = packet.destination_node_id ?? this.#pickServiceNode(packet.service)
                if (!targetNodeId) {
                    this.#sendOfflineError(socket, packet.request_id, packet.service)
                    return
                }
                const target = this.#nodes.get(targetNodeId)
                if (target?.readyState === WebSocket.OPEN) {
                    this.#pendingRequests.set(packet.request_id, target)
                    target.send(encodeRelayFrame(frame), { binary: true })
                }
                return
            }

            if (packet.kind === 'cancel') {
                const providerSocket = this.#pendingRequests.get(packet.request_id)
                if (!providerSocket) return
                this.#pendingRequests.delete(packet.request_id)
                if (providerSocket.readyState === WebSocket.OPEN) {
                    providerSocket.send(encodeRelayFrame(frame), { binary: true })
                }
                return
            }

            return
        }

        const senderId = this.#resolveSenderId(socket)
        if (!senderId) return

        const handler = this[`on_${frame.type}` as keyof WebsocketRelayServer] as ((frame: RelayFrame, sender: WebSocket) => void) | undefined
        if (!handler) {
            this.on_broadcast(frame, socket)
            return
        }

        handler.call(this, frame, socket)
    }

    private on_close(socket: WebSocket) {
        const nodeId = this.#resolveSenderId(socket)
        if (!nodeId) return

        this.#nodes.delete(nodeId)
        this.#removeTopicListeners(nodeId)
        this.#removeFromServiceIndex(nodeId)

        for (const [requestId, providerSocket] of this.#pendingRequests) {
            if (providerSocket === socket) {
                this.#pendingRequests.delete(requestId)
            }
        }

        this.on_broadcast({
            type: 'offline',
            node_id: nodeId,
        }, socket)
        this.#syncServerConnections()
    }

    #registerNode(socket: WebSocket, frame: RelayHelloFrame, nodeId: string) {
        const previousNodeId = this.#resolveSenderId(socket)
        if (previousNodeId && previousNodeId !== nodeId) {
            this.#nodes.delete(previousNodeId)
            this.#removeTopicListeners(previousNodeId)
            this.#removeFromServiceIndex(previousNodeId)
            this.on_broadcast({
                type: 'offline',
                node_id: previousNodeId,
            }, socket)
        }

        this.#nodes.set(nodeId, socket)
        this.#setSocketMetadata(socket, frame.me)
        this.#updateServiceIndex(nodeId, frame.me)
    }

    #updateServiceIndex(nodeId: string, metadata: RelayHelloFrame['me']) {
        for (const nodes of this.#services.values()) {
            nodes.delete(nodeId)
        }

        for (const service of Object.keys(metadata.services || {})) {
            const nodes = this.#services.get(service) ?? new Set<string>()
            nodes.add(nodeId)
            this.#services.set(service, nodes)
        }
    }

    #removeFromServiceIndex(nodeId: string) {
        for (const [service, nodes] of this.#services) {
            nodes.delete(nodeId)
            if (nodes.size === 0) {
                this.#services.delete(service)
                this.#serviceRrIndex.delete(service)
            }
        }
    }

    #pickServiceNode(service: string): string | null {
        const nodeIds = this.#services.get(service)
        if (!nodeIds || nodeIds.size === 0) return null

        const candidates = [...nodeIds].filter(id => this.#nodes.get(id)?.readyState === WebSocket.OPEN)
        if (candidates.length === 0) return null

        const current = this.#serviceRrIndex.get(service) ?? -1
        const next = (current + 1) % candidates.length
        this.#serviceRrIndex.set(service, next)
        return candidates[next]
    }

    #sendOfflineError(socket: WebSocket, requestId: string, service: string) {
        if (socket.readyState !== WebSocket.OPEN) return

        socket.send(encodeRelayFrame({
            type: 'rpc',
            data: {
                kind: 'response',
                request_id: requestId,
                error: { code: 'MICROSERVICE_OFFLINE', message: `No available node for service ${service}` },
                completed: true,
            },
        } satisfies RelayFrame), { binary: true })
    }

    #isServerConnection(socket: WebSocket) {
        return (socket as RelaySocket)[relaySocketState]?.isServerConnection === true
    }

    #setSocketMetadata(socket: WebSocket, metadata: RelayHelloFrame['me']) {
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

    #shouldSyncDiscovery(previous: RelayHelloFrame['me'] | undefined, next: RelayHelloFrame['me']) {
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

        for (const [, client] of this.#nodes) {
            if (socket.readyState !== WebSocket.OPEN) return
            if (client === socket) continue
            const metadata = this.#getSocketMetadata(client)
            if (!metadata) continue
            socket.send(encodeRelayFrame({
                type: 'hello',
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

    private on_subscribe(frame: RelaySubscribeFrame, sender: WebSocket) {
        const senderId = this.#resolveSenderId(sender)
        if (!senderId) return

        this.#removeTopicListeners(senderId)

        for (const topic of new Set(frame.topics.filter(Boolean))) {
            const listeners = this.#listeners.get(topic) || new Set<string>()
            listeners.add(senderId)
            this.#listeners.set(topic, listeners)
        }
    }

    private on_unsubscribe(frame: RelayUnsubscribeFrame, sender: WebSocket) {
        const senderId = this.#resolveSenderId(sender)
        if (!senderId) return

        for (const topic of new Set(frame.topics.filter(Boolean))) {
            const listeners = this.#listeners.get(topic)
            if (!listeners) continue

            listeners.delete(senderId)
            if (listeners.size === 0) {
                this.#listeners.delete(topic)
            }
        }
    }

    private on_publish(frame: RelayPublishFrame) {
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

    private on_broadcast(frame: RelayFrame, sender: WebSocket) {
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
