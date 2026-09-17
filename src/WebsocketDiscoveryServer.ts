import { WebSocket, WebSocketServer as WsServer } from 'ws'
import { BehaviorSubject, fromEvent, ignoreElements, merge, mergeMap, Subscription, take, tap } from 'rxjs'
import type { SpiderMeshNode } from '@spider-mesh/core'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame } from './websocketProtocol.js'

/** Metrics quan sát của một node đang kết nối discovery server. */
export type NodeMetrics = {
    node_id: string
    host: string
    namespace: string
    services: string[]
    version: number
    online_since: number
    last_seen: number
}

/** Snapshot metrics tổng hợp của WebSocket discovery server. */
export type DiscoveryMetrics = {
    total_nodes: number
    nodes: NodeMetrics[]
    services: Record<string, string[]>
    updated_at: number
}

/** Cấu hình địa chỉ listen của WebSocket discovery server. */
export type WebsocketDiscoveryServerOptions = {
    port?: number
    host?: string
    path?: string
}

const socketNodeId = Symbol('socketNodeId')
type TrackedSocket = WebSocket & { [socketNodeId]?: string }

/** Server đồng bộ hello/offline và cung cấp metrics; không truyền RPC. */
export class WebsocketDiscoveryServer {
    #server: WsServer
    #subscription = Subscription.EMPTY
    #metadata = new Map<string, SpiderMeshNode>()
    #sockets = new Map<string, WebSocket>()
    #onlineAt = new Map<string, number>()
    #lastSeen = new Map<string, number>()

    readonly metrics$ = new BehaviorSubject<DiscoveryMetrics>({
        total_nodes: 0,
        nodes: [],
        services: {},
        updated_at: Date.now(),
    })

    constructor(options: WebsocketDiscoveryServerOptions = {}) {
        this.#server = new WsServer({
            host: options.host,
            path: options.path,
            port: options.port ?? 8788,
        })

        this.#subscription = fromEvent<[WebSocket]>(this.#server, 'connection').pipe(
            mergeMap(([socket]) => this.#handleConnection(socket)),
        ).subscribe()
    }

    get port(): number | null {
        const addr = this.#server.address()
        return typeof addr === 'object' && addr ? addr.port : null
    }

    getMetrics(): DiscoveryMetrics {
        return this.metrics$.value
    }

    close() {
        this.#subscription.unsubscribe()
        this.#server.close()
    }

    #handleConnection(socket: WebSocket) {
        queueMicrotask(() => this.#syncExistingNodes(socket))

        return merge(
            fromEvent(socket, 'message').pipe(
                tap(event => {
                    const raw = Array.isArray(event)
                        ? event[0]
                        : (event && typeof event === 'object' && 'data' in (event as object)
                            ? (event as { data: unknown }).data
                            : event)
                    this.#handleMessage(socket, raw)
                }),
                ignoreElements(),
            ),
            fromEvent(socket, 'close').pipe(
                take(1),
                tap(() => this.#handleClose(socket)),
                ignoreElements(),
            ),
        )
    }

    #handleMessage(socket: WebSocket, raw: unknown) {
        const frame = decodeRelayFrame(normalizeRelayRawData(raw as any))
        if (!frame || frame.type !== 'hello') return

        const node = frame.me
        if (!node?.node_id) return

        const now = Date.now()
        const isNew = !this.#metadata.has(node.node_id)

        ;(socket as TrackedSocket)[socketNodeId] = node.node_id
        this.#sockets.set(node.node_id, socket)
        this.#metadata.set(node.node_id, node)
        this.#lastSeen.set(node.node_id, now)
        if (isNew) this.#onlineAt.set(node.node_id, now)

        this.#broadcastExcept({ type: 'hello', me: node }, socket)
        this.#refreshMetrics()
    }

    #handleClose(socket: WebSocket) {
        const nodeId = (socket as TrackedSocket)[socketNodeId]
        if (!nodeId) return

        this.#sockets.delete(nodeId)
        this.#metadata.delete(nodeId)
        this.#onlineAt.delete(nodeId)
        this.#lastSeen.delete(nodeId)

        this.#broadcastExcept({ type: 'offline', node_id: nodeId }, socket)
        this.#refreshMetrics()
    }

    #syncExistingNodes(socket: WebSocket) {
        for (const node of this.#metadata.values()) {
            if (socket.readyState !== WebSocket.OPEN) return
            socket.send(encodeRelayFrame({ type: 'hello', me: node }), { binary: true })
        }
    }

    #broadcastExcept(frame: RelayFrame, exclude: WebSocket) {
        const message = encodeRelayFrame(frame)
        for (const client of this.#server.clients) {
            if (client === exclude || client.readyState !== WebSocket.OPEN) continue
            client.send(message, { binary: true })
        }
    }

    #refreshMetrics() {
        const nodes: NodeMetrics[] = []
        const services: Record<string, string[]> = {}
        const now = Date.now()

        for (const [nodeId, node] of this.#metadata) {
            nodes.push({
                node_id: nodeId,
                host: node.host,
                namespace: node.namespace,
                services: Object.keys(node.services || {}),
                version: node.version,
                online_since: this.#onlineAt.get(nodeId) ?? now,
                last_seen: this.#lastSeen.get(nodeId) ?? now,
            })

            for (const service of Object.keys(node.services || {})) {
                const list = services[service] ??= []
                list.push(nodeId)
            }
        }

        this.metrics$.next({ total_nodes: nodes.length, nodes, services, updated_at: now })
    }
}
