import type { IncomingMessage } from 'node:http'
import { WebSocket, WebSocketServer as WsServer } from 'ws'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayFrame } from './websocketProtocol.js'

export type WebsocketRelayServerOptions = {
    port?: number
    host?: string
    path?: string
    heartbeatIntervalMs?: number
    heartbeatTimeoutMs?: number
    isServerConnection?: (socket: WebSocket, request: IncomingMessage) => boolean
}

export class WebsocketRelayServer {
    #server: WsServer
    #nodes = new Map<string, WebSocket>()
    #socketNodes = new WeakMap<WebSocket, Set<string>>()
    #serverConnections = new WeakMap<WebSocket, boolean>()
    #lastSeenAt = new WeakMap<WebSocket, number>()
    #heartbeat: ReturnType<typeof setInterval> | null = null

    constructor(options: WebsocketRelayServerOptions = {}) {
        this.#server = new WsServer({
            host: options.host,
            path: options.path,
            port: options.port || 8787,
        })

        this.#heartbeat = setInterval(() => {
            for (const socket of this.#server.clients) {
                const lastSeenAt = this.#lastSeenAt.get(socket) || 0
                if ((Date.now() - lastSeenAt) > (options.heartbeatTimeoutMs || ((options.heartbeatIntervalMs || 10000) * 2))) {
                    socket.terminate()
                }
            }
        }, options.heartbeatIntervalMs || 10000)

        this.#server.on('connection', (socket, request) => {
            this.#socketNodes.set(socket, new Set())
            this.#serverConnections.set(socket, options.isServerConnection ? options.isServerConnection(socket, request) : true)
            this.#touch(socket)

            socket.on('ping', () => {
                this.#touch(socket)
            })

            socket.on('pong', () => {
                this.#touch(socket)
            })

            socket.on('message', raw => {
                this.#touch(socket)
                const frame = this.#parseFrame(raw)
                if (!frame) return

                if (frame.header.type === 'register') {
                    this.#registerNode(socket, frame.header.node_id)
                    return
                }

                if ('node_id' in frame.header && frame.header.node_id) {
                    if (this.#requiresServerRole(frame) && !this.#isServerConnection(socket)) {
                        return
                    }

                    const target = this.#nodes.get(frame.header.node_id)
                    if (target && target.readyState === WebSocket.OPEN) {
                        target.send(encodeRelayFrame(frame.header, frame.payload), { binary: true })
                    }
                    return
                }

                this.#broadcast(frame, socket)
            })

            socket.on('close', () => {
                const nodeIds = this.#socketNodes.get(socket) || new Set<string>()
                for (const nodeId of nodeIds) {
                    this.#nodes.delete(nodeId)
                    this.#broadcast({
                        header: { type: 'offline', sender_id: nodeId },
                        payload: Buffer.alloc(0)
                    }, socket)
                }
                this.#socketNodes.delete(socket)
                this.#serverConnections.delete(socket)
                this.#lastSeenAt.delete(socket)
            })
        })
    }

    get port() {
        const address = this.#server.address()
        return typeof address === 'object' && address ? address.port : null
    }

    close() {
        if (this.#heartbeat) {
            clearInterval(this.#heartbeat)
            this.#heartbeat = null
        }
        this.#server.close()
    }

    #registerNode(socket: WebSocket, nodeId: string) {
        this.#nodes.set(nodeId, socket)
        const nodeIds = this.#socketNodes.get(socket) || new Set<string>()
        nodeIds.add(nodeId)
        this.#socketNodes.set(socket, nodeIds)
    }

    #touch(socket: WebSocket) {
        this.#lastSeenAt.set(socket, Date.now())
    }

    #isServerConnection(socket: WebSocket) {
        return this.#serverConnections.get(socket) !== false
    }

    #requiresServerRole(frame: RelayFrame) {
        return frame.header.type === 'request' || frame.header.type === 'cancel'
    }

    #broadcast(frame: RelayFrame, sender: WebSocket) {
        const message = encodeRelayFrame(frame.header, frame.payload)
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