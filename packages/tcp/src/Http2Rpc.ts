import { firstValueFrom, fromEvent, reduce, Subject, Subscription, takeUntil } from "rxjs"
import { connect, createServer, type ClientHttp2Session, type ClientHttp2Stream, type IncomingHttpHeaders, type ServerHttp2Session, type ServerHttp2Stream } from 'node:http2'
import { SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS, SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS, SPIDERMESH_HTTP2_RECONNECT_DELAY_MS, SPIDERMESH_HTTP2_RECONNECT_MAX_DELAY_MS } from "./const.js"
import { AddressInfo } from "node:net"
import { unpack, pack } from 'msgpackr'
import { Topology } from '@spider-mesh/core'
import type { RpcCancelPacket, RpcEvent, RpcProbeRequest, RpcProbeResult, RpcRequestPacket, RpcResponsePacket, RpcTransporter, RpcTransporterContext, SpiderMeshError, SpiderMeshNode, TopologyRoute } from '@spider-mesh/core'

/** Vòng thử kết nối tới một node; `wake()` cắt ngang lần chờ hiện tại để thử ngay. */
type ReconnectLoop = {
    task: Promise<void>
    /** Địa chỉ (host + metadata cổng) của lần thử gần nhất. */
    target: string
    wake: () => void
}

/** Địa chỉ HTTP/2 đã được resolver từ logical service name. */
export type Http2RpcTarget = {
    host: string
    port: number
}

/** Cấu hình network của HTTP/2 transporter, không chứa dependency môi trường. */
export type Http2RpcOptions = {
    port?: number
    resolveService?: (service: string) => Http2RpcTarget | undefined
}

/** Transporter RPC qua HTTP/2, dùng Topology nếu SpiderMesh được cấu hình Topology. */
export class Http2Rpc extends Subject<RpcEvent> implements RpcTransporter {

    public readonly name = 'http2'

    #connections = new Map<string, ClientHttp2Session>()
    #connecting = new Map<string, Promise<ClientHttp2Session>>()
    #reconnecting = new Map<string, ReconnectLoop>()
    #pendingConnections = new Set<ClientHttp2Session>()
    #responseStreams = new Map<string, ServerHttp2Stream>()
    #metadata: RpcEvent['endpoints'] | null = null
    #server = createServer({})
    #serverSessions = new Set<ServerHttp2Session>()
    #isDisposing = false
    #topology?: Topology
    readonly #options: Http2RpcOptions
    #serverStreamSubscription: Subscription
    #topologySubscription = Subscription.EMPTY

    /**
     * Code mới truyền `Http2RpcOptions`; overload Topology chỉ giữ cho standalone fixture cũ.
     * Trong ứng dụng, SpiderMesh sẽ inject Topology qua `start()`.
     */
    constructor(options: Http2RpcOptions | Topology = {}) {
        super()
        this.#options = options instanceof Topology ? {} : options
        if (options instanceof Topology) this.#bindTopology(options)

        this.#server.on('session', session => {
            this.#serverSessions.add(session)
            session.once('close', () => this.#serverSessions.delete(session))
        })

        this.#serverStreamSubscription = fromEvent(this.#server, 'stream').subscribe(args => {
            const [stream] = args as [ServerHttp2Stream]
            void this.#handleStream(stream)
        })

        this.#server.listen({
            port: this.#options.port ?? 0,
            isIPv4: true,
            isIPv6: true
        }, () => {
            const { port } = this.#server.address() as AddressInfo
            this.#metadata = { port }
            this.next({ endpoints: this.#metadata })
        })
    }

    get metadata() {
        return this.#metadata
    }

    /** Core truyền Topology optional vào transporter khi đăng ký. */
    start(context: RpcTransporterContext) {
        if (context.topology && this.#topology !== context.topology) this.#bindTopology(context.topology)
    }

    /** Dừng toàn bộ server, stream và connection do transporter sở hữu. */
    stop() {
        if (!this.closed) this.unsubscribe()
    }

    override unsubscribe() {
        this.#isDisposing = true
        for (const stream of this.#responseStreams.values()) {
            stream.close()
        }
        this.#responseStreams.clear()
        this.#topologySubscription.unsubscribe()
        for (const connection of this.#pendingConnections) connection.destroy()
        this.#pendingConnections.clear()
        for (const connection of this.#connections.values()) {
            connection.destroy()
        }
        this.#connections.clear()
        this.#connecting.clear()
        // Đánh thức các vòng đang chờ để chúng thấy transporter đã đóng và thoát ngay.
        for (const loop of this.#reconnecting.values()) loop.wake()
        this.#reconnecting.clear()
        this.#serverStreamSubscription.unsubscribe()
        for (const session of this.#serverSessions) {
            session.destroy()
        }
        this.#serverSessions.clear()
        this.#server.close()
        super.unsubscribe()
    }

    #bindTopology(topology: Topology) {
        this.#topologySubscription.unsubscribe()
        this.#topology = topology
        this.#topologySubscription = topology.nodes$.subscribe(nodes => {
            for (const node of nodes.values()) {
                if (!this.#hasRpcEndpoint(node)) continue
                this.#ensureConnection(node)
            }
        })
    }

    async send(packet: RpcRequestPacket | RpcCancelPacket | RpcResponsePacket): Promise<{ cancel: () => void }> {
        if (packet.kind === 'response') {
            const stream = this.#responseStreams.get(packet.request_id)
            if (stream) {
                await this.#writeResponsePacket(stream, packet)
                if (packet.completed || packet.error != undefined) {
                    this.#responseStreams.delete(packet.request_id)
                }
                return { cancel: () => {} }
            }
            return { cancel: () => {} }
        }

        const route = packet.kind === 'request'
            ? this.#resolveRequestRoute(packet)
            : this.#resolveExactRoute(packet.destination_node_id)
        const node = route?.node
        if (!node) {
            throw {
                code: 'MICROSERVICE_OFFLINE',
                message: packet.kind === 'request'
                    ? `No HTTP/2 target for service ${packet.service}`
                    : `Node ${packet.destination_node_id || '?'} is not available`,
            } satisfies SpiderMeshError
        }
        const resolvedNodeId = node.node_id

        const connection = await this.#connect(node)
        const request = connection.request({
            ':method': 'POST',
            ':path': '/rpc',
            'content-type': 'application/octet-stream',
        })

        if (packet.kind === 'request') {
            this.#bindResponseStream(packet, request)
        }

        const startedAt = Date.now()
        try {
            await new Promise<void>((resolve, reject) => {
            let settled = false

            request.on('response', (headers: IncomingHttpHeaders) => {
                const status = Number(headers[':status'] || 0)
                if (status >= 400 && !settled) {
                    settled = true
                    reject(new Error(`HTTP2 RPC request failed with status ${status}`))
                    return
                }

                if (!settled) {
                    settled = true
                    resolve()
                }
            })
            request.on('data', () => undefined)
            request.once('end', () => {
                if (!settled && packet.kind !== 'request') {
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
        } catch (error) {
            if (route) {
                this.#topology?.report(route, {
                    status: 'failure',
                    latency: Date.now() - startedAt,
                })
            }
            throw error
        }

        if (route) {
            this.#topology?.report(route, {
                status: 'success',
                latency: Date.now() - startedAt,
            })
        }

        if (packet.kind === 'request') {
            return {
                cancel: () => {
                    void this.send({
                        kind: 'cancel',
                        request_id: packet.request_id,
                        destination_node_id: resolvedNodeId,
                    }).catch(() => undefined)
                }
            }
        }

        return { cancel: () => {} }
    }

    async #connect(node: SpiderMeshNode): Promise<ClientHttp2Session> {
        const current_connection = this.#connections.get(node.node_id)
        if (current_connection) {
            if (!current_connection.destroyed && !current_connection.closed) {
                return current_connection
            }
            this.#connections.delete(node.node_id)
            current_connection.destroy()
        }

        const connecting = this.#connecting.get(node.node_id)
        if (connecting) return connecting

        const pending = this.#openConnection(node).finally(() => {
            if (this.#connecting.get(node.node_id) === pending) {
                this.#connecting.delete(node.node_id)
            }
        })
        this.#connecting.set(node.node_id, pending)
        return pending
    }

    async #openConnection(node: SpiderMeshNode): Promise<ClientHttp2Session> {
        const metadata = this.#getTransporterMetadata(node)
        const port = Number(metadata?.port)

        if (!port) {
            throw {
                code: 'MICROSERVICE_OFFLINE',
                message: `Http2Rpc metadata missing for node ${node.node_id}`
            } satisfies SpiderMeshError
        }

        const url = `http://${node.host.includes(':') ? `[${node.host}]` : node.host}:${port}`
        const connection = connect(url)
        this.#pendingConnections.add(connection)
        const connected = connection.connecting ? await new Promise<boolean>(resolve => {
            let settled = false
            const finish = (value: boolean) => {
                if (settled) return
                settled = true
                clearTimeout(timer)
                connection.off('connect', onConnect)
                connection.off('error', onFailure)
                connection.off('close', onFailure)
                resolve(value)
            }
            const onConnect = () => finish(true)
            const onFailure = () => finish(false)
            const timer = setTimeout(() => {
                connection.destroy()
                finish(false)
            }, Math.max(100, SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS))
            timer.unref?.()
            connection.once('connect', onConnect)
            connection.once('error', onFailure)
            connection.once('close', onFailure)
        }) : true
        this.#pendingConnections.delete(connection)
        if (connected) {
            this.#reportReachability(node.node_id, 'reachable')
            let lost = false
            const handleLoss = () => {
                if (lost) return
                lost = true
                this.#debug('connection closed', { node_id: node.node_id })
                this.#handleConnectionLoss(node.node_id, connection)
            }
            const handleError = () => {
                if (!connection.destroyed) connection.destroy()
                handleLoss()
            }
            connection.once('close', handleLoss)
            connection.once('error', handleError)
            connection.socket.once('close', handleLoss)
            connection.socket.once('error', handleError)
            this.#connections.set(node.node_id, connection)
            this.#debug('connection ready', { node_id: node.node_id, endpoint: url })
            return connection
        }
        connection.destroy()

        const e: SpiderMeshError = {
            code: 'MICROSERVICE_OFFLINE',
            message: `Connection attempt to node ${node.node_id} failed`,
        }
        throw e
    }

    #ensureConnection(node: SpiderMeshNode) {
        if (this.#isDisposing || this.closed) return
        const connection = this.#connections.get(node.node_id)
        if (connection && !connection.destroyed && !connection.closed) {
            this.#reportReachability(node.node_id, 'reachable')
            return
        }
        const running = this.#reconnecting.get(node.node_id)
        if (running) {
            // Discovery báo địa chỉ/cổng mới (ví dụ process khởi động lại): thử ngay, không đợi hết
            // thời gian chờ đã tăng dần. Thay đổi khác trong Topology không đánh thức vòng thử.
            if (running.target !== this.#targetKey(node)) running.wake()
            return
        }
        // Lần kết nối đầu tiên chưa phải bằng chứng endpoint có vấn đề. Giữ state mặc định
        // reachable để request đầu tiên vẫn có thể chọn node rồi await chính connection này.
        // Chỉ connection đã từng hoạt động nhưng bị mất mới chuyển sang `suspect`.
        const loop: ReconnectLoop = { task: Promise.resolve(), target: this.#targetKey(node), wake: () => {} }
        this.#reconnecting.set(node.node_id, loop)
        loop.task = this.#connectWithRetry(node, loop).finally(() => {
            if (this.#reconnecting.get(node.node_id) === loop) this.#reconnecting.delete(node.node_id)
        })
    }

    /**
     * Kết nối tới node cho tới khi thành công hoặc node rời Topology. Kết nối HTTP/2 là bằng chứng
     * node còn sống: không bỏ cuộc sau một số lần thất bại, vì discovery (như UDP) không phát lại để
     * "gõ cửa" lần nữa. Thất bại đủ `SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS` lần thì báo `unreachable`
     * để node bị loại khỏi việc chọn đích; việc xoá hẳn node do Topology quyết định
     * (`removeUnreachableAfterMs`).
     */
    async #connectWithRetry(node: SpiderMeshNode, loop: ReconnectLoop) {
        const unreachableAfter = Math.max(1, SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS)
        const base = Math.max(10, SPIDERMESH_HTTP2_RECONNECT_DELAY_MS)
        const maxDelay = Math.max(base, SPIDERMESH_HTTP2_RECONNECT_MAX_DELAY_MS)

        for (let attempt = 1; ; attempt++) {
            if (this.#isDisposing || this.closed) return
            // Luôn lấy snapshot mới nhất: node có thể đổi địa chỉ/cổng giữa hai lần thử.
            const current = this.#topology?.getPeer(node.node_id)
            if (!current) return
            const target = this.#targetKey(current)
            if (target !== loop.target) {
                // Địa chỉ mới: tính lại từ đầu, không mang theo thời gian chờ của địa chỉ cũ.
                loop.target = target
                attempt = 1
            }

            try {
                await this.#connect(current)
                return
            } catch {
                this.#debug('reconnect failed', { node_id: node.node_id, attempt })
            }

            if (attempt === unreachableAfter) {
                this.#reportReachability(node.node_id, 'unreachable', 'reconnect-failed')
                this.#debug('peer unreachable, still retrying', { node_id: node.node_id, attempts: attempt })
            }

            const backoff = Math.min(maxDelay, base * (2 ** Math.min(attempt - 1, 16)))
            const jitter = Math.floor(Math.random() * Math.max(1, backoff * 0.2))
            await new Promise<void>(resolve => {
                const timer = setTimeout(resolve, backoff + jitter)
                timer.unref?.()
                loop.wake = () => {
                    clearTimeout(timer)
                    resolve()
                }
            })
            loop.wake = () => {}
        }
    }

    #handleConnectionLoss(nodeId: string, connection: ClientHttp2Session) {
        if (this.#connections.get(nodeId) === connection) this.#connections.delete(nodeId)
        this.#reportReachability(nodeId, 'suspect', 'connection-lost')
        const node = this.#topology?.getPeer(nodeId)
        if (!this.#isDisposing && !this.closed && node) this.#ensureConnection(node)
    }

    /** Gửi trạng thái endpoint vào Topology; transporter không được tự xóa membership. */
    /** Địa chỉ kết nối của node; đổi khi process khởi động lại với host/cổng mới. */
    #targetKey(node: SpiderMeshNode) {
        return `${node.host}:${JSON.stringify(this.#getTransporterMetadata(node) ?? null)}`
    }

    #reportReachability(node_id: string, status: 'reachable' | 'suspect' | 'unreachable', reason?: string) {
        this.#topology?.reportReachability({
            node_id,
            transporter: this.name,
            status,
            reason,
        })
    }

    #debug(message: string, data: Record<string, unknown>) {
        if (process.env.SPIDERMESH_TCP_DEBUG) {
            console.error(`[spider-mesh/tcp] ${message}`, data)
        }
    }

    async #handleStream(stream: ServerHttp2Stream) {
        try {
            const end$ = fromEvent(stream, 'end')
            const buffers = await firstValueFrom(
                fromEvent(stream, 'data').pipe(
                    takeUntil(end$),
                    reduce((chunks, chunk) => {
                        chunks.push(chunk as Buffer)
                        return chunks
                    }, [] as Buffer[])
                )
            )

            const packet = unpack(Buffer.concat(buffers)) as RpcRequestPacket | RpcResponsePacket | RpcCancelPacket

            if (packet.kind === 'request') {
                stream.respond({
                    ':status': 200,
                    'content-type': 'application/octet-stream'
                })
                this.#responseStreams.set(packet.request_id, stream)
                stream.once('close', () => {
                    this.#responseStreams.delete(packet.request_id)
                })
            } else {
                stream.respond({ ':status': 204 })
                stream.end()
            }

            this.next({ rpc: packet })
        } catch {
            if (!stream.headersSent) {
                stream.respond({ ':status': 400 })
            }
            stream.end()
        }
    }

    async #writeResponsePacket(stream: ServerHttp2Stream, packet: RpcResponsePacket) {
        const frame = this.#encodeFrame(packet)
        const isTerminal = packet.kind === 'response' && (packet.completed || packet.error != undefined)

        await new Promise<void>((resolve, reject) => {
            const done = (error?: (Error & { code?: string }) | null) => {
                if (error) {
                    // Node có thể gọi callback của end() sau khi peer đã nhận terminal
                    // frame và đóng stream. Frame đã tới caller nên đây không phải lỗi RPC.
                    if (isTerminal && error.code === 'ERR_STREAM_DESTROYED') {
                        resolve()
                        return
                    }
                    reject(error)
                    return
                }
                resolve()
            }

            if (isTerminal) {
                stream.end(frame, done)
                return
            }

            stream.write(frame, done)
        })
    }

    #bindResponseStream(packet: RpcRequestPacket, request: ClientHttp2Stream) {
        let buffer = Buffer.alloc(0)
        let completed = false
        let accepted = false

        const emitResponse = (response: RpcResponsePacket) => {
            if (this.#isDisposing || this.closed) {
                return
            }

            this.next({ rpc: response })
        }

        request.on('response', (headers: IncomingHttpHeaders) => {
            const status = Number(headers[':status'] || 0)
            accepted = status > 0 && status < 400
        })

        request.on('data', chunk => {
            buffer = Buffer.concat([buffer, chunk as Buffer])

            while (buffer.length >= 4) {
                const size = buffer.readUInt32BE(0)
                if (buffer.length < size + 4) {
                    return
                }

                const frame = buffer.subarray(4, size + 4)
                buffer = buffer.subarray(size + 4)

                const response = unpack(frame) as RpcResponsePacket
                if (response.kind !== 'response') {
                    continue
                }

                if (response.completed || response.error != undefined) {
                    completed = true
                }

                emitResponse(response)
            }
        })

        request.once('end', () => {
            if (!accepted || completed) {
                return
            }

            emitResponse({
                kind: 'response',
                request_id: packet.request_id,
                error: {
                    code: 'MICROSERVICE_OFFLINE',
                    message: 'RPC response stream ended unexpectedly'
                },
                completed: true
            })
        })

        request.once('error', error => {
            if (!accepted || completed) {
                return
            }

            completed = true
            emitResponse({
                kind: 'response',
                request_id: packet.request_id,
                error: {
                    code: 'MICROSERVICE_OFFLINE',
                    message: error.message || 'RPC response stream failed'
                },
                completed: true
            })
        })
    }

    #encodeFrame(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
        const payload = pack(packet)
        const frame = Buffer.allocUnsafe(payload.length + 4)
        frame.writeUInt32BE(payload.length, 0)
        payload.copy(frame, 4)
        return frame
    }

    #getTransporterMetadata(node?: SpiderMeshNode | null) {
        if (!node) return undefined
        return node.transporters?.[this.name]
    }

    /** Khóa endpoint giúp discovery snapshot có host/port mới mở lại reconnect cycle. */

    /** Node chỉ route được sau khi đã quảng bá port dưới wire name `http2`. */
    #hasRpcEndpoint(node: SpiderMeshNode): boolean {
        return Number((this.#getTransporterMetadata(node) as { port?: number } | undefined)?.port) > 0
    }

    /** Kiểm tra route đã biết mà không thay đổi routing state. */
    canRoute(service: string, node_id?: string): boolean {
        if (node_id) {
            const node = this.#topology?.getPeer(node_id)
            return !!node
                && !!this.#topology?.isReachable(node_id, this.name)
                && node.services?.[service] != undefined
                && this.#hasRpcEndpoint(node)
        }
        return !!this.#options.resolveService?.(service)
            || !!this.#topology?.list(service).some(node => {
                return this.#topology?.isReachable(node.node_id, this.name) && this.#hasRpcEndpoint(node)
            })
    }

    /** Probe thật qua connection HTTP/2 khi không có Topology để Core dùng cho wait(). */
    async probe(request: RpcProbeRequest): Promise<RpcProbeResult> {
        const startedAt = Date.now()
        const route = request.node_id
            ? this.#resolveExactRoute(request.node_id)
            : this.#resolveRequestRoute({
                kind: 'request',
                request_id: 'probe',
                sender_node_id: 'probe',
                service: request.service,
                method: '__probe__',
                args: [],
            })
        if (!route) return { reachable: false }

        try {
            const connection = await this.#connect(route.node)
            return {
                reachable: !connection.closed && !connection.destroyed,
                node_id: route.node.node_id.startsWith('@service:') ? undefined : route.node.node_id,
                latency: Date.now() - startedAt,
            }
        } catch {
            return { reachable: false, latency: Date.now() - startedAt }
        }
    }

    #resolveRequestRoute(packet: RpcRequestPacket): TopologyRoute | undefined {
        const shouldUseTopology = !!packet.destination_node_id
            || !!packet.routing
            || !this.#options.resolveService

        if (shouldUseTopology && this.#topology) {
            const route = this.#topology.route({
                service: packet.service,
                transporter: this.name,
                node_id: packet.destination_node_id,
                // Direct HTTP/2 không có infrastructure router nên dùng round-robin mặc định.
                routing: packet.routing ?? (this.#options.resolveService ? undefined : { strategy: 'round-robin' }),
            })
            if (route) {
                this.#debug('route selected', {
                    service: packet.service,
                    node_id: route.node.node_id,
                    strategy: packet.routing?.strategy ?? 'round-robin',
                })
                return route
            }
        }

        const target = this.#options.resolveService?.(packet.service)
        return target ? this.#serviceRoute(packet.service, target) : undefined
    }

    #resolveExactRoute(nodeId?: string): TopologyRoute | undefined {
        if (!nodeId || !this.#topology) return undefined
        const node = this.#topology.getPeer(nodeId)
        if (!node || !this.#hasRpcEndpoint(node)) return undefined
        return {
            node,
            endpoint: this.#getTransporterMetadata(node),
        }
    }

    #serviceRoute(service: string, target: Http2RpcTarget): TopologyRoute {
        const node: SpiderMeshNode = {
            node_id: `@service:${service}:${target.host}:${target.port}`,
            namespace: 'infrastructure',
            host: target.host,
            version: 0,
            topics: [],
            services: { [service]: {} },
            nodes: {},
            transporters: {
                [this.name]: { port: target.port },
            },
        }
        return {
            node,
            endpoint: node.transporters[this.name],
        }
    }
}
