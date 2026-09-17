import WebSocket from 'ws'
import {
    BehaviorSubject,
    defer,
    distinctUntilChanged,
    finalize,
    fromEvent,
    ignoreElements,
    map,
    merge,
    Observable,
    retry,
    Subject,
    Subscription,
    switchMap,
    take,
    tap,
    throwError,
    timer,
} from 'rxjs'
import type {
    NodeRef,
    SpiderMeshNode,
    TopologyDiscoveryContext,
} from '@spider-mesh/core'
import type { DiscoveryEvent, DiscoveryMessage, DiscoveryTransporter } from '@spider-mesh/discovery'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayRawData } from './websocketProtocol.js'

/** Cấu hình reconnect và heartbeat cho discovery-only WebSocket client. */
export type WebsocketDiscoveryClientOptions = {
    reconnectIntervalMs?: number
    heartbeatIntervalMs?: number
}

const WS_OPEN = 1
const WS_CONNECTING = 0

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

/** Discovery-only client dùng relay để đồng bộ node vào Topology. */
export class WebsocketDiscoveryClient
    extends Subject<DiscoveryEvent<SpiderMeshNode>>
    implements DiscoveryTransporter<SpiderMeshNode> {

    #nodes = new Map<string, SpiderMeshNode>()
    #nodes$ = new BehaviorSubject<SpiderMeshNode[]>([])
    #localNode?: SpiderMeshNode
    #socket?: WebSocket
    #connection = Subscription.EMPTY
    #topologyBinding = Subscription.EMPTY
    #topologyContext?: TopologyDiscoveryContext

    constructor(
        private readonly url: string,
        private readonly options: WebsocketDiscoveryClientOptions = {},
    ) {
        super()
        this.#startConnectionLoop()
    }

    /** Gắn discovery client vào Topology theo contract hai chiều. */
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

    /** Đóng discovery client và dừng reconnect loop. */
    close() {
        this.unsubscribe()
    }

    override unsubscribe() {
        this.#connection.unsubscribe()
        this.#topologyBinding.unsubscribe()
        super.unsubscribe()
    }

    async broadcast(message: DiscoveryMessage<SpiderMeshNode>) {
        const node = { ...message.data }
        this.#localNode = node
        if (this.#socket?.readyState === WS_OPEN) {
            this.#socket.send(encodeRelayFrame({ type: 'hello', me: node }), { binary: true })
        }
    }

    /** Directory của relay chỉ authoritative khi WebSocket đang kết nối. */
    async verify(node_id: string) {
        if (this.#socket?.readyState !== WS_OPEN) return 'unknown' as const
        return this.#nodes.has(node_id) ? 'alive' as const : 'dead' as const
    }

    watchService(service: string): Observable<NodeRef[]> {
        return this.#nodes$.pipe(
            map(nodes => nodes.filter(n => service in (n.services || {}))),
            distinctUntilChanged((prev, curr) => {
                if (prev.length !== curr.length) return false
                const prevIds = new Set(prev.map(n => n.node_id))
                return curr.every(n => prevIds.has(n.node_id))
            }),
        )
    }

    listNodes(service: string): NodeRef[] {
        return [...this.#nodes.values()].filter(n => service in (n.services || {}))
    }

    #startConnectionLoop() {
        this.#connection = defer(() => {
            const socket = new WebSocket(this.url)
            socket.binaryType = 'arraybuffer'

            const open$ = fromEvent(socket, 'open').pipe(take(1))
            const close$ = fromEvent(socket, 'close').pipe(
                take(1),
                tap(() => this.#clearNodes()),
                switchMap(() => throwError(() => new Error(`WebsocketDiscoveryClient disconnected: ${this.url}`))),
            )
            const error$ = fromEvent(socket, 'error').pipe(
                take(1),
                switchMap(() => throwError(() => new Error(`WebsocketDiscoveryClient error: ${this.url}`))),
            )

            return open$.pipe(
                tap(() => {
                    this.#socket = socket
                    if (this.#localNode) {
                        socket.send(encodeRelayFrame({ type: 'hello', me: this.#localNode }), { binary: true })
                    }
                }),
                switchMap(() => merge(
                    fromEvent<RelayRawData | [RelayRawData, boolean] | { data: RelayRawData }>(socket as any, 'message').pipe(
                        tap(event => {
                            const raw = Array.isArray(event)
                                ? event[0]
                                : (event && typeof event === 'object' && 'data' in (event as object)
                                    ? (event as { data: RelayRawData }).data
                                    : event as RelayRawData)
                            this.#handleFrame(raw)
                        }),
                        ignoreElements(),
                    ),
                    timer(0, this.options.heartbeatIntervalMs ?? 30000).pipe(
                        tap(() => { if (socket.readyState === WS_OPEN) (socket as any).ping?.() }),
                        ignoreElements(),
                    ),
                    close$,
                    error$,
                )),
                finalize(() => {
                    this.#socket = undefined
                    if (socket.readyState === WS_OPEN) socket.close()
                    else if (socket.readyState === WS_CONNECTING) socket.terminate()
                }),
            )
        }).pipe(
            retry({ delay: () => timer(this.options.reconnectIntervalMs ?? 1000) }),
        ).subscribe()
    }

    #handleFrame(raw: RelayRawData) {
        const frame = decodeRelayFrame(normalizeRelayRawData(raw))
        if (!frame) return

        if (frame.type === 'hello') {
            const node = frame.me
            if (!node?.node_id || node.node_id === this.#localNode?.node_id) return
            this.#nodes.set(node.node_id, node)
            this.#publishNodes()
            this.#topologyContext?.upsertRemote(node)
            this.next(toDiscoveryEvent(node))
            return
        }

        if (frame.type === 'offline') {
            if (!this.#nodes.has(frame.node_id)) return
            this.#nodes.delete(frame.node_id)
            this.#publishNodes()
            this.#topologyContext?.removeRemote(frame.node_id)
        }
    }

    #clearNodes() {
        if (this.#nodes.size === 0) return
        // Mất kết nối tới relay chỉ có nghĩa trạng thái hiện tại chưa biết, không chứng minh
        // từng node đã chết. Chỉ offline frame mới được quyền xóa membership khỏi Topology.
        this.#nodes.clear()
        this.#publishNodes()
    }

    #publishNodes() {
        this.#nodes$.next([...this.#nodes.values()])
    }
}
