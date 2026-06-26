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
    switchMap,
    take,
    tap,
    throwError,
    timer,
} from 'rxjs'
import type {
    DiscoveryEvent,
    DiscoveryTransporter,
    MdnsMessage,
    NodeMetadata,
    NodeRef,
    ServiceDirectory,
    SpiderMeshNode,
} from '@spider-mesh/core'
import { decodeRelayFrame, encodeRelayFrame, normalizeRelayRawData, type RelayRawData } from './websocketProtocol.js'

export type WebsocketDiscoveryClientOptions = {
    reconnectIntervalMs?: number
    heartbeatIntervalMs?: number
}

const WS_OPEN = 1
const WS_CONNECTING = 0

export class WebsocketDiscoveryClient
    extends Subject<DiscoveryEvent>
    implements DiscoveryTransporter, ServiceDirectory {

    #nodes = new Map<string, SpiderMeshNode>()
    #nodes$ = new BehaviorSubject<SpiderMeshNode[]>([])
    #localNode?: SpiderMeshNode
    #socket?: WebSocket

    constructor(
        private readonly url: string,
        private readonly options: WebsocketDiscoveryClientOptions = {},
    ) {
        super()
        this.#startConnectionLoop()
    }

    async broadcast(data: MdnsMessage<NodeMetadata>) {
        const node = { ...data.node } as unknown as SpiderMeshNode
        this.#localNode = node
        if (this.#socket?.readyState === WS_OPEN) {
            this.#socket.send(encodeRelayFrame({ type: 'hello', me: node }), { binary: true })
        }
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
        defer(() => {
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
            this.next({ discovered: node })
            return
        }

        if (frame.type === 'offline') {
            if (!this.#nodes.has(frame.node_id)) return
            this.#nodes.delete(frame.node_id)
            this.#publishNodes()
        }
    }

    #clearNodes() {
        if (this.#nodes.size === 0) return
        this.#nodes.clear()
        this.#publishNodes()
    }

    #publishNodes() {
        this.#nodes$.next([...this.#nodes.values()])
    }
}
