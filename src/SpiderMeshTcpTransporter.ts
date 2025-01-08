import { randomUUID } from "crypto";
import { SpiderMeshRpcTransporter, SpiderMeshPubsubTransporter, SpiderMesh, PublishOptions, RpcOptions, SpiderMeshTransporterInitOptions } from "@spider-mesh/core";
import { BehaviorSubject, Observable, Subject, Subscriber, debounceTime, filter, lastValueFrom, map, merge, mergeAll, mergeMap, tap } from 'rxjs'
import { RxjsTcpSocket } from "./RxjsTcpSocket.js";
import { RxjsTcpServer } from "./RxjsTcpServer.js";
import { RxjsUdpServer } from "./RxjsUdpServer.js";
import { UDP_BROADCAST_PORT, NAMEPSACE, UDP_SECRET_KEY } from "./const.js"
import { Encoder } from "./Encoder.js";


export class ServiceNotFound extends Error { }
export class MissingPubsubTransporter extends Error { }
export class ServiceOffline extends Error { }


type TcpNode = {
    node_id: string
    host: string
    port: number
    listening: string[]
    services: string[]
}


type HelloMessage = TcpNode & { back?: boolean }
type NodeID = string
type EventID = string
type RpcRequest = RpcOptions & { id: string, r: string }
type RpcResponse = {
    id: string,
    end?: true
    data?: any
    error?: any
}

export class SpiderMeshTcpTransporter implements SpiderMeshRpcTransporter, SpiderMeshPubsubTransporter {

    $requests = new Subject<RpcOptions & { reply: (o: Observable<any> | Promise<any>) => void }>()
    $nodes = new Subject<{ node_id: string, status: "online" | "offline"; }>()

    #$tcpServer: RxjsTcpServer
    #$udpServer: RxjsUdpServer

    #remoteNodes = new Map<NodeID, { metadata: TcpNode, socket: RxjsTcpSocket }>
    #remoteListeners = new Map<EventID, Set<NodeID>>()

    #localListeners = new BehaviorSubject(new Map<EventID, Set<Subscriber<any>>>())

    #$requests = new Map<string, Subscriber<any>>()

    constructor(private sm: SpiderMesh) {


        this.#$tcpServer = new RxjsTcpServer()

        this.#$udpServer = new RxjsUdpServer({
            namespace: NAMEPSACE,
            port: UDP_BROADCAST_PORT,
            node_id: sm.node_id,
            key: UDP_SECRET_KEY
        })

        const $tcpSockets = this.#$tcpServer
        const $udpSockets = this.#$udpServer.pipe(
            mergeMap(options => RxjsTcpSocket.connect(options)),
            filter(Boolean)
        )


        lastValueFrom(merge($tcpSockets, $udpSockets).pipe(
            mergeMap(socket => this.#accept(socket))
        ))

        this.#$tcpServer.$online.subscribe(status => {
            if (!status) return
            this.#$udpServer.broadcast(status)
        })

        lastValueFrom(merge(this.#localListeners, sm.getLocalServices()).pipe(
            debounceTime(2000),
            map(() => [...this.#remoteNodes.values()]),
            mergeAll(),
            tap(({ socket }) => this.#hello(socket))
        ), { defaultValue: [] })

        sm.linkRpcTransporter(this)
        sm.linkPubsubTransporter(this)
    }


    #accept(socket: RxjsTcpSocket) {
        !socket.fromRemote && this.#hello(socket, true)
        return socket.pipe(
            map(buf => {
                try {
                    return Encoder.decode<PublishOptions<any>>(buf)
                } catch (e) { }
            }),
            filter(Boolean),
            mergeMap(async msg => {
                if (msg.event == `#hello`) {
                    const metadata = msg.data as HelloMessage
                    await this.#link(socket, metadata)
                    metadata.back && this.#hello(socket)
                    return
                }
                this.#handle(msg)
            })
        )
    }

    #handle(msg: PublishOptions<any>) {

        if (msg.event.startsWith('#rpc:')) {
            const options = msg.data as RpcRequest
            this.$requests.next({
                ...options,
                reply: async response => {

                    // Reply to remote server
                    if (response instanceof Promise) {
                        // Send #response
                        try {
                            const data: RpcResponse = {
                                data: await response,
                                id: options.id
                            }
                            this.publish({ event: options.r, data })
                        } catch (error) {
                            const data: RpcResponse = {
                                error,
                                id: options.id
                            }
                            this.publish({ event: options.r, data })
                        }
                    }

                    if (response instanceof Observable) {
                        response.subscribe({
                            complete: () => this.publish({ event: options.r, data: { id: options.id, end: true } }),
                            error: error => this.publish({ event: options.r, data: { id: options.id, error } }),
                            next: data => this.publish({ event: options.r, data: { id: options.id, data } }),
                        })
                    }
                }
            })
            return
        }

        if (msg.event == '#response') {
            const { data, error, id, end } = msg.data as RpcResponse
            const subscriber = this.#$requests.get(id)
            if (subscriber) {
                data != undefined && subscriber.next(data)
                error && subscriber.error(error)
                end && subscriber.complete()
            }
            return
        }

        const listeners = this.#localListeners.getValue()
        listeners.get(msg.event)?.forEach(o => o.next(msg));
    }

    async #link($: RxjsTcpSocket, metadata: HelloMessage) {

        const linked = this.#remoteNodes.get(metadata.node_id)
        metadata.host = metadata.host || $.remoteAddress || ''
        if (!metadata.host) return

        const socket = linked ? linked.socket : ($.fromRemote ? await RxjsTcpSocket.connect({
            host: metadata.host,
            port: metadata.port,
            keepAlive: true
        }) || $ : $)

        if (!socket) return

        this.#remoteNodes.set(metadata.node_id, {
            metadata,
            socket,
        })
        for (const event of metadata.listening) {
            const set = this.#remoteListeners.get(event) || new Set()
            set.add(metadata.node_id)
            this.#remoteListeners.set(event, set)
        }
        if (linked) return
        console.log({ new_node: metadata.node_id })
        this.$nodes.next({ node_id: metadata.node_id, status: 'online' })

        socket.subscribe({
            error(err) {
                console.error(err)
            },
            complete: () => {
                for (const event of metadata.listening) {
                    const set = this.#remoteListeners.get(event) || new Set()
                    set.delete(metadata.node_id)
                    this.#remoteListeners.set(event, set)
                }
                this.$nodes.next({ node_id: metadata.node_id, status: 'offline' })
                console.log(`Node ${metadata.node_id} oflfine`)
            }
        })

    }

    async #hello(socket: RxjsTcpSocket, back: boolean = false) {

        const port = this.#$tcpServer.port
        if (!port) return

        console.log(`Say hello to ${socket.remoteAddress || '?'} by ${socket.fromRemote ? 'incoming socket' : 'outgoing package'}`)

        const services = [...Object.keys(this.sm.getLocalServices().getValue())]

        const listening = [
            '#hello',
            this.sm.node_id,
            '__metadata__',
            ...services,
            ...this.#localListeners.getValue().keys()
        ]

        const msg: PublishOptions<HelloMessage> = {
            event: '#hello',
            data: {
                node_id: this.sm.node_id,
                host: '',
                port,
                listening,
                services,
                back
            }
        }
        socket.send(Encoder.encode(msg))
    }

    listen<T>(topic: string): Observable<T> {
        const map = this.#localListeners.getValue()
        const listeners = map.get(topic) || new Set()
        !map.has(topic) && map.set(topic, listeners)
        this.#localListeners.next(map)

        return new Observable<T>(o => {
            listeners.add(o)
            return () => {
                listeners.delete(o)
            }
        })
    }

    async publish<T>(options: PublishOptions<T>) {
        const buffer = Encoder.encode(options as any)
        for (const node_id of this.#remoteListeners.get(options.event) || []) {
            const node = this.#remoteNodes.get(node_id)
            node?.socket?.send(buffer)
        }
    }

    #indexes = new Map<string, number>()

    #getRpcNode(options: RpcOptions) {
        if (options.node_id) {
            const node = this.#remoteNodes.get(options.node_id)
            if (!node) throw new ServiceOffline('NODE_OFFLINE')
            return node
        }
        const key = `${options.service}.${options.method}`
        const nodes = this.#remoteListeners.get(options.service) || new Set()
        for (let i = 1; i <= nodes.size; i++) {
            const index = (this.#indexes.get(key) || 0 + i) % (nodes.size)
            const node_id = [...nodes][index]
            const node = this.#remoteNodes.get(node_id)
            if (!node) continue
            this.#indexes.set(key, index)
        }
        throw new ServiceOffline('NODE_OFFLINE')
    }


    rpc<T>(options: RpcOptions): Observable<T> {

        const node = this.#getRpcNode(options)

        const request: RpcRequest = {
            id: randomUUID(),
            ...options,
            r: this.sm.node_id
        }

        const data = Encoder.encode(request)

        return new Observable<T>(o => {
            this.#$requests.set(request.id, o)
            node.socket.send(data)
        })
    }


} 