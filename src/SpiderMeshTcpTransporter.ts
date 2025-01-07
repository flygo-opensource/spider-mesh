import { randomUUID } from "crypto";
import { SpiderMeshRpcTransporter, SpiderMeshPubsubTransporter, PublishOptions, RpcOptions, SpiderMeshTransporterInitOptions } from "./interfaces.js";
import { BehaviorSubject, Observable, Subject, Subscriber, filter, finalize, lastValueFrom, map, merge, mergeMap, switchMap, tap, throttleTime } from 'rxjs'
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
    version: number
}


type HelloMessage = TcpNode
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

    #localListeners = new BehaviorSubject({
        version: Date.now(),
        map: new Map<EventID, Set<Subscriber<any>>>()
    })

    #$requests = new Map<string, Subscriber<any>>()

    #node_id: string

    init(options: SpiderMeshTransporterInitOptions) {

        this.#node_id = options.node_id

        this.#$tcpServer = new RxjsTcpServer()

        this.#$udpServer = new RxjsUdpServer({
            namespace: NAMEPSACE,
            port: UDP_BROADCAST_PORT,
            node_id: options.node_id,
            key: UDP_SECRET_KEY
        })

        const $udpConnection = this.#$udpServer.pipe(
            tap(d => console.log(`[UDP] Connect to ${d.host}:${d.port}`)),
            mergeMap(options => RxjsTcpSocket.connect(options)),
            filter(Boolean)
        )

        lastValueFrom(merge(this.#$tcpServer, $udpConnection).pipe(
            tap(socket => socket.subscribe(console)),
            tap(socket => console.log(socket.fromRemote ? { socket_from: socket.remoteAddress } : { socket_to: socket.remoteAddress })),
            tap(socket => !socket.fromRemote && setTimeout(() => this.#hello(socket, options.services), 2000)),
            mergeMap(socket => socket.pipe(
                map(buf => {
                    console.log({ buf })
                    try {
                        return Encoder.decode<PublishOptions<any>>(buf)
                    } catch (e) { }
                }),
                filter(Boolean),
                mergeMap(async msg => { await this.#handle(socket, msg) })
            ))
        ))

        this.#localListeners.pipe(
            throttleTime(2000, undefined, { leading: false, trailing: true }),
            switchMap(() => this.#remoteNodes.values()),
            mergeMap(node => this.#hello(node.socket, options.services))
        ).subscribe()

        this.#$tcpServer.$online.subscribe(status => {
            if (!status) return
            this.#$udpServer.broadcast(status)
        })
    }

    async #handle(socket: RxjsTcpSocket, msg: PublishOptions<any>) {

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

        if (msg.event == `#hello`) {
            console.log({ hello_from: msg.data })
            const host = socket.remoteAddress
            host && await this.#link({
                ...msg.data as HelloMessage,
                host
            });
            return
        }

        this.#localListeners.getValue().map.get(msg.event)?.forEach(o => o.next(msg));
    }

    async #link(metadata: HelloMessage) {

        const linked = this.#remoteNodes.get(metadata.node_id)
        const { host } = metadata

        const socket = linked ? linked.socket : await RxjsTcpSocket.connect({
            host,
            port: metadata.port,
            keepAlive: true
        })
        if (!socket) return

        this.#remoteNodes.set(metadata.node_id, { metadata, socket })
        for (const event of metadata.listening) {
            const set = this.#remoteListeners.get(event) || new Set()
            set.add(metadata.node_id)
            this.#remoteListeners.set(event, set)
        }
        if (!linked) return
        console.log({ new_node: metadata })
        this.$nodes.next({ node_id: metadata.node_id, status: 'online' })

        socket.subscribe({
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

    async #hello(socket: RxjsTcpSocket, services: string[]) {

        const port = this.#$tcpServer.port
        if (!port) return

        const { map, version } = this.#localListeners.getValue()
        const msg: PublishOptions<HelloMessage> = {
            event: '#hello',
            data: {
                node_id: this.#node_id,
                host: '',
                port,
                listening: ['#hello', this.#node_id, '__metadata__', ...map.keys()],
                services,
                version
            }
        }
        console.log(`Say hello to ${socket.remoteAddress}`,{msg})
        socket.send(Encoder.encode(msg))
    }

    listen<T>(topic: string): Observable<T> {
        return new Observable<T>(o => {
            const { map } = this.#localListeners.getValue()
            const listeners = map.get(topic) || new Set()
            listeners.add(o)
            map.set(topic, listeners)
            this.#localListeners.next({
                map,
                version: Date.now()
            })
            return () => {
                listeners.delete(o)
                this.#localListeners.next({
                    map,
                    version: Date.now()
                })
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
            r: this.#node_id
        }

        const data = Encoder.encode(request)

        return new Observable<T>(o => {
            this.#$requests.set(request.id, o)
            node.socket.send(data)
        })
    }


} 