import { randomUUID } from "crypto";
import { SpiderMeshRpcTransporter, SpiderMeshPubsubTransporter, PublishOptions, RpcOptions } from "./interfaces.js";
import { BehaviorSubject, Observable, Subject, Subscriber, filter, finalize, lastValueFrom, map, merge, mergeMap, switchMap, tap, throttleTime } from 'rxjs'
import { RxjsTcpSocket } from "./RxjsTcpSocket.js";
import { RxjsTcpServer } from "./RxjsTcpServer.js";
import { RxjsUdpServer } from "./RxjsUdpServer.js";
import { UDP_BROADCAST_PORT, UDP_BROADCAST_ADDRESS, NAMEPSACE, UDP_SECRET_KEY } from "./const.js"
import { Encoder } from "./Encoder.js";

export class ServiceNotFound extends Error { }
export class MissingPubsubTransporter extends Error { }
export class ServiceOffline extends Error { }

type TcpNode = {
    node_id: string
    host: string
    port: number
    listening: string[]
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

    #$tcp_server: RxjsTcpServer
    #$udp_server: RxjsUdpServer

    #remote_nodes = new Map<NodeID, { metadata: TcpNode, socket: RxjsTcpSocket }>
    #remote_listeners = new Map<EventID, Set<NodeID>>()

    #local_listeners = new BehaviorSubject({
        version: Date.now(),
        map: new Map<EventID, Set<Subscriber<any>>>()
    })

    #$requests = new Map<string, Subscriber<any>>()

    #node_id: string

    init(options: { node_id: string; }) {

        console.log({ init: options })

        this.#node_id = options.node_id

        this.#$tcp_server = new RxjsTcpServer()

        this.#$udp_server = new RxjsUdpServer({
            namespace: NAMEPSACE,
            port: UDP_BROADCAST_PORT,
            node_id: options.node_id,
            key: UDP_SECRET_KEY
        })

        const $udpConnection = this.#$udp_server.pipe(
            map(d => new RxjsTcpSocket(d))
        )

        lastValueFrom(merge(this.#$tcp_server, $udpConnection).pipe(
            tap(socket => !socket.fromRemote && this.#hello(socket)),
            map(socket => socket.pipe(
                map(buf => {
                    try {
                        return Encoder.decode<PublishOptions<any>>(buf)
                    } catch (e) { }
                }),
                filter(Boolean),
                mergeMap(async msg => { await this.#handle(socket, msg) }),
                finalize(() => {
                    console.log(`Connection `)
                })
            ))
        ))

        this.#local_listeners.pipe(
            throttleTime(2000, undefined, { leading: false, trailing: true }),
            switchMap($ => this.#remote_nodes.values()),
            mergeMap(node => this.#hello(node.socket))
        ).subscribe()

        this.#$tcp_server.$port.subscribe(
            port => {
                if (!port) return
                console.log({ port })
                this.#$udp_server.broadcast({ port })
            }
        )
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

        this.#local_listeners.getValue().map.get(msg.event)?.forEach(o => o.next(msg));
    }

    async #link(metadata: HelloMessage) {

        const linked = this.#remote_nodes.get(metadata.node_id)
        const { host } = metadata

        const socket = linked ? linked.socket : new RxjsTcpSocket({
            host,
            port: metadata.port,
            keepAlive: true
        })
        if (!socket) return

        this.#remote_nodes.set(metadata.node_id, { metadata, socket })
        for (const event of metadata.listening) {
            const set = this.#remote_listeners.get(event) || new Set()
            set.add(metadata.node_id)
            this.#remote_listeners.set(event, set)
        }
        if (!linked) return
        console.log({ new_node: metadata })
        this.$nodes.next({ node_id: metadata.node_id, status: 'online' })

        socket.subscribe({
            complete: () => {
                for (const event of metadata.listening) {
                    const set = this.#remote_listeners.get(event) || new Set()
                    set.delete(metadata.node_id)
                    this.#remote_listeners.set(event, set)
                }
                this.$nodes.next({ node_id: metadata.node_id, status: 'offline' })
                console.log(`Node ${metadata.node_id} oflfine`)
            }
        })

    }


    async #hello(tcp_socket: RxjsTcpSocket) {

        const port = this.#$tcp_server.$port.getValue()
        if (!port) return

        console.log(`Say hello to ${tcp_socket.remoteAddress}`)

        const { map, version } = this.#local_listeners.getValue()
        const msg: PublishOptions<HelloMessage> = {
            event: '#hello',
            data: {
                node_id: this.#node_id,
                host: '',
                port,
                listening: ['#hello', this.#node_id, '__metadata__', ...map.keys()],
                version
            }
        }
        tcp_socket.write(Encoder.encode(msg))
    }

    listen<T>(topic: string): Observable<T> {
        return new Observable<T>(o => {
            const { map } = this.#local_listeners.getValue()
            const listeners = map.get(topic) || new Set()
            listeners.add(o)
            map.set(topic, listeners)
            this.#local_listeners.next({
                map,
                version: Date.now()
            })
            return () => {
                listeners.delete(o)
                this.#local_listeners.next({
                    map,
                    version: Date.now()
                })
            }
        })
    }

    async publish<T>(options: PublishOptions<T>) {
        const buffer = Encoder.encode(options as any)
        for (const node_id of this.#remote_listeners.get(options.event) || []) {
            const node = this.#remote_nodes.get(node_id)
            node?.socket?.write(buffer)
        }
    }

    #indexes = new Map<string, number>()

    #getRpcNode(options: RpcOptions) {
        if (options.node_id) {
            const node = this.#remote_nodes.get(options.node_id)
            if (!node) throw new ServiceOffline('NODE_OFFLINE')
            return node
        }
        const key = `${options.service}.${options.method}`
        const nodes = this.#remote_listeners.get(options.service) || new Set()
        for (let i = 1; i <= nodes.size; i++) {
            const index = (this.#indexes.get(key) || 0 + i) % (nodes.size)
            const node_id = [...nodes][index]
            const node = this.#remote_nodes.get(node_id)
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
            node.socket.write(data)
        })
    }


} 