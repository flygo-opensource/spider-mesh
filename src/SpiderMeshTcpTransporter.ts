import { randomUUID } from "crypto";
import { SpiderMeshRpcTransporter, SpiderMeshPubsubTransporter, SpiderMesh, PublishOptions, RpcOptions, SpiderMeshTransporterInitOptions, ServiceDiscovery } from "@spider-mesh/core";
import { BehaviorSubject, Observable, Subject, Subscriber, debounceTime, filter, firstValueFrom, interval, lastValueFrom, map, merge, mergeAll, mergeMap, of, retry, retryWhen, tap, throwError, timer } from 'rxjs'
import { RxjsTcpSocket } from "./RxjsTcpSocket.js";
import { RxjsTcpServer } from "./RxjsTcpServer.js";
import { RxjsUdpServer } from "./RxjsUdpServer.js";
import { UDP_BROADCAST_PORT, NAMEPSACE, UDP_SECRET_KEY } from "./const.js"
import { Encoder } from "./Encoder.js";
import { ServiceDiscoveryMetadata } from "../../corev2/build/src/interfaces/ServiceDiscovery.js";
import { hostname } from "os";


export class ServiceNotFound extends Error { }
export class MissingPubsubTransporter extends Error { }
export class ServiceOffline extends Error { }



type TcpNode = Omit<ServiceDiscoveryMetadata, 'status'> & {
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

export class SpiderMeshTcpTransporter implements SpiderMeshRpcTransporter, SpiderMeshPubsubTransporter, ServiceDiscovery {

    $nodes = new Subject<ServiceDiscoveryMetadata>()

    #$tcpServer: RxjsTcpServer
    #$udpServer: RxjsUdpServer

    #remoteNodes = new Map<NodeID, { metadata: TcpNode, socket: RxjsTcpSocket }>
    #remoteListeners = new Map<EventID, Set<NodeID>>()

    #localListeners = new BehaviorSubject(new Map<EventID, Set<Subscriber<any>>>())

    #$requests = new Map<string, { target_node_id: string, responder: Subscriber<any> }>()


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
        sm.linkServiceDiscovery(this)
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
                    if (!socket.remoteAddress) return
                    const metadata: HelloMessage = {
                        ...msg.data as HelloMessage,
                        hostname: socket.remoteAddress,
                        ip: socket.remoteAddress
                    }
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
            return setImmediate(async () => {
                const options = msg.data as RpcRequest
                const response = this.sm.handleRpc(options)


                if (response instanceof Observable) {
                    response.subscribe({
                        complete: () => this.publish({ event: options.r, data: { id: options.id, end: true } }),
                        error: error => this.publish({ event: options.r, data: { id: options.id, error } }),
                        next: data => this.publish({ event: options.r, data: { id: options.id, data } }),
                    })
                    return
                }

                // Send #response
                try {
                    const data: RpcResponse = {
                        data: await response,
                        id: options.id,
                        end: true
                    }
                    this.publish({ event: options.r, data })
                } catch (error) {
                    const data: RpcResponse = {
                        error,
                        id: options.id
                    }
                    this.publish({ event: options.r, data })
                }
                return
            })
        }

        if (msg.event == `#${this.sm.node_id}`) {
            const { data, error, id, end } = msg.data as RpcResponse
            const req = this.#$requests.get(id)
            if (req) {
                const subscriber = req.responder
                data != undefined && subscriber.next(data)
                error && subscriber.error(error)
                end && subscriber.complete();
                (error || end) && this.#$requests.delete(id)
            }
            return
        }

        const listeners = this.#localListeners.getValue()
        listeners.get(msg.event)?.forEach(o => o.next(msg.data));
    }

    async #link($: RxjsTcpSocket, metadata: HelloMessage) {

        const linked = this.#remoteNodes.get(metadata.node_id)
        metadata.hostname = metadata.hostname || $.remoteAddress || ''
        if (!metadata.hostname) return

        const socket = linked ? linked.socket : await RxjsTcpSocket.connect({
            host: metadata.hostname,
            port: metadata.port,
            keepAlive: true
        }) || $

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
        this.$nodes.next({ ...metadata, status: 'online' })

        const refuseRpcRequests = () => {
            for (const [_, { target_node_id, responder }] of this.#$requests) {
                target_node_id == metadata.node_id && responder.error(
                    new ServiceOffline()
                )
            }
            for (const event of metadata.listening) {
                const set = this.#remoteListeners.get(event) || new Set()
                set.delete(metadata.node_id)
                this.#remoteListeners.set(event, set)
            }
            const node = this.#remoteNodes.get(metadata.node_id)
            node && this.$nodes.next({
                ...metadata,
                status: 'offline'
            })
            this.#remoteNodes.delete(metadata.node_id)
        }

        socket.subscribe({
            error: refuseRpcRequests,
            complete: refuseRpcRequests
        })

    }

    async #hello(socket: RxjsTcpSocket, back: boolean = false) {

        const port = this.#$tcpServer.port
        if (!port) return


        const services = [...Object.keys(this.sm.getLocalServices().getValue())]

        const listening = [
            '#hello',
            this.sm.node_id,
            '#' + this.sm.node_id,
            ...services,
            ...this.#localListeners.getValue().keys()
        ]

        const msg: PublishOptions<HelloMessage> = {
            event: '#hello',
            data: {
                node_id: this.sm.node_id,
                hostname: '',
                port,
                listening,
                services,
                back,
                ip: ''
            }
        }
        socket.send(Encoder.encode(msg))
    }

    listen<T>(topic: string): Observable<T> {
        const map = this.#localListeners.getValue()
        const listeners = map.get(topic) || new Set()
        !map.has(topic) && map.set(topic, listeners)
        const isNewEvent = !topic.startsWith('#') && topic != this.sm.node_id
        isNewEvent && this.#localListeners.next(map)

        return new Observable<T>(o => {
            listeners.add(o)
            return () => {
                listeners.delete(o)
            }
        })
    }

    async publish<T>(options: PublishOptions<T>) {
        const buffer = Encoder.encode(options as any)
        const nodes = this.#remoteListeners.get(options.event) || new Set()

        for (const node_id of nodes) {
            const node = this.#remoteNodes.get(node_id)
            if (node && node.socket) {
                node.socket.send(buffer)
            }

        }
    }

    #indexes = new Map<string, number>()

    async #getRpcNode(options: RpcOptions) {
        if (options.routing) {
            const node = this.#remoteNodes.get(options.routing.node_id)
            if (!node) throw new ServiceOffline()
            return node
        }
        const key = `${options.service}.${options.method}`
        while (true) {
            const nodes = await firstValueFrom(merge(of(0), interval(1000)).pipe(
                map(() => this.#remoteListeners.get(options.service)),
                filter(Boolean),
                filter(nodes => nodes.size > 0)
            ))
            for (let i = 0; i < nodes.size; i++) {
                const index = (this.#indexes.get(key) || 0 + i) % (nodes.size)
                const node_id = [...nodes][index]
                const node = this.#remoteNodes.get(node_id)
                if (!node) continue
                this.#indexes.set(key, index + 1)
                return node
            }
            await firstValueFrom(timer(1000))
        }

    }


    rpc<T>(options: RpcOptions): Observable<T> {
        const request: PublishOptions<RpcRequest> = {
            data: {
                id: randomUUID(),
                service: options.service,
                method: options.method,
                args: options.args,
                r: '#' + this.sm.node_id
            },
            event: `#rpc:${options.service}.${options.method}`
        }
        const data = Encoder.encode(request)

        return new Observable<T>(o => {
            setImmediate(async () => {
                const node = await this.#getRpcNode(options)
                this.#$requests.set(request.data.id, {
                    target_node_id: node.metadata.node_id,
                    responder: o
                })
                node.socket.send(data)
            })
            return () => this.#$requests.delete(request.data.id)
        })

    }


} 