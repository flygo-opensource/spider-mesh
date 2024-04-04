import { randomUUID } from "crypto";
import { PublishMetadata, SpiderMeshTransporter, SpiderMeshTransporterEvent } from "../interfaces/SpiderMeshTransporter.js";
import { Observable, Subject, debounceTime, filter, finalize, first, from, map, merge, mergeAll, mergeMap, take, takeUntil, tap } from 'rxjs'
import { RxjsTcpSocket } from "./RxjsTcpSocket.js";
import { RxjsTcpServer } from "./RxjsTcpServer.js";
import { RxjsUdpBroadcaster } from "./RxjsUdpBroadcaster.js";
import { UDP_BROADCAST_PORT, UDP_BROADCAST_ADDRESS } from "../const.js"
import { Encoder } from "../Encoder.js";

type MeshMessage<T = any> = {
    topic: string
    namespace: string
    data: T,
    sender_node_id: string
}

type TcpNode = {
    host: string
    port: number
    node_id: string
    listening: string[]
    version: number,
    peers: TcpNode[]
}


type HelloMessage = TcpNode



type NodeID = string
type ListenderID = string
type ListenderCallback = (from_node_id: string, data: any) => any
type EventID = string
type NodeWithSocket = TcpNode & { socket: RxjsTcpSocket }

export class BuiltinTransporter implements SpiderMeshTransporter {

    public readonly $nodes_status = new Subject<{ node_id: string, online: boolean }>()

    #version = Date.now()
    #listeners = new Map<EventID, Map<ListenderID, ListenderCallback>>
    #nodes_map = new Map<NodeID, NodeWithSocket>
    #events_map = new Map<EventID, Set<NodeID>>()
    #$rebroadcast = new Subject<void>()

    #running_tcp_port: number | null = null

    constructor(
        public readonly node_id: string,
        public readonly namespace: string
    ) { }

    #on_new_connection(socket: RxjsTcpSocket) {
        socket.$incoming_data
            .pipe(
                takeUntil(socket.$status.pipe(filter(s => s == 'closed'))),
                map(buf => Encoder.decode<MeshMessage>(buf)),
                filter(Boolean),
                filter(msg => msg.sender_node_id != this.node_id),
                filter(msg => msg.namespace == this.namespace)
            )
            .subscribe(
                async msg => {
                    if (msg.topic == `#hello`) return await this.#sync_node(socket, msg.data)
                    this.#listeners.get(msg.topic)?.forEach(cb => cb(msg.sender_node_id, msg.data))
                }
            )
        !socket.opened_by_remote_side && this.#tcp_hello(socket)
    }

    async start() {

        const tcp_server = await RxjsTcpServer.start<MeshMessage>()

        const udp_broadcaster = await RxjsUdpBroadcaster.start({
            namespace: this.namespace,
            node_id: this.node_id,
            udp_port: UDP_BROADCAST_PORT,
            udp_address: UDP_BROADCAST_ADDRESS
        })

        const $udp_connections = udp_broadcaster.$new_node_discovered.pipe(
            mergeMap(node => RxjsTcpSocket.connect<MeshMessage>({
                ...node,
                keepAlive: true,
                retry_delay_ms: 5000,
                retry_times: 5
            })),
            filter(Boolean)
        )

        tcp_server.$online.subscribe(({ port, $connection: $tcp_connections, $error }) => {
            this.#running_tcp_port = port
            merge($tcp_connections, $udp_connections).pipe(
                takeUntil($error),
                map(socket => this.#on_new_connection(socket))
            ).subscribe()

            udp_broadcaster.broadcast(port)

            this.#$rebroadcast.pipe(
                debounceTime(1000),
                takeUntil($error),
                map(() => [... this.#nodes_map.values()]),
                mergeAll(),
                mergeMap(node => this.#tcp_hello(node.socket))
            ).subscribe()

        })


    }


    async #sync_node(node_socket: RxjsTcpSocket<any>, new_node: HelloMessage) {
        const host = node_socket.rawSocket.remoteAddress
        if (!host) return

        const new_node_id = new_node.node_id
        const remote_info = new_node.peers.find(p => p.node_id == this.node_id)
        const peer_updated = remote_info ? remote_info.version == this.#version : false

        for (const event of new_node.listening) {
            !this.#events_map.has(event) && this.#events_map.set(event, new Set())
            this.#events_map.get(event)?.add(new_node_id)
        }

        if (this.#nodes_map.has(new_node_id)) return

        const socket = node_socket.opened_by_remote_side ? (await RxjsTcpSocket.connect({
            host,
            port: new_node.port,
            keepAlive: true,
            retry_delay_ms: 200,
            retry_times: 5
        }) || node_socket) : node_socket

        const cleaner = () => {
            const node = this.#nodes_map.get(new_node_id)
            node && node.listening.map(evt => {
                this.#events_map.get(evt)?.delete(node.node_id)
            })
            this.#nodes_map.delete(new_node_id)
            this.$nodes_status.next({ node_id: new_node_id, online: false })
        }

        socket.$status.pipe(
            takeUntil(socket.$status.pipe(filter(s => s == 'closed'))),
            tap(status => {
                if (status == 'ready') {
                    const nn = {
                        ...new_node,
                        host,
                        socket,
                        peers: new_node.peers.map(p => ({ ...p, peers: [] }))
                    }
                    this.#nodes_map.set(new_node_id, nn)
                    this.$nodes_status.next({ node_id: new_node_id, online: true })
                } else {
                    cleaner()
                }
            }),
            finalize(cleaner)
        ).subscribe()

        !peer_updated && this.#tcp_hello(node_socket)

    }


    async #tcp_hello(tcp_socket: RxjsTcpSocket) {
        if (!this.#running_tcp_port) return
        const msg: MeshMessage = {
            data: {
                node_id: this.node_id,
                port: this.#running_tcp_port,
                listening: ['#hello', ... this.#listeners.keys()],
                peers: [... this.#nodes_map.values()].map(({ socket, ...node }) => node),
                version: this.#version
            } as HelloMessage,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic: '#hello'
        }
        tcp_socket.write(Encoder.encode(msg))
    }

    listen<T = any>(topic: string) {
        return new Observable<SpiderMeshTransporterEvent<T>>(o => {
            this.#version = Date.now()
            !this.#listeners.has(topic) && this.#listeners.set(topic, new Map())
            const id = randomUUID()
            this.#listeners.get(topic)?.set(
                id,
                (sender_node_id, data) => o.next({
                    sender_node_id,
                    data
                })
            )
            this.#$rebroadcast.next()
            return () => {
                this.#version = Date.now()
                this.#listeners.get(topic)?.delete(id)
                this.#listeners.get(topic)?.size == 0 && this.#listeners.delete(topic)
                this.#$rebroadcast.next()
            }
        })

    }


    async publish<T = any>({ data, event, node_id }: PublishMetadata<T>) {

        const msg: MeshMessage = {
            data,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic: event || node_id || '#'
        }

        const buf = Encoder.encode(msg)

        if (node_id == 'all' || !node_id) {
            for (const node_id of this.#events_map.get(event) || []) {
                const node = this.#nodes_map.get(node_id)
                await node?.socket?.write(buf)
            }
            return
        }


        node_id && await this.#nodes_map.get(node_id)?.socket?.write(buf)



    }

} 