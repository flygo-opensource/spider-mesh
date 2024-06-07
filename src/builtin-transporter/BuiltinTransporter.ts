import { randomUUID } from "crypto";
import { PublishMetadata, SpiderMeshTransporter, SpiderMeshTransporterEvent, SpiderMeshTransporterEventMetadata, TcpNodeStatus } from "../interfaces/SpiderMeshTransporter.js";
import { Observable, Subject, filter, finalize, first, map, merge, mergeMap, tap } from 'rxjs'
import { RxjsTcpSocket } from "./RxjsTcpSocket.js";
import { RxjsTcpServer } from "./RxjsTcpServer.js";
import { RxjsUdpServer } from "./RxjsUdpServer.js";
import { UDP_BROADCAST_PORT, UDP_BROADCAST_ADDRESS, NAMEPSACE } from "../const.js"
import { Encodable, Encoder } from "../Encoder.js";

type TcpNode = {
    transporter_id: string
    host: string
    port: number
    listening: string[]
    version: number,
    peers: TcpNode[]
}


type HelloMessage = TcpNode

type NodeID = string
type ListenderID = string
type ListenderCallback = (data: SpiderMeshTransporterEvent<any>) => any
type EventID = string
type NodeWithSocket = TcpNode & { socket: RxjsTcpSocket }

export class BuiltinTransporter implements SpiderMeshTransporter {

    public readonly $nodes_status = new Subject<TcpNodeStatus>()

    #version = Date.now()
    #listeners = new Map<EventID, Map<ListenderID, ListenderCallback>>
    #nodes_map = new Map<NodeID, NodeWithSocket>
    #events_map = new Map<EventID, Set<NodeID>>()
    #$rebroadcast = new Subject<void>()
    public readonly transporter_id: string = randomUUID()
    #$tcp = new RxjsTcpServer()

    constructor() {
        const $udp = new RxjsUdpServer({
            $tcp_server_port: this.#$tcp.$port,
            namespace: NAMEPSACE,
            udp_port: UDP_BROADCAST_PORT,
            udp_address: UDP_BROADCAST_ADDRESS,
            transporter_id: this.transporter_id
        })

        merge(this.#$tcp, $udp).pipe(
            mergeMap(socket => {
                const $ = socket.$incoming_data.pipe(
                    map(buf => {
                        try {
                            return Encoder.decode<SpiderMeshTransporterEvent>(buf)
                        } catch (e) { }
                    }),
                    filter(Boolean),
                    mergeMap(async msg => {
                        if (msg.topic == `#hello`) {
                            const host = socket.rawSocket.remoteAddress
                            host && await this.#sync_node(socket, {
                                ...msg.payload as HelloMessage,
                                host
                            });
                            return
                        }
                        const event: SpiderMeshTransporterEvent = { ...msg, received_at: Date.now() }
                        this.#listeners.get(msg.topic)?.forEach(cb => cb(event));
                    })
                )
                !socket.opened_by_remote_side && this.#tcp_hello(socket)
                return $
            })
        ).subscribe()


    }




    async #sync_node(current_socket: RxjsTcpSocket, metadata: HelloMessage) {

        const host = metadata.host

        for (const event of metadata.listening) {
            !this.#events_map.has(event) && this.#events_map.set(event, new Set());
            this.#events_map.get(event)?.add(metadata.transporter_id);
        }

        const cached = this.#nodes_map.get(metadata.transporter_id)
        if (cached && cached.version > 0) return;


        const socket = current_socket.opened_by_remote_side ? (await RxjsTcpSocket.connect({
            host,
            port: metadata.port,
            keepAlive: true,
            retry_delay_ms: 200,
            retry_times: 5
        }) || current_socket) : current_socket;

        const new_node = {
            ...metadata,
            host,
            socket,
            peers: metadata.peers.map(p => ({ ...p, peers: [] }))
        };
        this.#nodes_map.set(metadata.transporter_id, new_node);




        const remote_info = new_node.peers.find(p => p.transporter_id == this.transporter_id);
        const peer_updated = remote_info ? remote_info.version == this.#version : false;
        !peer_updated && this.#tcp_hello(socket);

        this.$nodes_status.next({
            online: true,
            remote_transporter_id: metadata.transporter_id,
        });

        socket.$status.pipe(
            filter(status => status != 'ready'),
            first(),
            finalize(() => {
                const node = this.#nodes_map.get(metadata.transporter_id);
                node && node.listening.map(evt => {
                    this.#events_map.get(evt)?.delete(metadata.transporter_id);
                });
                this.#nodes_map.delete(metadata.transporter_id);
                this.$nodes_status.next({
                    remote_transporter_id: metadata.transporter_id,
                    online: false
                });
            })
        ).subscribe();

    }


    async #tcp_hello(tcp_socket: RxjsTcpSocket) {
        const port = this.#$tcp.$port.getValue()

        const msg: SpiderMeshTransporterEvent<HelloMessage, { namespace: string }> = {
            created_at: Date.now(),
            id: randomUUID(),
            metadata: {
                namespace: NAMEPSACE,
            },
            topic: '#hello',
            sti: this.transporter_id,
            payload: {
                host: '',
                transporter_id: this.transporter_id,
                port,
                listening: ['#hello', ... this.#listeners.keys()],
                peers: [... this.#nodes_map.values()].map(({ socket, ...node }) => node),
                version: this.#version
            },
            received_at: 0
        }
        tcp_socket.write(Encoder.encode(msg))
    }

    listen<T extends Encodable = Encodable, Metadata extends SpiderMeshTransporterEventMetadata = SpiderMeshTransporterEventMetadata>(topic: string) {
        return new Observable<SpiderMeshTransporterEvent<T, Metadata>>(o => {
            this.#version = Date.now()
            !this.#listeners.has(topic) && this.#listeners.set(topic, new Map())
            const id = randomUUID()
            this.#listeners.get(topic)?.set(
                id,
                (data) => o.next(data as SpiderMeshTransporterEvent<T, Metadata>)
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


    async publish<T extends Encodable, Metadata extends SpiderMeshTransporterEventMetadata = SpiderMeshTransporterEventMetadata>({ payload, event, metadata, rti }: PublishMetadata<T, Metadata>) {

        const msg: SpiderMeshTransporterEvent<T, Metadata> = {
            created_at: Date.now(),
            id: randomUUID() as string,
            metadata,
            payload,
            received_at: 0,
            sti: this.transporter_id,
            topic: event || rti || '#'
        }

        const buffer = Encoder.encode(msg)

        if (rti) {
            await this.#nodes_map.get(rti)?.socket?.write(buffer)
        } else {
            for (const node_id of this.#events_map.get(event) || []) {
                const node = this.#nodes_map.get(node_id)
                await node?.socket?.write(buffer)
            }
            return
        }
    }

} 