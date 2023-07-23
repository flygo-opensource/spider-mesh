import { randomUUID } from "crypto";
import dgram from 'dgram'
import { createServer } from "net";
import { PublishMetadata, SpiderMeshTransporter, SpiderMeshTransporterEvent } from "./SpiderMeshTransporter";
import { BehaviorSubject, Observable, firstValueFrom, fromEvent, merge } from 'rxjs'
import { StableTCP } from "./StableTCP";
import { waitFirstEvent } from "./helpers/waitFirstEvent";


type MeshMessage<T = any> = {
    topic: string
    namespace: string
    data: T,
    sender_node_id: string
    broadcastable?: boolean
}

type TcpNode = {
    host: string
    port: number
    node_id: string
    listening: string[]
    version: number
}


type HelloMessage = TcpNode & {
    peers: TcpNode[]
}

const UDP_PORT = Number(process.env.UDP_PORT || 10000)
const SEEDING_IP_RANGES = process.env.SEEDING_IP_RANGE
const SEEDING_IPS = process.env.SEEDING_IP



export class BuiltinTransporter implements SpiderMeshTransporter {

    #node_offline_callbacks = new Map<string, (node_id: string) => any>
    #node_online_callbacks = new Map<string, (node_id: string) => any>
    #listeners = new Map<string, Map<string, (from_node_id: string, data: any) => any>>
    #version = Date.now()
    #tcp_listening_port: number
    #nodes_map = new Map<string, TcpNode & {
        socket: StableTCP
    }>
    public readonly $nodes_status = new Observable<{ node_id: string, online: boolean }>()
    #events_map = new Map<string, Set<string>>()
    #round_robin_indexes = new Map<string, number>()


    constructor(
        public readonly node_id: string,
        public readonly namespace: string
    ) { }

    #starting: Promise<void> | undefined
    async start() {
        if (!this.#starting) this.#starting = new Promise<void>(async s => {
            while (true) {
                const tcp_listener = await this.#start_tcp_server()
                const udp_adverting = await this.#start_udp_broadcaster()
                s()
                await Promise.race([tcp_listener.$error, udp_adverting.$error])
                tcp_listener.stop()
                udp_adverting.stop()
            }
        })
        return await this.#starting
    }

    async #start_tcp_server() {
        for (let port = 10001; true; port++) {
            const server = createServer()
            server.listen(port)
            const success = await new Promise(s => {
                server.on('listening', () => s(true))
                server.on('error', () => s(false))
            })
            if (!success) continue
            server.on('connection', async socket => {
                const remote_host = socket.remoteAddress?.split(':')?.pop()
                if (remote_host) {
                    const stable_tcp = new StableTCP(undefined, socket)
                    stable_tcp.on('data', data => this.#on_message(remote_host, data, stable_tcp))
                }
            })
            this.#tcp_listening_port = port
            return {
                port,
                server,
                stop: () => server.close(),
                $error: waitFirstEvent(server, 'close', 'drop', 'error')
            }
        }
    }

    async #start_udp_broadcaster() {
        const udp = dgram.createSocket({ type: 'udp4', reuseAddr: true })
        udp.bind({
            address: '0.0.0.0',
            port: UDP_PORT,
        }, () => udp.setBroadcast(true))

        udp.on('message', async (raw, rinfo) => {
            try {
                const json = JSON.parse(raw.toString('utf-8'))
                this.#on_message(rinfo.address, json)
            } catch (e) {
            }
        })


        const broadcast_ips = ['255.255.255.255'] as string[]

        // Add multicast IP or scan all subnets
        if (SEEDING_IP_RANGES) {
            const list = SEEDING_IP_RANGES.split(',').map(ip => ip.trim())
            for (const range of list) {
                const subnet = range.split('.').slice(0, 3).join('.')
                for (let i = 1; i <= 255; i++) {
                    const host = `${range}.${i}`
                    broadcast_ips.push(host)
                }
            }
        }

        // Add seeding ip
        if (SEEDING_IPS) {
            const list = SEEDING_IPS.split(',').map(ip => ip.trim())
            for (const host of list) {
                broadcast_ips.push(host)
            }
        }

        // Hello via UDP
        broadcast_ips.forEach(host => {
            this.#hello(undefined, { host, socket: udp })
        })

        return {
            socket: udp,
            stop: () => udp.close(),
            $error: waitFirstEvent(udp, 'close', 'error')
        }
    }

    async #hello(socket?: StableTCP, udp_socket?: { socket: dgram.Socket, host: string }) {

        const msg: MeshMessage = {
            data: {
                node_id: this.node_id,
                port: this.#tcp_listening_port,
                listening: [...new Set(['#hello'])],
                peers: [... this.#nodes_map.values()].map(({ host, listening, node_id, port }) => ({ listening, node_id, port, host })),
                version: this.#version
            } as HelloMessage,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic: '#hello'
        }
        if (socket) {
            socket?.write(msg)
        }

        if (udp_socket) {
            const buffer = Buffer.from(JSON.stringify(msg), 'utf8')
            udp_socket?.socket.send(buffer, UDP_PORT, udp_socket.host)
        }


    }

    async #on_message(remote_address: string, msg: MeshMessage, old_socket?: StableTCP) {

        if (!remote_address) return
        if (!msg) return
        if (msg.sender_node_id == this.node_id) return

        // New node 
        if (msg.topic == `#hello` && msg.namespace == this.namespace) {
            return this.#add_node(remote_address, msg.data as HelloMessage, old_socket)
        }

        this.#listeners.get(msg.topic)?.forEach(cb => cb(msg.sender_node_id, msg.data))
    }

    async #add_node(host: string, new_node: HelloMessage, tcp_socket?: StableTCP) {


        process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] [TCP] Node online [${host}]`, new_node)

        const remote_info = new_node.peers.find(p => p.node_id == this.node_id)
        const peer_updated = remote_info && remote_info.version == this.#version


        if (!this.#nodes_map.get(new_node.node_id)?.socket) {
            const socket = await new StableTCP({ host, port: new_node.port, keepAlive: true, timeout: 5000 }).connect() || tcp_socket
            if (!socket) return
            const on_offline = (e) => {
                process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] [TCP] Node offline ${new_node.node_id}`)
                this.#node_offline_callbacks?.forEach(cb => cb(new_node.node_id))
                const node = this.#nodes_map.get(new_node.node_id)
                node && node.listening.map(evt => {
                    this.#events_map.get(evt)?.delete(node.node_id)
                })
                this.#nodes_map.delete(new_node.node_id)

            }
            socket.on('error', on_offline)
            socket.on('close', on_offline)
            this.#nodes_map.set(new_node.node_id, { ...new_node, host, socket })
            !peer_updated && await this.#hello(socket)
        }



        for (const event of new_node.listening) {
            !this.#events_map.has(event) && this.#events_map.set(event, new Set())
            this.#events_map.get(event)?.add(new_node.node_id)
        }

        remote_info && this.#node_online_callbacks.forEach(fn => fn(new_node.node_id))


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
            return () => {
                this.#version = Date.now()
                this.#listeners.get(topic)?.delete(id)
                this.#listeners.get(topic)?.size == 0 && this.#listeners.delete(topic)
            }
        })

    }


    async publish<T = any>({ data, event, node_id, routing_key }: PublishMetadata<T>) {

        const msg: MeshMessage = {
            data,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic: event || node_id || '#'
        }

        if (node_id == 'all' || !node_id) {
            for (const node_id of this.#events_map.get(event) || []) {
                const node = this.#nodes_map.get(node_id)
                await node?.socket?.write(msg)
            }
            return
        }

        if (node_id == 'rr') {
            const key = `${event}.${routing_key}`
            const nodes = [... this.#events_map.get(event) || []]
            const current_index = (this.#round_robin_indexes.get(key) || 0) + 1
            this.#round_robin_indexes.set(key, current_index)
            const node_id = nodes[current_index % nodes.length]
            await this.#nodes_map.get(node_id)?.socket?.write(msg)
            return
        }


        node_id && await this.#nodes_map.get(node_id)?.socket?.write(msg)



    }

} 