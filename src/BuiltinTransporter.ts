import { randomUUID } from "crypto";
import dgram from 'dgram'
import { createConnection, createServer, Server, NetConnectOpts, Socket } from "net";
import { networkInterfaces } from 'os'
import { EventEmitter } from "events";
import { SpiderMeshTransporter } from "./SpiderMeshTransporter";
import { TcpNetConnectOpts } from "net";
import { PassThrough } from "stream";



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
    listening_events: string[]
}


type HelloMessage = TcpNode & {
    peers: TcpNode[]
}

const UDP_PORT = Number(process.env.UDP_PORT || 10001)
const SEEDING_IP = process.env.SEEDING_IP

class StableTCP extends EventEmitter {

    #input = new PassThrough()

    private constructor(
        private options: TcpNetConnectOpts
    ) {
        super()
    }

    async #connect() {
        return new Promise<Socket | null>(s => {
            const socket = createConnection(this.options)
            socket.on('ready', () => s(socket))
            socket.on('connect', () => s(socket))
            socket.on('timeout', () => s(null))
            socket.on('error', (e) => s(null))
        })
    }


    #join_stream(socket: Socket) {
        let buffer = ''
        this.#input.removeAllListeners('data')
        this.#input.on('data', data => socket.write(data))
        socket.on('data', msg => {
            buffer += msg.toString('utf8')
            if (buffer.endsWith('\n')) {
                try {
                    const json = JSON.parse(buffer)
                    buffer = ''
                    this.emit('data', json)
                } catch (e) {

                }
            }
        })
    }

    async #init() {
        return await new Promise<boolean>(async s => {
            for (let i = 0; i <= 5; i++) {
                const socket = await this.#connect()
                if (!socket) return s(false)

                this.#join_stream(socket)
                i = 0
                s(true)

                // await socket error here
                const code = await new Promise(s => {
                    socket.on('close', () => s('close'))
                    socket.on('error', () => s('error'))
                    socket.on('end', () => s('close'))
                })

                if (code == 'close') {
                    this.emit('close')
                    return
                }

                process.env.SPIDERMESH_TCP_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] Socket error, retrying in 1 sec`)
                await new Promise(s => setTimeout(s, 1000))
            }
            s(false)
        })
    }

    static async connect(options: TcpNetConnectOpts) {
        const instance = new this(options)
        const success = await instance.#init()
        return success ? instance : null
    }

    static async update(socket: Socket) {
        const instance = new this({ host: '', port: 1 })
        socket.on('error', () => instance.emit('error'))
        socket.on('close', () => instance.emit('close'))
        await instance.#join_stream(socket)
        return instance
    }

    async write(data: any) {
        const buffer = Buffer.from(JSON.stringify(data) + '\n')
        this.#input.write(buffer)
    }
}



export class BuiltinTransporter implements SpiderMeshTransporter {

    #node_offline_callbacks = new Map<string, (node_id: string) => any>
    #node_online_callbacks = new Map<string, (node_id: string) => any>
    #listeners = new Map<string, Map<string, (from_node_id: string, data: any) => any>>

    #nodes_map = new Map<string, TcpNode & {
        socket: StableTCP
    }>

    #events_map = new Map<string, Set<string>>()

    #initing: Promise<{
        udp: dgram.Socket;
        tcp_port: number;
        next: () => Promise<void>;
        wait_error: () => Promise<any>;
        flush: () => void;
    }>

    constructor(
        public readonly node_id: string,
        public readonly namespace: string,
    ) {
        process.env.SPIDERMESH_TCP_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] Online ${node_id}`)
        setTimeout(async () => {
            while (true) {
                this.#initing = this.#init()
                const task = await this.#initing
                await task.next()
                await task.wait_error()
                await task.flush()
            }
        })

    }
    async #init() {

        // Create UDP
        const udp = dgram.createSocket({ type: 'udp4', reuseAddr: true })
        udp.bind({
            address: '0.0.0.0',
            exclusive: false,
            port: UDP_PORT
        })

        udp.on('message', async (raw, rinfo) => {
            try {
                const json = JSON.parse(raw.toString('utf-8'))
                this.#on_message(rinfo.address, json)

            } catch (e) {
            }
        })


        // Find free port
        const { tcp_port, server } = await new Promise<{ server: Server, tcp_port: number }>(async s => {
            for (let tcp_port = 10000; true; tcp_port++) {
                const server = createServer()
                server.listen(tcp_port)
                const success = await new Promise(s => {
                    server.on('listening', () => s(true))
                    server.on('error', () => s(false))
                })
                if (success) return s({ tcp_port, server })
            }
        })

        server.on('connection', async socket => {
            const remote_host = socket.remoteAddress?.split(':')?.pop()
            if (remote_host) {
                const stable_tcp = await StableTCP.update(socket)
                stable_tcp.on('data', data => this.#on_message(remote_host, data, stable_tcp))
            }
        })






        return {
            udp,
            tcp_port,
            next: async () => {
                const broadcast_ips = ['127.0.0.1'] as string[]

                // Add multicast IP or scan all subnets
                const current_ips = Object.values(networkInterfaces()).map(c => c?.map(f => f.address) || []).flat(2)
                const current_ip = current_ips.find(ip => ip.startsWith('192.168'))
                const subnet = current_ip?.split('.').slice(0, 3).join('.')
                if (subnet) {
                    for (let i = 1; i <= 255; i++) {
                        const host = `${subnet}.${i}`
                        !current_ips.includes(host) && broadcast_ips.push(host)
                    }
                }



                // Hello via UDP
                broadcast_ips.forEach(host => this.#hello(undefined, { host, socket: udp }))

                // Scan remote node ( <= internet)
                if (SEEDING_IP) {
                    const ips = SEEDING_IP.split(',').map(c => c.trim().split(':'))
                    await Promise.all(ips.map(async ([host, port]) => {
                        const stable_tcp = await StableTCP.connect({ host, port: Number(port), keepAlive: true, timeout: 5000 })
                        if (stable_tcp) {
                            stable_tcp.on('data', data => this.#on_message(host, data, stable_tcp))
                            this.#hello(stable_tcp)
                        }
                    }))
                }

            },
            wait_error: () => new Promise<any>(async s => {
                await new Promise(s => {
                    udp.on('close', s)
                    udp.on('error', s)
                    server.on('error', s)
                    server.on('drop', s)
                    server.on('close', s)
                })
            }),

            flush: () => {
                udp.close()
                server.close()
            }
        }

    }



    async #hello(socket?: StableTCP, udp_socket?: { socket: dgram.Socket, host: string }) {
        const { tcp_port } = await this.#initing

        const msg: MeshMessage = {
            data: {
                node_id: this.node_id,
                port: tcp_port,
                listening_events: ['#hello', ...this.#listeners.keys()],
                peers: [... this.#nodes_map.values()].map(({ host, listening_events, node_id, port }) => ({ listening_events, node_id, port, host }))
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


        process.env.SPIDERMESH_TCP_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] [TCP] Node online [${host}]`, new_node)

        const remote_peers_included = new_node.peers.some(p => p.node_id == this.node_id)

        if (!this.#nodes_map.get(new_node.node_id)?.socket) {
            const socket = await StableTCP.connect({ host, port: new_node.port, keepAlive: true, timeout: 5000 }) || tcp_socket
            if (!socket) return

            const on_offline = (e) => {
                process.env.SPIDERMESH_TCP_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] [TCP] Node offline ${new_node.node_id}`)
                this.#node_offline_callbacks?.forEach(cb => cb(new_node.node_id))
                const node = this.#nodes_map.get(new_node.node_id)
                node && node.listening_events.map(evt => {
                    this.#events_map.get(evt)?.delete(node.node_id)
                })
                this.#nodes_map.delete(new_node.node_id)

            }
            socket.on('error', on_offline)
            socket.on('close', on_offline)
            this.#nodes_map.set(new_node.node_id, { ...new_node, host, socket })

            !remote_peers_included && await this.#hello(socket)
        }



        for (const event of new_node.listening_events) {
            !this.#events_map.has(event) && this.#events_map.set(event, new Set())
            this.#events_map.get(event)?.add(new_node.node_id)
        }

        remote_peers_included && this.#node_online_callbacks.forEach(fn => fn(new_node.node_id))


    }

    on_node_offline(cb: (node_id: string) => any) {
        const id = randomUUID()
        this.#node_offline_callbacks.set(id, cb)
        return { unsubscribe: () => this.#node_offline_callbacks.delete(id) }
    }

    on_node_online(cb: (node_id: string) => any) {
        const id = randomUUID()
        this.#node_online_callbacks.set(id, cb)
        return { unsubscribe: () => this.#node_online_callbacks.delete(id) }
    }


    #broadcast_listen: NodeJS.Timer
    listen<T = any>(topic: string, cb: (node_id: string, data: T) => any) {

        this.#broadcast_listen && clearTimeout(this.#broadcast_listen)
        this.#broadcast_listen = setTimeout(() => {
            // Notify about update
            this.#nodes_map.forEach(node => this.#hello(node.socket))
        }, 1000)
        !this.#listeners.has(topic) && this.#listeners.set(topic, new Map())
        const id = randomUUID()
        this.#listeners.get(topic)?.set(id, cb)
        return {
            unsubscribe: () => {
                this.#listeners.get(topic)?.delete(id)
                this.#listeners.get(topic)?.size == 0 && this.#listeners.delete(topic)
            }
        }
    }


    async publish<T = any>(
        event: string,
        node_id: string | null,
        data: T,
        queue?: boolean
    ) {

        const msg: MeshMessage = {
            data,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic: event || node_id || '#'
        }

        if (node_id) {
            const node = this.#nodes_map.get(node_id)
            node?.socket?.write(msg)
            return
        }



        for (const node_id of this.#events_map.get(event) || []) {
            const node = this.#nodes_map.get(node_id)
            node?.socket?.write(msg)
        }


    }

} 