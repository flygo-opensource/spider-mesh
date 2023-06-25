import { randomUUID } from "crypto";
import dgram from 'dgram'
import { createConnection, createServer, Server, Socket } from "net";
import { EventEmitter } from "events";
import { SpiderMeshTransporter } from "./SpiderMeshTransporter";
import { TcpNetConnectOpts } from "net";
import { PassThrough } from "stream";
import { ListenEventList } from "./decorators/createMicroserviceEvent";



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


class StableTCP extends EventEmitter {

    #input = new PassThrough()
    static #separator = String.fromCharCode(0x1E)

    private constructor() {
        super()
    }


    async #connect(socket: Socket) {
        let buffer = ''
        this.#input.removeAllListeners('data')
        this.#input.on('data', data => socket.write(data))

        socket.on('data', msg => {

            buffer += msg.toString('utf8')
            if (buffer.endsWith(StableTCP.#separator)) {
                for (const part of buffer.split(StableTCP.#separator)) {
                    if (part != '') {
                        try {
                            const json = JSON.parse(part)
                            buffer = ''
                            this.emit('data', json)
                        } catch (e) {
                            console.log({
                                can_not_decode: part
                            })
                        }
                    }
                }
                buffer = ''
            }
        })
    }

    static async init(options: TcpNetConnectOpts) {
        const $this = new this()
        await new Promise<boolean>(async s => {
            for (let i = 0; i <= 5; i++) {
                const socket = await new Promise<Socket | null>(s => {
                    const socket = createConnection(options)
                    socket.on('ready', () => s(socket))
                    socket.on('connect', () => s(socket))
                    socket.on('timeout', () => s(null))
                    socket.on('error', (e) => s(null))
                })
                if (!socket) return s(false)

                await $this.#connect(socket)
                i = 0
                s(true)

                // await socket error here
                const code = await new Promise(s => {
                    socket.on('close', () => s('close'))
                    socket.on('error', () => s('error'))
                    socket.on('end', () => s('close'))
                })

                if (code == 'close') {
                    $this.emit('close')
                    return
                }


                process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] Socket error, retrying in 1 sec`)
                await new Promise(s => setTimeout(s, 1000))
            }
            s(false)
        })
        return $this
    }


    static async join(socket: Socket) {
        const $this = new this()
        socket.on('error', () => $this.emit('error'))
        socket.on('close', () => $this.emit('close'))
        await $this.#connect(socket)
        return $this
    }

    async write(data: any) {
        const msg = JSON.stringify(data) + StableTCP.#separator
        const buffer = Buffer.from(msg)
        this.#input.write(buffer)
    }
}



export class BuiltinTransporter implements SpiderMeshTransporter {

    #node_offline_callbacks = new Map<string, (node_id: string) => any>
    #node_online_callbacks = new Map<string, (node_id: string) => any>
    #listeners = new Map<string, Map<string, (from_node_id: string, data: any) => any>>
    #version = Date.now()
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
        process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] Online ${node_id}:${UDP_PORT}`)
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
            port: UDP_PORT,
        }, () => udp.setBroadcast(true))

        udp.on('message', async (raw, rinfo) => {
            try {
                const json = JSON.parse(raw.toString('utf-8'))
                this.#on_message(rinfo.address, json)
            } catch (e) {
            }
        })


        // Find free port
        const { tcp_port, server } = await new Promise<{ server: Server, tcp_port: number }>(async s => {
            for (let tcp_port = 10001; true; tcp_port++) {
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
                const stable_tcp = await StableTCP.join(socket)
                stable_tcp.on('data', data => this.#on_message(remote_host, data, stable_tcp))
            }
        })






        return {
            udp,
            tcp_port,
            next: async () => {
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
                listening: [...new Set(['#hello', ...ListenEventList])],
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
            const socket = await StableTCP.init({ host, port: new_node.port, keepAlive: true, timeout: 5000 }) || tcp_socket
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


    listen<T = any>(topic: string, cb: (node_id: string, data: T) => any) {
        this.#version = Date.now()
        !this.#listeners.has(topic) && this.#listeners.set(topic, new Map())
        const id = randomUUID()
        this.#listeners.get(topic)?.set(id, cb)
        return {
            unsubscribe: () => {
                this.#version = Date.now()
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