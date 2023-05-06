import { randomUUID } from "crypto";
import dgram from 'dgram'
import { createConnection, createServer, Server, NetConnectOpts } from "net";
import { networkInterfaces } from 'os'
import { Duplex, PassThrough } from "stream";

async function tryCatch<T>(fn: (...args: any[]) => T | Promise<T> | Promise<T>) {
    try {
        return [null, typeof fn == 'function' ? await fn() : await fn] as [any, T]
    } catch (e) {
        return [e, null] as [any, T]
    }
}

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

const UDP_MULTICAST_PORT = Number(process.env.UDP_MULTICAST_PORT || 10001)
const MULTICAST_IP = process.env.MULTICAST_IP || '239.255.255.250'
const PRIVATE_SUBNET = process.env.PRIVATE_SUBNET
const SEEDING_IP = process.env.SEEDING_IP


const initAutoReconnectConnection = ({ reconnect_intervel = 10, ...options }: NetConnectOpts & { reconnect_intervel?: number }) => {

    return new Promise<Duplex | null>(async s => {

        const $ = new PassThrough()

        for (let i = 1; i <= reconnect_intervel; i++) {
            const socket = await new Promise<Duplex | null>(s => {
                const socket = createConnection(options)
                socket.on('connect', () => s(socket))
                socket.on('error', () => s(null))
                socket.on('timeout', () => s(null))
            })
            if (socket) {
                s($)
                $.pipe(socket)
                socket.on('data', data => $.emit('data', data))
                socket.on('close', () => $.emit('close'))
                i = 1
                await new Promise(s => socket.on('error', s))
                await new Promise(s => setTimeout(s, 500))
                continue
            }
            return s(null)

        }
        $.emit('error')

    })

}


export class BuiltinTransporter {

    public readonly id = 'lan-transporter'

    #node_offline_callbacks = new Map<string, (node_id: string) => any>
    #listeners = new Map<string, Map<string, (from_node_id: string, data: any) => any>>

    #nodes_map = new Map<string, TcpNode & {
        socket?: Duplex
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
        private node_id: string,
        private namespace: string,

    ) {

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
        const udp = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        udp.bind(UDP_MULTICAST_PORT, () => udp.addMembership(MULTICAST_IP))
        udp.on('message', (raw, rinfo) => {
            this.#on_message(rinfo.address, raw)
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

        server.on('connection', socket => {
            const remote_host = socket.remoteAddress?.split(':')?.pop()
            remote_host && socket.on('data', data => this.#on_message(remote_host, data, socket))
        })






        return {
            udp,
            tcp_port,
            next: async () => {
                const broadcast_ips = [] as string[]

                // Add multicast IP
                MULTICAST_IP && broadcast_ips.push(MULTICAST_IP)

                // Scan subnet IP 
                if (PRIVATE_SUBNET) {
                    const current_ips = Object.values(networkInterfaces()).map(c => c?.map(f => f.address) || []).flat(2)
                    for (let i = 1; i <= 255; i++) {
                        const host = `${PRIVATE_SUBNET}.${i}`
                        !current_ips.includes(host) && broadcast_ips.push(host)
                    }
                }

                // Hello via UDP
                broadcast_ips.forEach(host => this.#hello(undefined, { host, socket: udp }))

                // Scan remote node ( <= internet)
                if (SEEDING_IP) {
                    const ips = SEEDING_IP.split(',').map(c => c.trim().split(':'))
                    for (const [host, port] of ips) {
                        const socket = await initAutoReconnectConnection({ host, port: Number(port), timeout: 2500, })
                        socket?.on('data', data => this.#on_message(host, data, socket))
                        socket && this.#hello(socket)
                    }
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



    async #hello(socket?: Duplex, udp_socket?: { socket: dgram.Socket, host: string }) {
        const { tcp_port } = await this.#initing

        const msg: MeshMessage = {
            data: {
                node_id: this.node_id,
                port: tcp_port,
                listening_events: ['#hello', '#join', ...this.#listeners.keys(), this.node_id],
                peers: [... this.#nodes_map.values()].map(({ host, listening_events, node_id, port }) => ({ listening_events, node_id, port, host }))
            } as HelloMessage,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic: '#hello'
        }


        const buffer = Buffer.from(JSON.stringify(msg), 'utf8')
        socket?.write(buffer)
        udp_socket?.socket.send(buffer, UDP_MULTICAST_PORT, udp_socket.host)
    }




    async #on_message(remote_address: string, data: Buffer, tcp_socket?: Duplex) {
        if (!remote_address) return
        const [_, msg] = await tryCatch<MeshMessage>(() => JSON.parse(data.toString('utf8')))
        if (!msg) return
        if (msg.sender_node_id == this.node_id) return

        // New node
        if (msg.topic == `#hello` && msg.namespace == this.namespace) {
            return this.#add_node(remote_address, msg.data as HelloMessage, tcp_socket)
        }

        this.#listeners.get(msg.topic)?.forEach(cb => cb(msg.sender_node_id, msg.data))
    }

    async #add_node(host: string, new_node: HelloMessage, tcp_socket?: Duplex) {

        process.env.MESHSCALE_TCP_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] New node [${host}]`, new_node)
        if (!this.#nodes_map.get(new_node.node_id)?.socket) {

            const socket = await initAutoReconnectConnection({ host, port: new_node.port, timeout: 2500 }) || tcp_socket
            if (!socket) return

            const on_offline = (e) => {
                process.env.MESHSCALE_TCP_DEBUG && console.log(`Node offline: ${new_node.node_id}`)
                this.#node_offline_callbacks?.forEach(cb => cb(new_node.node_id))
                this.#nodes_map.delete(new_node.node_id)
            }
            socket.on('error', e => on_offline)
            socket.on('close', e => on_offline)
            this.#nodes_map.set(new_node.node_id, { ...new_node, host, socket })

            new_node.peers.every(p => p.node_id != this.node_id) && await this.#hello(socket)
        }

        for (const event of new_node.listening_events) {
            !this.#events_map.has(event) && this.#events_map.set(event, new Set())
            this.#events_map.get(event)?.add(new_node.node_id)
        }



        new_node.peers.filter(peer => peer.node_id != this.node_id && !this.#nodes_map.has(peer.node_id)).forEach(peer => this.#add_node(host, { ...peer, peers: [] }))
    }

    on_node_offline(cb: (node_id: string) => any) {
        const id = randomUUID()
        this.#node_offline_callbacks.set(id, cb)
        return { unsubscribe: () => this.#node_offline_callbacks.delete(id) }
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
        const buffer = Buffer.from(JSON.stringify(msg), 'utf8')

        if (node_id) {
            const node = this.#nodes_map.get(node_id)
            node?.socket?.write(buffer)
            return
        }


        for (const node_id of this.#events_map.get(event) || []) {
            const node = this.#nodes_map.get(node_id)
            node?.socket?.write(buffer)
        }


    }

}

