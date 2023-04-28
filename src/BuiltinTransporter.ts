import { randomUUID } from "crypto";
import dgram from 'dgram'
import MessagePack from 'msgpack-js'
import { Socket, createConnection, createServer, Server } from "net";

type MeshMessage<T = any> = {
    topic: string
    namespace: string
    data: T,
    sender_node_id: string
    broadcastable?: boolean
}

type HelloMessage = {
    node_id: string,
    port: number
}

const UDP_BROADCAST_PORT = Number(process.env.UDP_BROADCAST_PORT || 10001)
const MULTICAST_IP = process.env.MULTICAST_IP || '239.255.255.250'

export class BuiltinTransporter {

    public readonly id = 'lan-transporter'
    #udp_sender?: dgram.Socket
    #udp_receiver?: dgram.Socket

    #node_offline_callbacks = new Map<string, (node_id: string) => any>
    #node_online_callbacks = new Map<string, Function>
    #listeners = new Map<string, Map<string, (from_node_id: string, data: any) => any>>

    #nodes_map = new Map<string, {
        node_id: string,
        host: string,
        port: number
        socket?: Socket
    }>

    #initing: Promise<{ next_loop: Promise<void> }>

    constructor(
        private node_id: string,
        private namespace: string
    ) {
        console.log({ME: node_id})
        setTimeout(async () => {
            while (true) {
                this.#initing = new Promise<{ next_loop: Promise<void> }>(async done => {
                    
                    console.log(`Starting TCP mesh`)

                    this.#udp_sender = dgram.createSocket({ type: 'udp4', reuseAddr: true });
                    this.#udp_sender.bind(UDP_BROADCAST_PORT);
                    this.#udp_receiver = dgram.createSocket({ type: 'udp4', reuseAddr: true });
                    this.#udp_receiver.bind(UDP_BROADCAST_PORT, () => this.#udp_receiver?.addMembership(MULTICAST_IP))

                    const { port, server } = await new Promise<{ server: Server, port: number }>(async s => {
                        for (let port = 10000; true; port++) {
                            console.log({port})
                            const server = createServer()
                            server.listen(port)
                            const success = await new Promise(s => {
                                server.on('listening', () => s(true))
                                server.on('error', () => s(false))
                            })
                            if (success) return s({ port, server })
                        }
                    })

                    console.log(`[${new Date().toLocaleTimeString()}] Listening on ${port}`)

                    server.on('connection', socket => {
                        socket.on('data', data => this.#on_message(port, data, undefined, socket))
                    })

                    this.#udp_receiver.on('message', (raw, rinfo) => this.#on_message(port, raw, rinfo, undefined))

                    // Ready for process
                    done({
                        next_loop: new Promise(async ok => {
                            await new Promise(s => {
                                server.on('error', s)
                                server.on('drop', s)
                                server.on('close', s)
                            })

                            this.#udp_sender?.close()
                            this.#udp_receiver?.close()
                            ok()
                        })
                    })

                    // UDP broadcast 
                    await this.publish(`#hello`, { port, node_id, namespace })

                   

                })
                const { next_loop } = await this.#initing
                await next_loop
                console.log(`TCP mesh error, fixing ...`)
            }
        })

    }




    async #on_message(current_tcp_port: number, data: Buffer, rinfo?: dgram.RemoteInfo, tcp_socket?: Socket) {
        const remote_address = rinfo?.address || tcp_socket?.remoteAddress
        if (!remote_address) return
        const msg = MessagePack.decode(data) as MeshMessage
        if (msg.sender_node_id == this.node_id) return

        // New node
        if (msg.topic == `#hello` && msg.namespace == this.namespace) {
            const new_node = msg.data as HelloMessage
            if (this.#nodes_map.has(new_node.node_id)) return
            // console.log(`[${new Date().toLocaleTimeString()}] New node: [${new_node.node_id}]`)
            

            const socket = tcp_socket || createConnection({
                host: rinfo?.address,
                port: new_node.port
            })

            this.#nodes_map.set(new_node.node_id, { ...new_node, host: remote_address,socket })
            rinfo && socket.on('data', data => this.#on_message(current_tcp_port, data, undefined, socket))
            const on_offline = () => {
                this.#node_offline_callbacks?.forEach(cb => cb(new_node.node_id))
                this.#nodes_map.delete(new_node.node_id)
            }
            socket.on('error', on_offline)
            socket.on('timeout', on_offline)
            socket.on('close', on_offline)
            socket.connecting && await new Promise(s => socket.on('connect', s))
            rinfo && this.request(msg.sender_node_id, { node_id: this.node_id, port: current_tcp_port } as HelloMessage, '#hello')
            return
        } 
        this.#listeners.get(msg.topic)?.forEach(cb => cb(msg.sender_node_id, msg.data))
    }


    on_node_offline(cb: (node_id: string) => any) {
        const id = randomUUID()
        this.#node_offline_callbacks.set(id, cb)
        return { unsubscribe: () => this.#node_offline_callbacks.delete(id) }
    }


    listen<T = any>(topic: string, cb: (node_id: string, data: T) => any) {
        !this.#listeners.has(topic) && this.#listeners.set(topic, new Map())
        const id = randomUUID()
        this.#listeners.get(topic)?.set(id, cb)
        return { unsubscribe: () => this.#listeners.get(topic)?.delete(id) }
    }

    async request<T = any>(node_id: string, data: T, topic?: string) {
        // console.log(`[${new Date().toLocaleTimeString()}] Send ${topic || 'request'} to ${node_id}`)
        await this.#initing
        const msg: MeshMessage = {
            data,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic: topic || node_id
        }
        const buffer = MessagePack.encode(msg)
        const socket = this.#nodes_map.get(node_id)?.socket
        if (!socket) throw new Error('SOCKET_NOT_FOUND')
        socket.write(buffer)
    }

    async publish<T = any>(topic: string, data: T, queue?: boolean) {
        await this.#initing
        // console.log(`[${new Date().toLocaleTimeString()}] Broadcast ${topic}`, this.namespace)
        const msg: MeshMessage = {
            data,
            namespace: this.namespace,
            sender_node_id: this.node_id,
            topic
        }
        const buffer = MessagePack.encode(msg)
        await new Promise(
            s => this.#udp_sender?.send(buffer, 0, buffer.length, UDP_BROADCAST_PORT, MULTICAST_IP, s)
        )
    }

}
