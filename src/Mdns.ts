import { createSocket } from "dgram";
import { SpiderMesh, SpiderMeshNode, DiscoveryTransporter } from "@spider-mesh/core";
import { networkInterfaces } from "os";
import { SPIDERMESH_UDP_BROADCAST_ADDRESS, SPIDERMESH_UDP_BROADCAST_PORT } from "./const.js";


export type HelloEvent<T = {}> = T & {
    namespace: string
    sender_id: string
    node_id: string
    host: string
}


export class Mdns implements DiscoveryTransporter {

    #udp4 = createSocket({
        type: 'udp4',
        reuseAddr: true
    })

    #localAddress = new Set(Object.values(networkInterfaces()).flat(2).map(e => e?.address).filter(Boolean))
    #broadcastAddress = [
        '255.255.255.255',
        ...SPIDERMESH_UDP_BROADCAST_ADDRESS.split(',').map(e => {
            const ppps = e.trim().split('.')
            if (ppps.length == 4) return e.trim()
            if (ppps.length == 3) return new Array(255).fill(0).map(h => {
                return `${e.trim()}.${h}`
            })
            return []
        }).flat(2)
    ]

    constructor(private sm: SpiderMesh) {
      
        this.#udp4.bind(SPIDERMESH_UDP_BROADCAST_PORT, '0.0.0.0', () => {
            this.#udp4.setBroadcast(true)
            this.#udp4.on('message', (raw: Buffer, r) => {
                const e = JSON.parse(raw.toString()) as HelloEvent<SpiderMeshNode>
                e.host = r.address
                const is_remote = !this.#localAddress.has(r.address)
                if (e.sender_id == this.sm.node_id) return
                if (e.namespace != this.sm.namespace) return

                // From remote
                if (is_remote) {
                    const payload: HelloEvent = {
                        ...e,
                        host: r.address,
                        sender_id: this.sm.node_id
                    }
                    this.#udp4.send(JSON.stringify(payload), SPIDERMESH_UDP_BROADCAST_PORT, '255.255.255.255')
                }

                // Process
                sm.sync({ ...e, online: true })
            })
            sm.add(this)
        })

    }

    broadcast<T>(payload: T, ip?: string) {
        const e: HelloEvent<T> = {
            ...payload,
            host: '',
            namespace: this.sm.namespace,
            node_id: this.sm.node_id,
            sender_id: this.sm.node_id
        }
        const msg = JSON.stringify(e)
        if (ip) {
            const target = this.#localAddress.has(ip) ? '255.255.255.255' : ip
            this.#udp4.send(msg, SPIDERMESH_UDP_BROADCAST_PORT, target)
        } else {
            for (const ip of this.#broadcastAddress) {
                this.#udp4.send(msg, SPIDERMESH_UDP_BROADCAST_PORT, ip)
            }
        }
    }
} 
