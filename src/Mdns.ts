import { createSocket } from "dgram";
import { SpiderMeshDiscover,  SpiderMeshDiscoveryRegistry } from "@spider-mesh/core";
import { networkInterfaces } from "os";
import { SPIDERMESH_NAMESPACE, SPIDERMESH_UDP_BROADCAST_ADDRESS, SPIDERMESH_UDP_BROADCAST_PORT } from "./const.js";
import { Subject } from "rxjs/internal/Subject";
import { randomUUID } from "crypto";


export type MdnsMessage<T = any> = {
    namespace: string
    sender_id: string
    host: string
    data: T
}




export class Mdns extends Subject<any> implements SpiderMeshDiscover {


    #udp4 = createSocket({ type: 'udp4' })
    #id = randomUUID()
    #localAddress = new Set(Object.values(networkInterfaces()).flat(2).map(e => e?.address).filter(Boolean))
    #broadcastAddress = [
        '255.255.255.255',
        ...(SPIDERMESH_UDP_BROADCAST_ADDRESS || '').split(',').map(e => {
            const ppps = e.trim().split('.')
            if (ppps.length == 4) return e.trim()
            if (ppps.length == 3) return new Array(255).fill(0).map(h => {
                return `${e.trim()}.${h}`
            })
            return []
        }).flat(2)
    ]

    constructor() {
        super()
        this.#udp4.bind(SPIDERMESH_UDP_BROADCAST_PORT, '0.0.0.0', () => {
            this.#udp4.setBroadcast(true)
            this.#udp4.on('message', (raw: Buffer, r) => {
                const e = JSON.parse(raw.toString()) as MdnsMessage 
                e.host = r.address
                const is_remote = !this.#localAddress.has(r.address)
                if (e.sender_id == this.#id) return
                if (e.namespace != SPIDERMESH_NAMESPACE) return

                // From remote
                if (is_remote) {
                    const payload: MdnsMessage = {
                        host: r.address,
                        sender_id: this.#id,
                        namespace: SPIDERMESH_NAMESPACE,
                        data: e.data
                    }
                    this.#udp4.send(JSON.stringify(payload), SPIDERMESH_UDP_BROADCAST_PORT, '255.255.255.255')
                }
                // Process 
                this.next(e.data)
            })
            SpiderMeshDiscoveryRegistry.register(this)
        })
    }

    broadcast(data: any, ip?: string) {
        const e: MdnsMessage  = {
            data,
            namespace: SPIDERMESH_NAMESPACE,
            sender_id: this.#id,
            host: ''
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
