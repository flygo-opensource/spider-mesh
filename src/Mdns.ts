import { createSocket } from "dgram";
import { DiscoveryTransporter, SpiderMesh, SpiderMeshNode } from "@spider-mesh/core";
import { networkInterfaces } from "os";
import { SPIDERMESH_UDP_BROADCAST_ADDRESS, SPIDERMESH_UDP_BROADCAST_PORT } from "./const.js";
import { filter, map, Observable } from "rxjs";
import { Subject, merge, firstValueFrom } from "rxjs";
import { debounceTime } from "rxjs/operators";
import { ReplaySubject } from "rxjs/internal/ReplaySubject";


export type MdnsMessage = {
    hi: boolean
    node: SpiderMeshNode
    sender_id: string,
    seq: number
}




export class Mdns extends DiscoveryTransporter {

    #seq = 0
    #localAddress = new Set(
        Object.values(networkInterfaces()).flat(2).map(e => e?.address).filter(Boolean)
    )
    #broadcastAddress = new Set([
        '255.255.255.255',
        ...(SPIDERMESH_UDP_BROADCAST_ADDRESS || '').split(',').map(e => {
            const ppps = e.trim().split('.')
            if (ppps.length == 4) return e.trim()
            if (ppps.length == 3) return new Array(255).fill(0).map(h => {
                return `${e.trim()}.${h}`
            })
            return []
        }).flat(2)
    ])
    #bus = new Subject<{ data: MdnsMessage, port: number, ip: string }>()
    #ready = new ReplaySubject(1)

    constructor(private sm: SpiderMesh) {
        super()
        sm.linkTransporter(this)
        console.log(`I'm node ${sm.node_id}`)
    }

    link(metadata$: Observable<SpiderMeshNode>) {
        return merge(
            new Observable<SpiderMeshNode>(o => {
                const udp4 = createSocket({
                    type: 'udp4',
                    reuseAddr: true
                })

                udp4.on('message', async (raw: Buffer, r) => {
                    try {
                        const { node, hi, sender_id, seq } = JSON.parse(raw.toString()) as MdnsMessage
                        if (node.node_id == this.sm.node_id) return
                        if (sender_id == this.sm.node_id) return
                        if (node.namespace != this.sm.namespace) return
                        node.host = node.host || r.address
                        const is_remote = !this.#localAddress.has(r.address)
                        if (is_remote && !this.#broadcastAddress.has(r.address)) return

                        // From remote
                        is_remote && await this.#broadcast(node, hi, '255.255.255.255')

                        // Process 
                        o.next(node)
                        hi && await this.#broadcast(await firstValueFrom(metadata$), false, node.host)
                    } catch (e) {
                        console.log(e)
                    }
                })

                udp4.on('listening', () => {
                    console.log(`mDNS listening on port `, SPIDERMESH_UDP_BROADCAST_PORT)
                    udp4.setBroadcast(true)
                    const subscription = this.#bus.subscribe(({ data, port, ip }) => {
                        const msg = JSON.stringify(data)
                        udp4.send(msg, 0, msg.length, port, ip)
                    })
                    o.add(subscription)
                    this.#ready.next(true)
                })
                udp4.on('error', (e) => {
                    throw e
                })
                udp4.bind(SPIDERMESH_UDP_BROADCAST_PORT, '0.0.0.0')


                return () => udp4.close()
            }),


            // Sync metadata
            metadata$.pipe(
                debounceTime(1000),
                map((metadata, index) => {
                    this.#broadcast(metadata, index == 0)
                    return null
                }),
                filter(Boolean)
            )
        )
    }

    async #broadcast(node: SpiderMeshNode, hi: boolean, target?: string) {
        await firstValueFrom(this.#ready)
        const seq = this.#seq++;
        const data: MdnsMessage = {
            sender_id: this.sm.node_id,
            node,
            hi,
            seq
        }
        if (target) {
            const ip = this.#localAddress.has(target) ? '255.255.255.255' : target
            this.#bus.next({ data, port: SPIDERMESH_UDP_BROADCAST_PORT, ip })
        } else {
            for (const ip of this.#broadcastAddress) {
                this.#bus.next({ data, port: SPIDERMESH_UDP_BROADCAST_PORT, ip })
            }
        }
    }
}
