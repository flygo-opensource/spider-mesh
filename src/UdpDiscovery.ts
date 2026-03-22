import { createSocket } from "dgram";
import { DiscoveryTransporter, SpiderMesh, SpiderMeshNode } from "@spider-mesh/core";
import { networkInterfaces } from "os";
import { SPIDERMESH_UDP_BROADCAST_ADDRESS, SPIDERMESH_UDP_BROADCAST_PORT } from "./const.js";
import { from, map, Observable } from "rxjs";
import { firstValueFrom } from "rxjs";
import { debounceTime, mergeMap } from "rxjs/operators";
import { unpack, pack } from 'msgpackr'

export type MdnsMessage = {
    hi: boolean
    node: SpiderMeshNode
    sender_id: string,
    receiver_id?: string
}


export class UdpDiscovery extends DiscoveryTransporter {

    #localAddress = new Set(
        Object.values(networkInterfaces()).flat(2).map(e => e?.address).filter(Boolean)
    )
    #broadcastAddress = new Set([
        'localhost',
        ...(SPIDERMESH_UDP_BROADCAST_ADDRESS || '').split(',').map(e => {
            const ppps = e.trim().split('.')
            if (ppps.length == 4) return e.trim()
            if (ppps.length == 3) return new Array(256).fill(0).map((h, index) => {
                return `${e.trim()}.${index}`
            })
            return []
        }).flat(2)
    ])

    constructor() {
        super()
        SpiderMesh.linkTransporter(this)
    }


    link(metadata$: Observable<SpiderMeshNode>) {

        return from(firstValueFrom(metadata$)).pipe(
            mergeMap(metadata => new Observable<SpiderMeshNode>(o => {

                const udp4 = createSocket({
                    type: 'udp4',
                    reuseAddr: true
                })


                const broadcast = async (node: SpiderMeshNode, hi: boolean, target?: string, receiver_id?: string) => {

                    const data: MdnsMessage = {
                        sender_id: metadata.node_id,
                        node,
                        hi,
                        receiver_id
                    }
                    const msg = pack(data)
                    const ips = target ? [this.#localAddress.has(target) ? 'localhost' : target] : this.#broadcastAddress
                    for (const ip of ips) udp4.send(msg, 0, msg.length, SPIDERMESH_UDP_BROADCAST_PORT, ip, e => {
                        e && console.error('UDP Broadcast error', e)
                    })
                }

                udp4.on('message', async (raw: Buffer, r) => {
                    try {
                        const { node, hi, sender_id, receiver_id } = unpack(raw) as MdnsMessage
                        if (node.node_id == metadata.node_id) return
                        if (sender_id == metadata.node_id) return
                        if (node.namespace != metadata.namespace) return
                        node.host = node.host || r.address
                        const is_remote = !this.#localAddress.has(r.address)
                        if (is_remote && !this.#broadcastAddress.has(r.address)) return

                        // Re-broadcast from remote
                        is_remote && await broadcast(node, hi, 'localhost', receiver_id)

                        // Process 
                        if (receiver_id && receiver_id != metadata.node_id) return
                        o.next(node)
                        hi && await broadcast(await firstValueFrom(metadata$), false, node.host, node.node_id)
                    } catch (e) {
                    }
                })

                udp4.on('listening', () => {
                    udp4.setBroadcast(true)

                    const s = metadata$.pipe(
                        debounceTime(1000),
                        map((metadata, index) => broadcast(metadata, index == 0))
                    ).subscribe()

                    o.add(() => s.unsubscribe())

                })
                udp4.on('error', (e) => {
                    throw e
                })
                udp4.bind(SPIDERMESH_UDP_BROADCAST_PORT, '0.0.0.0')
                return () => udp4.close()
            })
            )
        )
    }

}
