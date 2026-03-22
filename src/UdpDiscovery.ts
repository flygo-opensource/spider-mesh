import { createSocket } from "dgram";
import { DiscoveryTransporter, SpiderMesh, SpiderMeshNode } from "@spider-mesh/core";
import { networkInterfaces } from "os";
import { SPIDERMESH_UDP_BROADCAST_ADDRESS, SPIDERMESH_UDP_BROADCAST_PORT, SPIDERMESH_UDP_MULTICAST_ADDRESS } from "./const.js";
import { from, map, Observable } from "rxjs";
import { firstValueFrom } from "rxjs";
import { debounceTime, mergeMap } from "rxjs/operators";
import { unpack, pack } from 'msgpackr'

export type MdnsMessage = {
    hi: boolean
    node: SpiderMeshNode
    sender_id: string
    forwarder_id?: string
    receiver_id?: string
}


export class UdpDiscovery extends DiscoveryTransporter {

    #localAddress = new Set(
        Object.values(networkInterfaces()).flat(2).map(e => e?.address).filter(Boolean)
    )
    #broadcastAddress = new Set([
        SPIDERMESH_UDP_MULTICAST_ADDRESS,
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


                const broadcast = async (data: MdnsMessage, ips: string[] = [...this.#broadcastAddress]) => {
                    const msg = pack(data)
                    for (const ip of ips) {
                        udp4.send(msg, 0, msg.length, SPIDERMESH_UDP_BROADCAST_PORT, ip, e => {
                            e && console.error('UDP Broadcast error', e)
                        })
                    }
                }

                udp4.on('message', async (raw: Buffer, r) => {
                    try {
                        const msg = unpack(raw) as MdnsMessage
                        if (msg.node.node_id == metadata.node_id) return
                        if (msg.sender_id == metadata.node_id) return
                        if (msg.forwarder_id == metadata.node_id) return
                        if (msg.node.namespace != metadata.namespace) return
                        
                        // Forward message in case from remote
                        const node = { ...msg.node, host: msg.node.host || r.address }
                        const is_remote = !this.#localAddress.has(r.address);
                        is_remote && !msg.forwarder_id && await broadcast({
                            ...msg,
                            node,
                            forwarder_id: metadata.node_id
                        }, [SPIDERMESH_UDP_MULTICAST_ADDRESS]);

                        // Ignore if message has receiver and it's not me
                        if (msg.receiver_id && msg.receiver_id != metadata.node_id) return;

                        // Say hi back if first time seen 
                        msg.hi && await broadcast({
                            node: metadata,
                            hi: false,
                            sender_id: metadata.node_id,
                            receiver_id: msg.sender_id
                        }, [is_remote ? r.address : SPIDERMESH_UDP_MULTICAST_ADDRESS]);

                        // Emit node
                        o.next(node);

                    } catch (e) {
                    }
                })

                udp4.on('listening', () => {
                    udp4.setMulticastInterface("127.0.0.1");
                    udp4.addMembership(SPIDERMESH_UDP_MULTICAST_ADDRESS, "127.0.0.1");
                    const s = metadata$.pipe(
                        debounceTime(1000),
                        map(node => broadcast({
                            node,
                            hi: true,
                            sender_id: metadata.node_id
                        }))
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
