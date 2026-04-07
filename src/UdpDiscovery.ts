import { createSocket } from "dgram";
import { networkInterfaces } from "os";
import { SPIDERMESH_WHITELIST_ADDRESS, SPIDERMESH_MULTICAST_PORT, SPIDERMESH_MULTICAST_ADDRESS } from "./const.js";
import { BehaviorSubject, debounceTime, exhaustMap, from, map, ReplaySubject } from "rxjs";
import { firstValueFrom, fromEvent, merge, switchMap, mergeMap, filter, tap } from "rxjs";
import { unpack, pack } from 'msgpackr'
import { MdnsMessage, NodeMetadata, SpiderMeshNode } from "@spider-mesh/types";



export class UdpDiscovery {

    #udp4 = createSocket({
        type: 'udp4',
        reuseAddr: true
    })
    #ready$ = new ReplaySubject<boolean>(1)

    #localAddress = new Set(
        Object.values(networkInterfaces()).flat(2).map(e => e?.address).filter(Boolean)
    )
    #broadcastAddress = new Set([
        SPIDERMESH_MULTICAST_ADDRESS,
        ...(SPIDERMESH_WHITELIST_ADDRESS || '').split(',').map(e => {
            const ppps = e.trim().split('.')
            if (ppps.length == 4) return e.trim()
            if (ppps.length == 3) return new Array(254).fill(0).map((h, index) => {
                return `${e.trim()}.${index + 1}`
            })
            return []
        }).flat(2)
    ])

    constructor() {
        this.#udp4.on('listening', () => {
            this.#udp4.setMulticastInterface("0.0.0.0");
            this.#udp4.setMulticastLoopback(true);
            this.#udp4.setMulticastTTL(1);
            this.#udp4.addMembership(SPIDERMESH_MULTICAST_ADDRESS, "0.0.0.0");
            this.#ready$.next(true)
        })
        this.#udp4.on('error', (e) => {
            throw e
        })
        this.#udp4.bind(SPIDERMESH_MULTICAST_PORT, '0.0.0.0')
    }

    async broadcast<T extends NodeMetadata>(data: MdnsMessage<T>, ips: string[] = [...this.#broadcastAddress]) {
        await firstValueFrom(this.#ready$)
        const msg = pack(data)
        for (const ip of ips) {
            this.#udp4.send(msg, 0, msg.length, SPIDERMESH_MULTICAST_PORT, ip, e => {
                e && console.error('Spidermesh UDP broadcast error', e)
            })
        }
    }


    link<T extends NodeMetadata>(metadata$: BehaviorSubject<T> | ReplaySubject<T>) {
        return merge(
            fromEvent<[Buffer, { address: string }]>(this.#udp4, 'message').pipe(
                mergeMap(async ([raw, r]) => {
                    try {
                        const msg = unpack(raw) as MdnsMessage<T>
                        const me = await firstValueFrom(metadata$)
                        if (msg.node.node_id == me.node_id) return
                        if (msg.sender_id == me.node_id) return
                        if (msg.forwarder_id == me.node_id) return
                        if (msg.node.namespace != me.namespace) return

                        // Forward message in case from remote
                        const is_remote = !this.#localAddress.has(r.address);
                        const node = {
                            ...msg.node,
                            host: msg.node.host || (is_remote ? r.address : 'localhost')
                        }
                        is_remote && !msg.forwarder_id && await this.broadcast({
                            ...msg,
                            node,
                            forwarder_id: me.node_id
                        }, [SPIDERMESH_MULTICAST_ADDRESS]);

                        // Ignore if message has receiver and it's not me
                        if (msg.receiver_id && msg.receiver_id != me.node_id) return;

                        // Say hi back if first time seen 
                        if (msg.hi) {
                            const node = await firstValueFrom(metadata$)
                            await this.broadcast({
                                node,
                                hi: false,
                                sender_id: node.node_id,
                                receiver_id: msg.sender_id
                            }, [is_remote ? r.address : SPIDERMESH_MULTICAST_ADDRESS]);
                        }
                        return node

                    } catch (e) { }
                }),
            ),

            metadata$.pipe(
                debounceTime(500),
                exhaustMap(async metadata => {
                    await this.broadcast({
                        node: metadata,
                        hi: true,
                        sender_id: metadata.node_id
                    })
                }),
                map(() => false)
            )
        ).pipe(
            filter(Boolean)
        )
    }

}
