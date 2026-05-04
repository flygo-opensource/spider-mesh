import { createSocket } from "node:dgram"
import { networkInterfaces } from "node:os"
import { finalize, firstValueFrom, fromEvent, map, merge, ReplaySubject, Subject, takeUntil, tap } from "rxjs"
import { unpack, pack } from 'msgpackr'
import type { DiscoveryEvent, DiscoveryTransporter, MdnsMessage, NodeMetadata, SpiderMeshNode } from "./types.js"
import { SPIDERMESH_WHITELIST_ADDRESS, SPIDERMESH_MULTICAST_PORT, SPIDERMESH_MULTICAST_ADDRESS } from "./const.js"
import { transportRuntime } from "./runtime.js"

export class UdpDiscovery extends Subject<DiscoveryEvent> implements DiscoveryTransporter {

    #udp4 = createSocket({
        type: 'udp4',
        reuseAddr: true
    })
    #ready = new ReplaySubject<void>(1)
    #stop$ = new Subject<void>()

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
        super()
        merge(
            fromEvent(this.#udp4, 'listening').pipe(
                tap(() => {
                    try {
                        this.#udp4.setMulticastInterface("0.0.0.0")
                        this.#udp4.setMulticastLoopback(true)
                        this.#udp4.setMulticastTTL(1)
                        this.#udp4.addMembership(SPIDERMESH_MULTICAST_ADDRESS, "0.0.0.0")
                        this.#ready.next()
                        this.#ready.complete()
                    } catch (error) {
                        this.#ready.error(error as Error)
                        this.#stop$.next()
                        this.#stop$.complete()
                        throw error
                    }
                }),
                map(() => undefined)
            ),
            fromEvent(this.#udp4, 'error').pipe(
                tap(error => {
                    this.#ready.error(error as Error)
                    this.#stop$.next()
                    this.#stop$.complete()
                    throw error
                }),
                map(() => undefined)
            ),
            fromEvent(this.#udp4, 'message').pipe(
                tap(args => {
                    const [raw, remote] = args as [Buffer, { address: string }]
                    void this.#onMessage(raw, remote.address)
                }),
                map(() => undefined)
            )
        ).pipe(
            takeUntil(this.#stop$),
            finalize(() => {
                this.#udp4.close()
            })
        ).subscribe()
        this.#udp4.bind(SPIDERMESH_MULTICAST_PORT, '0.0.0.0')
    }

    override unsubscribe() {
        this.#stop$.next()
        this.#stop$.complete()
        super.unsubscribe()
    }

    async broadcast(data: MdnsMessage<NodeMetadata>, ips: string[] = [...this.#broadcastAddress]) {
        await firstValueFrom(this.#ready)
        const node = transportRuntime.updateLocalNode(transportRuntime.withTransporters(data.node as unknown as SpiderMeshNode))
        const msg = pack({
            ...data,
            node
        })
        const targets = new Set<string>([...ips, ...this.#broadcastAddress])
        for (const ip of targets) {
            this.#udp4.send(msg, 0, msg.length, SPIDERMESH_MULTICAST_PORT, ip, e => {
                // e && console.error('Spidermesh UDP broadcast error', e)
            })
        }
    }

    async #onMessage(raw: Buffer, address: string) {
        try {
            const msg = unpack(raw) as MdnsMessage<SpiderMeshNode>
            const me = transportRuntime.localNode
            if (!me) return
            if (msg.node.node_id === me.node_id) return
            if (msg.sender_id === me.node_id) return
            if (msg.forwarder_id === me.node_id) return
            if (msg.node.namespace !== me.namespace) return

            const isRemote = !this.#localAddress.has(address)
            const node = transportRuntime.updateNode({
                ...msg.node,
                host: msg.node.host || (isRemote ? address : 'localhost')
            })

            if (isRemote && !msg.forwarder_id) {
                await this.broadcast({
                    ...msg,
                    node,
                    forwarder_id: me.node_id
                }, [SPIDERMESH_MULTICAST_ADDRESS])
            }

            if (msg.receiver_id && msg.receiver_id !== me.node_id) return

            if (msg.hi) {
                await this.broadcast({
                    node: me,
                    hi: false,
                    sender_id: me.node_id,
                    receiver_id: msg.sender_id
                }, [isRemote ? address : SPIDERMESH_MULTICAST_ADDRESS])
            }

            this.next({ discovered: node })
        } catch {
            return
        }
    }
}
