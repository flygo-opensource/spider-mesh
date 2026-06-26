import { createSocket } from "node:dgram"
import { networkInterfaces } from "node:os"
import { distinctUntilChanged, EMPTY, finalize, firstValueFrom, fromEvent, map, merge, Observable, of, ReplaySubject, Subject, takeUntil, tap } from "rxjs"
import { unpack, pack } from 'msgpackr'
import type { DiscoveryEvent, DiscoveryTransporter, MdnsMessage, NodeMetadata, NodeRef, Registry, ServiceDirectory, SpiderMeshNode } from '@spider-mesh/core'
import { SPIDERMESH_WHITELIST_ADDRESS, SPIDERMESH_MULTICAST_PORT, SPIDERMESH_MULTICAST_ADDRESS } from "./const.js"

export class UdpDiscovery extends Subject<DiscoveryEvent> implements DiscoveryTransporter, ServiceDirectory {

    #udp4 = createSocket({
        type: 'udp4',
        reuseAddr: true
    })
    #ready = new ReplaySubject<void>(1)
    #stop$ = new Subject<void>()
    #localNode: SpiderMeshNode | null = null
    #registry?: Registry

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

    constructor(registry?: Registry) {
        super()
        this.#registry = registry
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
                    // On Linux, sending a UDP datagram to a host/port with no listener makes the
                    // kernel surface the resulting ICMP "destination/port unreachable" on the next
                    // recv as a socket 'error' (ECONNREFUSED / ENETUNREACH / EHOSTUNREACH / ECONNRESET).
                    // During unicast peer discovery (whitelist scan) this is expected and routine, so
                    // it must NOT tear down the discovery socket. (macOS never delivers these to the
                    // socket, which is why the bug only manifests on Linux.)
                    const code = (error as NodeJS.ErrnoException)?.code
                    if (code && ['ECONNREFUSED', 'ENETUNREACH', 'EHOSTUNREACH', 'ECONNRESET'].includes(code)) return
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

    async broadcast(data: MdnsMessage<NodeMetadata>) {
        await firstValueFrom(this.#ready)
        const node = data.node as unknown as SpiderMeshNode
        this.#localNode = node
        const msg = pack({
            ...data,
            node
        })
        const targets = new Set<string>(this.#broadcastAddress)
        this.#send(msg, targets)
    }

    #send(msg: Buffer, targets: Iterable<string>) {
        for (const ip of targets) {
            this.#udp4.send(msg, 0, msg.length, SPIDERMESH_MULTICAST_PORT, ip, e => {
                // e && console.error('Spidermesh UDP broadcast error', e)
            })
        }
    }

    async #onMessage(raw: Buffer, address: string) {
        try {
            const msg = unpack(raw) as MdnsMessage<SpiderMeshNode>
            const me = this.#localNode
            if (!me) return
            if (msg.node.node_id === me.node_id) return
            if (msg.sender_id === me.node_id) return
            if (msg.forwarder_id === me.node_id) return
            if (msg.node.namespace !== me.namespace) return

            const isRemote = !this.#localAddress.has(address)
            const node = {
                ...msg.node,
                host: msg.node.host || (isRemote ? address : 'localhost')
            } satisfies SpiderMeshNode

            if (isRemote && !msg.forwarder_id) {
                const forwarded = pack({
                    ...msg,
                    node,
                    forwarder_id: me.node_id
                })
                this.#send(forwarded, [SPIDERMESH_MULTICAST_ADDRESS])
            }

            if (msg.receiver_id && msg.receiver_id !== me.node_id) return

            if (msg.hi) {
                const reply = pack({
                    node: me,
                    hi: false,
                    sender_id: me.node_id,
                    receiver_id: msg.sender_id
                })
                this.#send(reply, [isRemote ? address : SPIDERMESH_MULTICAST_ADDRESS])
            }

            // tcp owns its peer table: ingest into the shared registry so Http2Rpc /
            // Http2Pubsub can route, and so core can read availability via ServiceDirectory.
            this.#registry?.upsertPeer(node)
            this.next({ discovered: node })
        } catch {
            return
        }
    }

    // --- ServiceDirectory: expose the registry-backed peer table to core ---
    //
    // Only nodes that are actually RPC-routable are reported, i.e. those whose Http2Rpc
    // endpoint port has been announced. A provider broadcasts its service before its HTTP/2
    // port finishes binding, so a half-formed peer (service known, no port yet) must NOT be
    // counted — otherwise `wait()` resolves early and the first call hits "metadata missing".
    //
    // We subscribe to `nodes$` directly (not `registry.watch`, which de-dupes by node_id and
    // would swallow the half-formed → ready transition of the same node).

    // NOTE (known coupling): this keys on the 'Http2Rpc' transporter name. It matches both
    // Http2Rpc#getTransporterMetadata and the default registered name (the class name), so a
    // standard setup is consistent. Registering Http2Rpc under a CUSTOM name would defeat this
    // readiness check (the metadata would live under that custom key) — keep the three in sync.
    #isRpcReady(node: SpiderMeshNode): boolean {
        const meta = node.transporters?.Http2Rpc ?? node.transporters?.http2rpc
        return Number((meta as { port?: number } | undefined)?.port) > 0
    }

    #readyNodes(service: string): NodeRef[] {
        return (this.#registry?.listPeers(service) ?? []).filter(node => this.#isRpcReady(node))
    }

    watchService(service: string): Observable<NodeRef[]> {
        const registry = this.#registry
        if (!registry) return of([])
        return registry.nodes$.pipe(
            map(() => this.#readyNodes(service)),
            distinctUntilChanged((prev, curr) => {
                if (prev.length !== curr.length) return false
                const prevIds = new Set(prev.map(n => n.node_id))
                return curr.every(n => prevIds.has(n.node_id))
            })
        )
    }

    listNodes(service: string): NodeRef[] {
        return this.#readyNodes(service)
    }
}
