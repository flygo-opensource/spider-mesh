import { createSocket } from "dgram"
import { Observable, filter, first, from, mergeMap, tap } from "rxjs"
import { networkInterfaces } from 'os'
import { createHmac } from "crypto"
import { UDP_SECRET_KEY } from "./const.js"
import { RxjsTcpSocket } from "./RxjsTcpSocket.js"

export type BroadcastMessage = {
    // master: boolean
    host: string
    port: number
    node_id: string
    namespace: string
    sig: string
}

export type RxjsUdpBroadcasterConfig = {
    namespace: string,
    address?: string,
    port: number
    node_id: string
}

export class RxjsUdpServer extends Observable<RxjsTcpSocket> {

    #udp = createSocket({
        type: 'udp4',
        reuseAddr: true
    })

    constructor(private config: RxjsUdpBroadcasterConfig) {
        super(o => {
            const nodes = new Set<string>()
            this.#udp.on('message', async (data, rinfo) => {
                try {
                    const msg = JSON.parse(data.toString('utf-8')) as BroadcastMessage
                    if (msg.namespace != config.namespace) return
                    if (msg.node_id == config.node_id) return
                    if (nodes.has(msg.node_id)) return
                    const sig = createHmac('SHA256', UDP_SECRET_KEY).update(`${msg.namespace}|${msg.node_id}|${msg.port}`).digest('base64')
                    if (msg.sig != sig) return

                    const socket = await RxjsTcpSocket.connect({
                        ...msg,
                        host: msg.host || rinfo.address,
                        keepAlive: true,
                        retry_delay_ms: 5000,
                        retry_times: 5
                    })
                    if (socket) {
                        if (nodes.has(msg.node_id)) {
                            socket.close()
                        } else {
                            nodes.add(msg.node_id)
                            socket.$status.pipe(
                                filter(s => s == 'closed' || s == 'error'),
                                tap(s => nodes.delete(msg.node_id)),
                                first()
                            ).subscribe()
                            o.next(socket)
                        }
                    }

                } catch (e) {

                }
            })
        })
    }

    start() {
        try {
            this.#udp.on('error', e => console.log({e}))
            this.#udp.bind(10000)
        } catch (e) {
            console.error((e as Error).message)
        }
    }

    broadcast(port: number) {
        const { namespace, address = '' } = this.config
        const broadcast_ips = ['255.255.255.255']
        const network_address = (
            Object.entries(networkInterfaces())
                .filter(([i]) => !i.startsWith('lo'))
                .map(e => e[1])
                .flat(2)
                .filter(d => d && d.family == 'IPv4' && d.address)
                .map(d => [
                    d?.address!,
                    d!.address.split('.').slice(0, 3).join('.') + '.255'
                ])
                .flat(2)
        )
        const env_address = address.split(',').map(a => a.trim()).filter(a => !!a)
        const ips = [...network_address, ...env_address]
        for (const address of ips) {
            const splited = address.split('.')
            splited.length == 4 && broadcast_ips.push(address)
            splited.length == 3 && new Array(256).fill(0).map((_, i) => broadcast_ips.push(`${address}.${i}`))
        }
        const msg: BroadcastMessage = {
            host: '',
            namespace,
            node_id: this.config.node_id,
            port,
            sig: ''
        }
        const sig = createHmac('SHA256', UDP_SECRET_KEY).update(`${msg.namespace}|${msg.node_id}|${msg.port}`).digest('base64')
        const json = JSON.stringify({ ...msg, sig })


        from(broadcast_ips).pipe(
            mergeMap(async host => {
                await this.#udp.send(
                    Buffer.from(json),
                    this.config.port,
                    host,
                    e => { }
                )
            }, 50)
        ).subscribe()
    }
}