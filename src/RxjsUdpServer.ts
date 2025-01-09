import { createSocket } from "dgram"
import { Observable, from, mergeMap } from "rxjs"
import { networkInterfaces } from 'os'
import { createHmac } from "crypto"
import { UDP_BROADCAST_PORT, UDP_SECRET_KEY } from "./const.js"


export type RxjsUdpBroadcasterConfig = {
    namespace: string,
    host?: string
    port: number
    node_id: string,
    key: string
}

export type BroadcastMessage = {
    host: string
    port: number
    node_id: string
    namespace: string
    sig: string
}


export class RxjsUdpServer extends Observable<BroadcastMessage> {

    #udp = createSocket({
        type: 'udp4',
        reuseAddr: true
    })

    constructor(private config: RxjsUdpBroadcasterConfig) {
        super(o => {
            const nodes = new Set<string>()
            this.#udp.on('error', e => console.log({ e }))
            this.#udp.on('message', async (data, rinfo) => {
                try {
                    const msg = JSON.parse(data.toString('utf-8')) as BroadcastMessage
                    if (msg.namespace != config.namespace) return
                    if (msg.node_id == config.node_id) return
                    if (nodes.has(msg.node_id)) return
                    nodes.add(msg.node_id)
                    const sig = createHmac('SHA256', UDP_SECRET_KEY).update(`${msg.namespace}|${msg.node_id}|${msg.port}`).digest('base64')
                    if (msg.sig != sig) return
                    const host = msg.host || rinfo.address
                    o.next({ ...msg, host })
                } catch (e) {

                }
            })
            try {
                this.#udp.bind(UDP_BROADCAST_PORT, '0.0.0.0', () => this.#udp.setBroadcast(true))
            } catch (e) {
                console.error((e as Error).message)
            }
        })
    }

    broadcast(options: Omit<BroadcastMessage, 'node_id' | 'namespace' | 'sig' | 'host'>) {
        const { namespace } = this.config
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
        for (const address of network_address) {
            const splited = address.split('.')
            splited.length == 4 && broadcast_ips.push(address)
            splited.length == 3 && new Array(256).fill(0).map((_, i) => broadcast_ips.push(`${address}.${i}`))
        }
        const msg: BroadcastMessage = {
            host: this.config.host || '',
            namespace,
            node_id: this.config.node_id,
            port: options.port,
            sig: ''
        }
        const sig = createHmac('SHA256', UDP_SECRET_KEY).update(`${msg.namespace}|${msg.node_id}|${msg.port}`).digest('base64')
        const json = JSON.stringify({ ...msg, sig } as BroadcastMessage)


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