import { Socket, createSocket } from "dgram"
import { BehaviorSubject, Observable, Subject, combineLatest, filter, finalize, first, from, interval, map, mergeMap, takeWhile, tap } from "rxjs"
import { networkInterfaces } from 'os'
import { createHmac } from "crypto"
import { BROADCAST_INTERVAL, UDP_SECRET_KEY } from "./const.js"
import { RxjsTcpSocket } from "./RxjsTcpSocket.js"

export type BroadcastMessage = {
    port: number
    transporter_id: string
    namespace: string
    sig: string
}

export type RxjsUdpBroadcasterConfig = {
    namespace: string,
    udp_address?: string,
    udp_port: number,
    transporter_id: string
    $tcp_server_port: BehaviorSubject<number>
}

export class RxjsUdpServer extends Observable<RxjsTcpSocket> {


    constructor(private config: RxjsUdpBroadcasterConfig) {
        super(o => {

            const nodes = new Set<string>()

            const { udp_port } = config
            const udp = createSocket({
                type: 'udp4',
                reuseAddr: true
            })

            udp.bind({
                address: '0.0.0.0',
                port: udp_port,
            }, () => udp.setBroadcast(true))

            udp.on('message', async (data, rinfo) => {
                try {
                    const msg = JSON.parse(data.toString('utf-8')) as BroadcastMessage
                    if (msg.namespace != config.namespace) return
                    if (msg.transporter_id == config.transporter_id) return
                    if(nodes.has(msg.transporter_id)) return 
                    const sig = createHmac('SHA256', UDP_SECRET_KEY).update(`${msg.namespace}|${msg.transporter_id}|${msg.port}`).digest('base64')
                    if (msg.sig != sig) return

                    const socket = await RxjsTcpSocket.connect({
                        ...msg,
                        host: rinfo.address,
                        keepAlive: true,
                        retry_delay_ms: 5000,
                        retry_times: 5
                    })
                    if (socket) {
                        if (nodes.has(msg.transporter_id)) {
                            socket.close()
                        } else {
                            nodes.add(msg.transporter_id)
                            o.next(socket)
                            socket.$status.pipe(
                                filter(s => s == 'closed' || s == 'error'),
                                tap(s => nodes.delete(msg.transporter_id)),
                                first()
                            ).subscribe()
                        }
                    }

                } catch (e) {

                }
            })

            config.$tcp_server_port.subscribe(port => this.#broadcast(udp, port))

            if (BROADCAST_INTERVAL) {
                combineLatest([
                    config.$tcp_server_port,
                    interval(BROADCAST_INTERVAL)
                ]).subscribe(([port]) => port && this.#broadcast(udp, port))
            }

        })
    }

    #broadcast(udp: Socket, port: number) {

        const { namespace, transporter_id, udp_port, udp_address = '' } = this.config
        const broadcast_ips = ['255.255.255.255']
        const network_address = (
            Object.entries(networkInterfaces())
                .filter(([i]) => !i.startsWith('lo'))
                .map(e => e[1])
                .flat(2)
                .filter(d => d && d.family == 'IPv4' && d.address)
                .map(d => d!.address.split('.').slice(0, 3).join('.') + '.255')
        )
        const env_address = udp_address.split(',').map(a => a.trim()).filter(a => !!a)

        for (const address of [...network_address, ...env_address]) {
            const splited = address.split('.')
            splited.length == 4 && broadcast_ips.push(address)
            splited.length == 3 && new Array(256).fill(0).map((_, i) => broadcast_ips.push(`${address}.${i}`))
        }
        const msg: BroadcastMessage = {
            namespace,
            transporter_id,
            port,
            sig: ''
        }
        const sig = createHmac('SHA256', UDP_SECRET_KEY).update(`${msg.namespace}|${msg.transporter_id}|${msg.port}`).digest('base64')
        const json = JSON.stringify({ ...msg, sig })


        from(broadcast_ips).pipe(
            mergeMap(async host => {
                await udp.send(
                    Buffer.from(json),
                    udp_port,
                    host,
                    e => {}
                )
            }, 50)

        ).subscribe()
    }
}