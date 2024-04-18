import { createSocket } from "dgram"
import { Observable } from "rxjs"
import { networkInterfaces } from 'os'

export type RxjsUdpBroadcasterConfig = {
    node_id: string,
    namespace: string,
    udp_address?: string,
    udp_port: number
}

export class RxjsUdpBroadcaster {



    static async start({ namespace, node_id, udp_port, udp_address = '' }: RxjsUdpBroadcasterConfig) {
        type BroadcastMessage = {
            port: number
            node_id: string
            namespace: string
        }

        const udp = createSocket({
            type: 'udp4',
            reuseAddr: true
        })

        udp.bind({
            address: '0.0.0.0',
            port: udp_port,
        }, () => udp.setBroadcast(true))

        const $new_node_discovered = new Observable<BroadcastMessage & { host: string }>(o => {
            udp.on('message', async (raw, rinfo) => {
                try {
                    const info = JSON.parse(raw.toString('utf-8')) as BroadcastMessage
                    if (info.namespace == namespace && info.node_id != node_id) {
                        o.next({ ...info, host: rinfo.address })
                    }

                } catch (e) {
                }
            })
        })

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

        const broadcast = async (port: number) => {
            const msg: BroadcastMessage = {
                namespace,
                node_id,
                port
            }
            const json = JSON.stringify(msg)
            for (const host of broadcast_ips) {
                await udp.send(
                    Buffer.from(json),
                    udp_port,
                    host
                )
            }
        }

        return {
            broadcast,
            $new_node_discovered
        }
    }
}