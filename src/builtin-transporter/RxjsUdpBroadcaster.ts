import { createSocket } from "dgram"
import { Observable } from "rxjs"

export type RxjsUdpBroadcasterConfig = {
    node_id: string,
    namespace: string,
    UDP_PORT?: number,
    SEEDING_IP_RANGES?: string,
    SEEDING_IPS?: string
}

export class RxjsUdpBroadcaster {



    static async start({ UDP_PORT = 10000, namespace, node_id, SEEDING_IPS, SEEDING_IP_RANGES }: RxjsUdpBroadcasterConfig) {
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
            port: UDP_PORT,
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

        // Add multicast IP or scan all subnets
        if (SEEDING_IP_RANGES) {
            const list = SEEDING_IP_RANGES.split(',').map(ip => ip.trim())
            for (const range of list) {
                for (let i = 1; i <= 255; i++) {
                    const host = `${range}.${i}`
                    broadcast_ips.push(host)
                }
            }
        }

        // Add seeding ip
        if (SEEDING_IPS) {
            const list = SEEDING_IPS.split(',').map(ip => ip.trim())
            for (const host of list) {
                broadcast_ips.push(host)
            }
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
                    UDP_PORT,
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