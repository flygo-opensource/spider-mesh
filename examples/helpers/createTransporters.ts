import type { Registry } from '@spider-mesh/core'
import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '../../src/index.js'

export function createTransporters(registry: Registry) {
    return [
        new UdpDiscovery(registry),
        new Http2Rpc(registry),
        new Http2Pubsub(registry),
    ]
}