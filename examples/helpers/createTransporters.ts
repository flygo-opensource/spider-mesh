import { Http2Pubsub, Http2Rpc, UdpDiscovery } from '../../src/index.js'

export function createTransporters() {
    return [
        new UdpDiscovery(),
        new Http2Rpc(),
        new Http2Pubsub(),
    ]
}