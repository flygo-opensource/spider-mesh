import process from 'node:process'
import { Http2Rpc } from '../src/index.js'

const rpc = new Http2Rpc()

const guard = setTimeout(() => {
    console.error('RPC contract server timed out')
    process.exit(1)
}, 15000)

rpc.subscribe(event => {
    if (event.endpoints?.port) {
        console.log(`RPC_SERVER_READY:${event.endpoints.port}`)
    }

    if (event.rpc) {
        console.log(JSON.stringify({
            type: 'rpc',
            hasRpc: 'rpc' in event,
            hasMessage: 'message' in (event as Record<string, unknown>),
            packetKind: event.rpc.packet.kind,
            senderNodeId: event.rpc.node_id,
            sourceNodeId: event.rpc.packet.source_node_id,
            targetNodeId: event.rpc.packet.target_node_id,
        }))
        clearTimeout(guard)
        process.exit(0)
    }
})

setInterval(() => undefined, 1000)