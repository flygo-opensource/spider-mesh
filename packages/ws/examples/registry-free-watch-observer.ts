/**
 * Observer node: subscribes DIRECTLY to `greeter.watch()` over the ws relay with NO
 * Registry, and reports the availability stream transitions.
 *
 *   WATCH_READY    — subscribed
 *   WATCH_PRESENT  — watch() emitted a non-empty provider list (provider appeared)
 *   WATCH_GONE     — watch() emitted an empty list after having seen a provider (disappeared)
 */
import { RemoteServiceLinker, SpiderMesh, Topology } from '@spider-mesh/core'
import { WebsocketTransporter } from '../src/node.js'

type GreetingService = { hello(name: string): Promise<string> }

const transporter = new WebsocketTransporter()
transporter.connect(process.env.WS_URL || 'ws://127.0.0.1:8787')

// watch()/nodes() cần Topology; relay routing RPC thông thường thì không cần.
const mesh = new SpiderMesh({
    topology: new Topology({ discovery: transporter }),
    transporters: [transporter],
})

const greeter = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })

console.log('WATCH_READY')

let sawProvider = false
greeter.watch().subscribe(nodes => {
    if (nodes.length > 0) {
        sawProvider = true
        console.log('WATCH_PRESENT')
    } else if (sawProvider) {
        console.log('WATCH_GONE')
    }
})

setInterval(() => undefined, 1000)
