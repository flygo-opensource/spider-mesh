/**
 * Getting started (ws) — Step 3: a client node.
 *
 * Calls GreetingService on whichever provider the relay routes to. Still NO Registry.
 *
 *   bun run examples/getting-started/client.ts
 */
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../../src/node.js'

// A typed contract for the remote service. The runtime does not validate this — it is
// purely how you get type-safety on the proxy.
type GreetingService = {
    hello(name: string): Promise<string>
}

const transporter = new WebsocketTransporter()
transporter.connect(process.env.WS_URL || 'ws://127.0.0.1:8787')

const mesh = new SpiderMesh()
mesh.registerTransporter(transporter)

const greeter = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })

// wait() resolves once the relay has told us a provider exists (via ServiceDirectory).
await greeter.wait()

// Remote methods return an RxJS Observable that is also awaitable.
console.log(await greeter.hello('world'))

process.exit(0)
