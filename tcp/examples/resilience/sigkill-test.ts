import { spawn, type ChildProcess } from 'node:child_process'
import { Registry } from '@spider-mesh/core'
import { Http2Rpc } from '../../src/index.js'
import { createDiscovery } from '../helpers/createDiscovery.js'
import { callRpc, discoveryMessage, makeNode, waitFor, waitForRpcPort } from './helpers.js'

const namespace = process.env.SPIDERMESH_NAMESPACE || 'sigkill-test'

function startProvider(generation: number) {
    const child = spawn('bun', ['run', 'examples/resilience/sigkill-provider.ts'], {
        cwd: new URL('../..', import.meta.url),
        env: {
            ...process.env,
            SPIDERMESH_NAMESPACE: namespace,
            SPIDERMESH_NODE_ID: 'sigkill-provider',
            PROVIDER_GENERATION: String(generation),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    })
    return child
}

async function waitForReady(child: ChildProcess) {
    return await new Promise<{ port: number, generation: number }>((resolve, reject) => {
        let stdout = ''
        let stderr = ''
        const timeout = setTimeout(() => reject(new Error(`Provider ready timeout\n${stdout}\n${stderr}`)), 5000)
        child.stdout?.on('data', chunk => {
            stdout += chunk.toString()
            const line = stdout.split('\n').find(value => value.includes('"event":"provider-ready"'))
            if (!line) return
            clearTimeout(timeout)
            resolve(JSON.parse(line))
        })
        child.stderr?.on('data', chunk => { stderr += chunk.toString() })
        child.once('exit', code => {
            clearTimeout(timeout)
            reject(new Error(`Provider exited early with ${code}\n${stdout}\n${stderr}`))
        })
    })
}

async function waitForExit(child: ChildProcess) {
    if (child.exitCode !== null || child.signalCode) return
    await new Promise<void>(resolve => child.once('exit', () => resolve()))
}

const registry = new Registry()
const reachabilityEvents: string[] = []
registry.events$.subscribe(event => {
    if (event.type.startsWith('endpoint-')) reachabilityEvents.push(event.type)
})
const discovery = createDiscovery('sigkill-consumer', registry)
const rpc = new Http2Rpc(registry)
const consumerPort = await waitForRpcPort(rpc)
await discovery.broadcast(discoveryMessage(makeNode({
    node_id: 'sigkill-consumer',
    namespace,
    transporters: { http2: { port: consumerPort } },
})))

let provider = startProvider(1)
try {
    const first = await waitForReady(provider)
    await waitFor(() => !!registry.getPeer('sigkill-provider'), 3000, 'first SIGKILL provider')
    await callRpc({ rpc, sender_node_id: 'sigkill-consumer', service: 'SigkillService' })

    provider.kill('SIGKILL')
    await waitForExit(provider)
    await waitFor(
        () => !!registry.getPeer('sigkill-provider')
            && registry.getReachability('sigkill-provider', 'http2') !== 'reachable',
        3000,
        'SIGKILL endpoint unreachable',
    )

    provider = startProvider(2)
    const second = await waitForReady(provider)
    await waitFor(
        () => registry.getPeer('sigkill-provider')?.services.SigkillService?.generation === 2
            && registry.getReachability('sigkill-provider', 'http2') === 'reachable',
        3000,
        'SIGKILL provider rediscovery',
    )
    await callRpc({ rpc, sender_node_id: 'sigkill-consumer', service: 'SigkillService' })

    console.log(JSON.stringify({
        sigkillPassed: true,
        topologyMembershipPreserved: true,
        rediscovered: true,
        firstPort: first.port,
        secondPort: second.port,
        reachabilityEvents,
    }))
} finally {
    if (provider.exitCode === null && !provider.signalCode) {
        provider.kill('SIGTERM')
        await waitForExit(provider)
    }
    rpc.unsubscribe()
    discovery.close()
}
