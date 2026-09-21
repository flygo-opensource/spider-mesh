import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9900)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    const providerA = start('provider-a', 'examples/websocket-e2e-provider.ts', { ...env, PROVIDER_ID: 'provider-a' })
    const providerB = start('provider-b', 'examples/websocket-e2e-provider.ts', { ...env, PROVIDER_ID: 'provider-b' })
    const providerC = start('provider-c', 'examples/websocket-e2e-provider.ts', { ...env, PROVIDER_ID: 'provider-c' })

    await Promise.all([
        waitForOutput(providerA, /provider ready/, 5000, 'provider-a'),
        waitForOutput(providerB, /provider ready/, 5000, 'provider-b'),
        waitForOutput(providerC, /provider ready/, 5000, 'provider-c'),
    ])

    const client = start('client', 'examples/websocket-failover-client.ts', env)
    await waitForOutput(client, /WebSocket failover client connected/, 5000, 'client')

    // Wait for phase 1 (6 requests across all 3 nodes)
    const phase1Output = await waitForOutput(client, /"phase1UniqueProviders"/, 15000, 'client')
    console.log(phase1Output.trim())

    // Kill provider-c — relay broadcasts offline event to all connected clients
    providerC.kill('SIGTERM')

    // Wait for phase 2 (4 requests across the 2 surviving nodes)
    const phase2Output = await waitForOutput(client, /"phase2UniqueProviders"/, 20000, 'client')
    console.log(phase2Output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
