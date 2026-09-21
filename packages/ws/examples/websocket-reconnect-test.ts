import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9880)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    // Start provider-v1
    const providerV1 = start('provider-v1', 'examples/websocket-e2e-provider.ts', {
        ...env,
        PROVIDER_ID: 'provider-v1',
    })
    await waitForOutput(providerV1, /provider ready/, 5000, 'provider-v1')

    const client = start('client', 'examples/websocket-reconnect-client.ts', env)
    await waitForOutput(client, /WebSocket reconnect client connected/, 5000, 'client')

    // Wait for client to complete first call, then emit that output
    const firstOutput = await waitForOutput(client, /"firstCallOk":true/, 10000, 'client')
    console.log(firstOutput.trim())

    // Kill provider-v1 — relay broadcasts offline to all clients
    providerV1.kill('SIGTERM')

    // Wait for client to detect offline, then emit that output
    const offlineOutput = await waitForOutput(client, /"providerOffline":true/, 10000, 'client')
    console.log(offlineOutput.trim())

    // Start provider-v2 — same service, reconnects to relay
    const providerV2 = start('provider-v2', 'examples/websocket-e2e-provider.ts', {
        ...env,
        PROVIDER_ID: 'provider-v2',
    })
    await waitForOutput(providerV2, /provider ready/, 5000, 'provider-v2')

    // Client discovers provider-v2 and calls successfully; emit that output
    const reconnectOutput = await waitForOutput(client, /"reconnectOk":true/, 12000, 'client')
    console.log(reconnectOutput.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
