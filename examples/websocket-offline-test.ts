import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9840)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    const provider = start('provider', 'examples/websocket-e2e-provider.ts', env)
    await waitForOutput(provider, /provider ready/, 5000, 'provider')

    const client = start('client', 'examples/websocket-offline-client.ts', env)
    await waitForOutput(client, /WebSocket offline client connected/, 5000, 'client')

    // Wait for client to complete first successful call, then emit that output
    const firstOutput = await waitForOutput(client, /"firstCallOk":true/, 10000, 'client')
    console.log(firstOutput.trim())

    // Kill the provider — relay broadcasts offline event to all connected nodes
    provider.kill('SIGTERM')

    // Client detects offline via registry watch and reports; emit that output too
    const offlineOutput = await waitForOutput(client, /"offlineDetected":true/, 12000, 'client')
    console.log(offlineOutput.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
